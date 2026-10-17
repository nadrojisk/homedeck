#!/usr/bin/env python3
"""HomeDeck collector: a small, read-only, standard-library-only backend for the HomeDeck dashboard.

It polls the homelab's real signals in background threads and serves the results (see LIVE_DATA.md):

    GET /api/homedeck.json   Snapshot (CONTRACT.md "Data model") assembled from in-memory state
    GET /api/logs            recent Loki lines for one service
    GET /api/healthz         which sources are currently answering

Sources (all optional; a failing source is logged and simply left out of the snapshot):
    Docker   via a read-only docker-socket-proxy (containers, inspect, stats, events, info)
    Gatus    endpoint statuses (health checks)
    Pi-hole  v6 API with an app password (DNS stats)
    Loki     logs (queried on demand by /api/logs)
    /proc    host CPU, memory, uptime, load and ZFS pool state (host /proc mounted at /host/proc)
    statvfs  of the ZFS dataset mounted at /host/srv (capacity only; no file is ever read there)

Logs go to stdout as one JSON object per line.
"""
import json
import os
import re
import signal
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# --- Configuration (environment, with defaults that match docker-compose.yml)
ENV = os.environ.get
DOCKER_URL = ENV("DOCKER_URL") or "http://homedeck-dockerproxy:2375"
GATUS_URL = ENV("GATUS_URL") or "http://gatus:8080"
PIHOLE_URL = ENV("PIHOLE_URL") or "http://pihole"
PIHOLE_PASSWORD = ENV("PIHOLE_APP_PASSWORD") or ""
LOKI_URL = ENV("LOKI_URL") or "http://loki:3100"
LAN_IP = ENV("LAN_IP") or None
PROC = ENV("HOST_PROC") or "/host/proc"
SRV = ENV("HOST_SRV") or "/host/srv"          # mountpoint of the ZFS dataset used for capacity
SRV_POOL = ENV("SRV_POOL") or "tank"          # pool that SRV belongs to
DMI = ENV("HOST_DMI") or "/host/dmi"          # /sys/class/dmi/id (hardware name)
DATA = ENV("DATA_DIR") or "/data"
PORT = int(ENV("PORT") or 8000)
SERVICES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "services.json")

DOCKER_EVERY, GATUS_EVERY, PIHOLE_EVERY, HOST_EVERY, LOKI_EVERY = 12, 30, 30, 5, 60
MAX_EVENTS, MAX_ALERTS, ALERT_KEEP, HISTORY_KEEP = 50, 20, 24 * 3600, 48 * 3600
TIB = 1024 ** 4


# --- Small helpers
def log(level, msg, **kw):
    print(json.dumps({"ts": datetime.now().astimezone().isoformat(timespec="seconds"),
                      "level": level, "msg": msg, **kw}, default=str), flush=True)


def now_ms():
    return int(time.time() * 1000)


def kebab(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")


def http_json(url, method="GET", body=None, headers=None, timeout=6):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    return json.loads(raw) if raw else None


def docker(path, timeout=6):
    return http_json(DOCKER_URL + path, timeout=timeout)


def parse_ts(s):
    """RFC 3339 with up to nanosecond fractions (Docker, Gatus) -> ms epoch, or None."""
    if not s or s.startswith("0001-"):
        return None
    m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)$", s)
    if not m:
        return None
    base, frac, tz = m.groups()
    dt = datetime.fromisoformat(base + ("+00:00" if tz == "Z" else tz))
    return int(dt.timestamp() * 1000) + int((frac or "0")[:3].ljust(3, "0"))


def read(path, default=None):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return default


def loop(name, fn, every, wake=None):
    """Run fn forever in a daemon thread; exceptions are logged, never fatal. `wake` (a threading.Event)
    lets another thread trigger an early run."""
    def run():
        while True:
            t0 = time.time()
            try:
                fn()
            except Exception as e:  # noqa: BLE001 - every source is optional
                log("error", f"{name} failed", error=repr(e))
            delay = max(1.0, every - (time.time() - t0))
            if wake:
                wake.wait(delay)
                time.sleep(1.0)  # let a burst of events settle
                wake.clear()
            else:
                time.sleep(delay)
    threading.Thread(target=run, name=name, daemon=True).start()


# --- Shared state (guarded by LOCK)
LOCK = threading.RLock()
S = {
    "containers": {},      # id -> normalised container record (see docker_poll)
    "docker_info": {},     # /info subset
    "docker_ok": 0,        # time of last successful poll, per source
    "gatus": [],           # [{key, name, group, hostname, success, status, lastCheck, durationMs}]
    "gatus_ok": 0,
    "pihole": None,        # /api/stats/summary
    "pihole_ok": 0,
    "loki_ok": False,
    "host": {},            # cpu/mem/uptime/load
    "zfs": {},             # pool -> {state, usedPct, usedTB, sizeTB}
    "events": [],          # newest first
    "event_ids": set(),
    "alerts": {},          # id -> alert
    "prev_status": {},     # service id -> status (for transition events)
    "prev_checks": {},     # gatus key -> success
    "svc_image": {},       # (project, compose service) -> image id last seen running (deploy detection)
    "health": {},          # container id -> last health status seen in events
    "log_selectors": {},   # service id -> LogQL selector ('' = no logs)
    "service_ids": set(),
    "snapshot": b"{}",
    "events_dirty": False,
}


def load_events():
    """Docker's daemon only buffers ~256 recent events (healthcheck execs included), so the event list is
    also persisted in the data volume to survive collector restarts."""
    try:
        with open(EVENTS_FILE) as f:
            for ev in json.load(f):
                add_event(ev)
        S["events_dirty"] = False
    except (OSError, ValueError, TypeError, KeyError):
        pass


def save_events():
    with LOCK:
        if not S["events_dirty"]:
            return
        data, S["events_dirty"] = list(S["events"]), False
    try:
        with open(EVENTS_FILE + ".tmp", "w") as f:
            json.dump(data, f)
        os.replace(EVENTS_FILE + ".tmp", EVENTS_FILE)
    except OSError as e:
        log("warn", "events not persisted", error=repr(e))


EVENTS_FILE = os.path.join(DATA, "events.json")


def add_event(ev):
    """Insert an event (deduplicated by id), keep newest first, cap at MAX_EVENTS."""
    with LOCK:
        if ev["id"] in S["event_ids"]:
            return
        S["event_ids"].add(ev["id"])
        S["events_dirty"] = True
        S["events"].append(ev)
        S["events"].sort(key=lambda e: e["ts"], reverse=True)
        del S["events"][MAX_EVENTS:]
        if len(S["event_ids"]) > 2000:
            S["event_ids"] = {e["id"] for e in S["events"]}


# --- services.json overrides (reloaded when the file changes)
_overrides = {"mtime": None, "data": {}}


def overrides():
    try:
        mtime = os.path.getmtime(SERVICES_FILE)
        if mtime != _overrides["mtime"]:
            with open(SERVICES_FILE) as f:
                raw = json.load(f)
            _overrides.update(mtime=mtime, data={k: v for k, v in raw.items() if not k.startswith("_")})
            log("info", "loaded services.json", services=len(_overrides["data"]))
    except FileNotFoundError:
        pass
    except Exception as e:  # noqa: BLE001
        log("error", "services.json unreadable, keeping previous", error=repr(e))
    return _overrides["data"]


# --- Docker: container list + inspect (cached) + one-shot stats with our own CPU deltas
DOCKER_WAKE = threading.Event()  # set by the event stream so state changes show up within seconds
STATS_POOL = ThreadPoolExecutor(max_workers=6, thread_name_prefix="stats")
HOST_RE = re.compile(r"Host\(([^)]*)\)")
PELICAN = lambda labels: labels.get("Service") == "Pelican" and labels.get("ContainerType") == "server_process"  # noqa: E731


def list_health(status):
    s = status or ""
    return "unhealthy" if "(unhealthy)" in s else "healthy" if "(healthy)" in s else \
        "starting" if "health: starting" in s else None


def fetch_stats(cid):
    s = docker(f"/containers/{cid}/stats?stream=false&one-shot=true", timeout=10)
    cpu = s.get("cpu_stats") or {}
    mem = s.get("memory_stats") or {}
    st = mem.get("stats") or {}
    cache = st.get("inactive_file", st.get("total_inactive_file", st.get("cache", 0)))
    return ((cpu.get("cpu_usage") or {}).get("total_usage"), cpu.get("system_cpu_usage"),
            max(0, (mem.get("usage") or 0) - cache))


def docker_poll():
    if not S["docker_info"] or time.time() - S["docker_info"].get("_at", 0) > 300:
        info = docker("/info")
        S["docker_info"] = {"_at": time.time(), "name": info.get("Name"), "version": info.get("ServerVersion"),
                            "os": info.get("OperatingSystem"), "cpus": info.get("NCPU")}
    listing = docker("/containers/json?all=1")
    old = S["containers"]
    fresh = {}
    for c in listing:
        cid = c["Id"]
        prev = old.get(cid)
        health = list_health(c.get("Status"))
        stale = (prev is None or time.time() - prev["_inspected"] > 60
                 or prev["state"] != c.get("State") or prev["health"] != health)
        if stale:
            try:
                insp = docker(f"/containers/{cid}/json")
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    continue  # removed between list and inspect
                raise
        labels = c.get("Labels") or {}
        rec = dict(prev) if prev and not stale else {
            "id": cid,
            "name": insp["Name"].lstrip("/"),
            "labels": labels,
            "project": labels.get("com.docker.compose.project"),
            "cservice": labels.get("com.docker.compose.service"),
            "image": (insp["Config"].get("Image") or c.get("Image") or "").split("@")[0],
            "imageId": insp.get("Image"),
            "state": insp["State"].get("Status"),
            "health": (insp["State"].get("Health") or {}).get("Status"),
            "startedAt": parse_ts(insp["State"].get("StartedAt")),
            "finishedAt": parse_ts(insp["State"].get("FinishedAt")),
            "exitCode": insp["State"].get("ExitCode"),
            "restarts": insp.get("RestartCount", 0),
            "restartPolicy": (insp["HostConfig"].get("RestartPolicy") or {}).get("Name") or "no",
            "memLimit": insp["HostConfig"].get("Memory") or 0,
            "hosts": [h for k, v in labels.items() if re.match(r"traefik\.http\.routers\.[^.]+\.rule$", k)
                      for grp in HOST_RE.findall(v) for h in re.findall(r"`([^`]+)`", grp)],
            "routers": sum(1 for k in labels if re.match(r"traefik\.http\.routers\.[^.]+\.rule$", k)),
            "_inspected": time.time(),
            "cpu": None, "memMB": None, "_cpu": None,
        }
        rec["ports"] = sorted({f'{p["PublicPort"]}:{p["PrivatePort"]}/{p["Type"]}'
                               for p in c.get("Ports") or [] if p.get("PublicPort")},
                              key=lambda s: (int(s.split(":")[0]), s))
        if prev:  # carry CPU sample across re-inspects
            rec["_cpu"], rec["cpu"], rec["memMB"] = prev.get("_cpu"), prev.get("cpu"), prev.get("memMB")
        fresh[cid] = rec

    # Stats for running containers, in parallel. CPU % of the whole host from our own deltas between
    # polls: d(container cpu ns) / d(host cpu ns, all cores) * 100.
    running = [r for r in fresh.values() if r["state"] == "running"]
    futures = {r["id"]: STATS_POOL.submit(fetch_stats, r["id"]) for r in running}
    for cid, fut in futures.items():
        rec = fresh[cid]
        try:
            total, system, mem = fut.result(timeout=15)
        except Exception as e:  # noqa: BLE001
            log("warn", "stats failed", container=rec["name"], error=repr(e))
            continue
        rec["memMB"] = round(mem / 1048576, 1)
        p = rec.get("_cpu")
        if p and total is not None and system and system > p[1] and total >= p[0]:
            rec["cpu"] = round((total - p[0]) / (system - p[1]) * 100, 2)
        rec["_cpu"] = (total, system) if total is not None and system else None
    for r in fresh.values():
        if r["state"] != "running":
            r["cpu"], r["memMB"], r["_cpu"] = None, None, None

    with LOCK:
        S["containers"] = fresh
        for r in running:
            if r["project"] and r["cservice"]:
                S["svc_image"].setdefault((r["project"], r["cservice"]), r["imageId"])
        S["docker_ok"] = time.time()


# --- Docker event stream (start / die / oom / health_status), seeded with the last hour, reconnecting
def event_target(attrs):
    """Container event attributes -> (service id, short label) or (None, None) if not ours."""
    name = attrs.get("name", "")
    project, cservice = attrs.get("com.docker.compose.project"), attrs.get("com.docker.compose.service")
    if project:
        return kebab(project), cservice or name
    if PELICAN(attrs):
        sid = "pelican-server-" + kebab(name)[:8]
        return sid, (overrides().get(sid) or {}).get("name", "Game server")
    sid = kebab(name)
    return (sid, name) if sid in S["service_ids"] else (None, None)


def handle_docker_event(ev):
    action, actor = ev.get("Action", ""), ev.get("Actor") or {}
    attrs, cid = actor.get("Attributes") or {}, actor.get("ID", "")
    sid, label = event_target(attrs)
    if not sid:
        return
    DOCKER_WAKE.set()
    ts = int(ev.get("timeNano", time.time() * 1e9) // 1_000_000)
    base = {"id": f"d-{ev.get('timeNano')}-{cid[:8]}-{kebab(action)}", "ts": ts, "serviceId": sid,
            "hostId": S["docker_info"].get("name") or "host"}
    if action == "start":
        key = (attrs.get("com.docker.compose.project"), attrs.get("com.docker.compose.service"))
        try:
            image_id = docker(f"/containers/{cid}/json").get("Image") if key[0] else None
        except Exception:  # noqa: BLE001 - container may already be gone
            image_id = None
        with LOCK:
            before = S["svc_image"].get(key)
            if image_id:
                S["svc_image"][key] = image_id
        if image_id and before and before != image_id:
            tag = attrs.get("image", "").split("@")[0]
            add_event({**base, "kind": "deploy", "message": f"{label} updated" + (f" ({tag})" if tag else "")})
        else:
            add_event({**base, "kind": "restart", "message": f"{label} started"})
    elif action == "die":
        add_event({**base, "kind": "status", "message": f"{label} stopped (exit {attrs.get('exitCode', '?')})"})
    elif action == "oom":
        add_event({**base, "kind": "status", "message": f"{label} ran out of memory"})
    elif action.startswith("health_status"):
        status = action.split(":", 1)[-1].strip()
        prev = S["health"].get(cid)
        S["health"][cid] = status
        if status == "unhealthy":
            add_event({**base, "kind": "status", "message": f"{label} is unhealthy"})
        elif status == "healthy" and prev == "unhealthy":
            add_event({**base, "kind": "status", "message": f"{label} is healthy again"})


def docker_events_forever():
    since = int(time.time()) - 3600  # seed with the last hour
    flt = urllib.parse.quote(json.dumps({"type": ["container"],
                                         "event": ["start", "die", "oom", "health_status"]}))
    while True:
        try:
            url = f"{DOCKER_URL}/events?since={since}&filters={flt}"
            with urllib.request.urlopen(url, timeout=330) as r:
                log("info", "docker event stream connected", since=since)
                for line in r:
                    if not line.strip():
                        continue
                    ev = json.loads(line)
                    since = max(since, int(ev.get("time", since)))
                    try:
                        handle_docker_event(ev)
                    except Exception as e:  # noqa: BLE001
                        log("error", "docker event handling failed", error=repr(e))
        except Exception as e:  # noqa: BLE001 - idle timeouts and proxy restarts land here
            log("warn", "docker event stream reconnecting", error=repr(e))
            time.sleep(3)


# --- Gatus, Pi-hole, Loki
def gatus_poll():
    eps = http_json(f"{GATUS_URL}/api/v1/endpoints/statuses", timeout=8)
    out = []
    for e in eps or []:
        res = (e.get("results") or [])
        if not res:
            continue
        r = res[-1]
        out.append({"key": e.get("key") or kebab(e.get("name")), "name": e.get("name"), "group": e.get("group"),
                    "hostname": r.get("hostname"), "success": bool(r.get("success")), "status": r.get("status"),
                    "lastCheck": parse_ts(r.get("timestamp")),
                    "durationMs": round((r.get("duration") or 0) / 1e6, 1)})
    with LOCK:
        S["gatus"], S["gatus_ok"] = out, time.time()


class Pihole:
    """Pi-hole v6: one session reused until it expires; a 401 triggers a single re-auth."""
    sid = None
    retry_at = 0  # after a rejected login, back off instead of hammering the auth endpoint

    @classmethod
    def auth(cls):
        try:
            r = http_json(f"{PIHOLE_URL}/api/auth", method="POST", body={"password": PIHOLE_PASSWORD})
        except urllib.error.HTTPError as e:
            r = json.loads(e.read() or b"{}") if e.code in (400, 401, 429) else None
            if r is None:
                raise
        sess = (r or {}).get("session") or {}
        if not sess.get("valid") or not sess.get("sid"):
            cls.retry_at = time.time() + 300
            raise RuntimeError(f"pihole login rejected ({sess.get('message') or 'no session'}); retrying in 5 min")
        cls.sid = sess["sid"]

    @classmethod
    def poll(cls):
        if not PIHOLE_PASSWORD or time.time() < cls.retry_at:
            return
        for attempt in (1, 2):
            if not cls.sid:
                cls.auth()
            try:
                data = http_json(f"{PIHOLE_URL}/api/stats/summary", headers={"X-FTL-SID": cls.sid})
                with LOCK:
                    S["pihole"], S["pihole_ok"] = data, time.time()
                return
            except urllib.error.HTTPError as e:
                if e.code == 401 and attempt == 1:
                    cls.sid = None
                    continue
                raise


def loki_poll():
    """Probe Loki with the cheapest real query API call (/ready flaps to 503 during ring updates)."""
    try:
        http_json(f"{LOKI_URL}/loki/api/v1/labels?since=5m", timeout=5)
        S["loki_ok"] = True
    except Exception:
        S["loki_ok"] = False
        raise


ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
LEVELS = {"error": "error", "err": "error", "eror": "error", "fatal": "error", "panic": "error", "crit": "error",
          "critical": "error", "alert": "error", "emerg": "error", "warn": "warn", "warning": "warn",
          "info": "info", "information": "info", "notice": "info", "debug": "debug", "dbug": "debug",
          "trace": "debug"}
LEVEL_WORDS = [("error", re.compile(r"\b(FATAL|PANIC|ERROR|ERR|CRIT(?:ICAL)?)\b")),
               ("warn", re.compile(r"\b(WARN|WARNING)\b")),
               ("info", re.compile(r"\b(INFO|NOTICE)\b")),
               ("debug", re.compile(r"\b(DEBUG|TRACE)\b"))]


def detect_level(labels, line):
    for k in ("level", "detected_level"):
        if LEVELS.get(str(labels.get(k, "")).lower()):
            return LEVELS[labels[k].lower()]
    if line.startswith("{"):
        try:
            obj = json.loads(line)
            for k in ("level", "lvl", "severity", "log.level"):
                if LEVELS.get(str(obj.get(k, "")).lower()):
                    return LEVELS[str(obj[k]).lower()]
        except (ValueError, AttributeError):
            pass
    m = re.search(r"\b(?:level|lvl)=\"?(\w+)", line)
    if m and LEVELS.get(m.group(1).lower()):
        return LEVELS[m.group(1).lower()]
    for level, rx in LEVEL_WORDS:
        if rx.search(line):
            return level
    return None


def query_logs(sid, limit):
    selector = S["log_selectors"].get(sid)
    if selector is None:
        return 404, {"error": f"unknown service {sid!r}"}
    if not selector:
        return 200, {"service": sid, "lines": []}
    end = time.time_ns()
    qs = urllib.parse.urlencode({"query": selector, "start": end - 3600 * 10**9, "end": end,
                                 "limit": limit, "direction": "backward"})
    try:
        r = http_json(f"{LOKI_URL}/loki/api/v1/query_range?{qs}", timeout=10)
    except Exception as e:  # noqa: BLE001
        log("warn", "loki query failed", service=sid, error=repr(e))
        S["loki_ok"] = False
        return 502, {"service": sid, "lines": [], "error": "loki unavailable"}
    S["loki_ok"] = True
    lines = []
    for stream in (r.get("data") or {}).get("result") or []:
        labels = stream.get("stream") or {}
        for ts, line in stream.get("values") or []:
            text = ANSI.sub("", line).rstrip("\r\n")
            lines.append({"ts": int(ts) // 1_000_000, "container": labels.get("container"),
                          "level": detect_level(labels, text), "line": text[:500]})
    lines.sort(key=lambda x: x["ts"], reverse=True)
    return 200, {"service": sid, "lines": lines[:limit]}


# --- Host: /proc sampler and ZFS pools
_cpu_prev = {"v": None}


def host_sample():
    h = {}
    fields = [int(x) for x in read(f"{PROC}/stat", "cpu 0").splitlines()[0].split()[1:9]]
    total, idle = sum(fields), fields[3] + (fields[4] if len(fields) > 4 else 0)
    p = _cpu_prev["v"]
    if p and total > p[0]:
        h["cpu"] = round((1 - (idle - p[1]) / (total - p[0])) * 100, 1)
    _cpu_prev["v"] = (total, idle)
    mem = {ln.split(":")[0]: int(ln.split()[1]) for ln in read(f"{PROC}/meminfo", "").splitlines() if ":" in ln}
    if mem.get("MemTotal"):
        h["memTotalMB"] = mem["MemTotal"] // 1024
        h["memUsedMB"] = (mem["MemTotal"] - mem.get("MemAvailable", 0)) // 1024
    up = read(f"{PROC}/uptime")
    if up:
        h["uptimeSec"] = int(float(up.split()[0]))
    la = read(f"{PROC}/loadavg")
    if la:
        h["load"] = [float(x) for x in la.split()[:3]]
    arc = read(f"{PROC}/spl/kstat/zfs/arcstats")
    if arc:
        m = re.search(r"^size\s+\d+\s+(\d+)", arc, re.M)
        h["zfsArcMB"] = int(m.group(1)) // 1048576 if m else None

    pools = {}
    kdir = f"{PROC}/spl/kstat/zfs"
    for name in sorted(os.listdir(kdir)) if os.path.isdir(kdir) else []:
        state = read(f"{kdir}/{name}/state")
        if state is None:
            continue
        pool = {"state": state.strip() or "UNKNOWN", "usedPct": None, "usedTB": None, "sizeTB": None}
        if name == SRV_POOL:
            try:
                st = os.statvfs(SRV)
                size, free = st.f_blocks * st.f_frsize, st.f_bfree * st.f_frsize
                if size:
                    pool.update(usedPct=round((size - free) / size * 100, 1),
                                usedTB=round((size - free) / TIB, 2), sizeTB=round(size / TIB, 2))
            except OSError as e:
                log("warn", "statvfs failed", path=SRV, error=repr(e))
        pools[name] = pool
    with LOCK:
        S["host"].update(h)
        S["zfs"] = pools


def hardware_name():
    product = (read(f"{DMI}/product_name") or "").strip()
    vendor = (read(f"{DMI}/sys_vendor") or "").strip().replace(" Inc.", "").replace(",", "")
    if product and vendor and not product.startswith(vendor) and vendor.lower() not in ("to be filled by o.e.m.",):
        return f"{vendor} {product}"
    return product or None


# --- Snapshot assembly
TYPE_DEFAULTS = [(r"^traefik$", "proxy"), (r"^pihole$", "dns"), (r"^nextcloud", "files"),
                 (r"^media[-_]stack$", "media-server"), (r"^pelican$", "game-panel"),
                 (r"^(observability|gatus|isp-monitor)$", "monitoring"), (r"^pocket-id$", "auth"),
                 (r"^discord-bot", "automation"), (r"^homedeck$", "dashboard")]
CATEGORY = {"proxy": "network", "auth": "network", "dns": "network", "media-server": "media",
            "media-manager": "media", "indexer": "media", "downloader": "media", "game-panel": "games",
            "game-server": "games", "storage": "storage", "monitoring": "ops", "automation": "ops",
            "dashboard": "ops", "database": "ops", "files": "apps", "generic": "apps"}


def expected_running(m):
    """Should this stopped container be running? (restart policy says so, or it crashed)."""
    pol = m["restartPolicy"]
    return pol in ("always", "unless-stopped") or (m["exitCode"] or 0) != 0


def group_containers(containers, ov):
    """Containers -> {service id: {kind, project, members}}."""
    groups = {}
    for c in sorted(containers.values(), key=lambda r: r["name"]):
        if c["project"]:
            sid, kind = kebab(c["project"]), "compose"
        elif PELICAN(c["labels"]):
            sid, kind = "pelican-server-" + kebab(c["name"])[:8], "pelican"
        elif c["state"] == "running":
            sid, kind = kebab(c["name"]), "single"
        else:
            continue  # exited non-compose, non-Pelican: ignored
        g = groups.setdefault(sid, {"kind": kind, "project": c["project"], "members": []})
        g["members"].append(c)
    return groups


def build_service(sid, g, ov, host_id, now, claimed):
    o = ov.get(sid, {})
    members = g["members"]
    project = g["project"] or members[0]["name"]
    stype = o.get("type") or ("game-server" if g["kind"] == "pelican" else
                              next((t for rx, t in TYPE_DEFAULTS if re.search(rx, project)), "generic"))
    label = lambda m: m["cservice"] or m["name"]  # noqa: E731
    # primary member: services.json "primary" (compose service), else the one named like the project
    primary = next((m for m in members if m["cservice"] == o.get("primary", project)), members[0])
    running = [m for m in members if m["state"] == "running"]

    # Traefik hosts: services.json "hosts" first; hosts claimed by another service's override are dropped.
    hosts = list(dict.fromkeys(o.get("hosts", []) + [h for m in [primary] + members for h in m["hosts"]
                                                      if claimed.get(h, sid) == sid]))
    starts = [m["startedAt"] for m in running if m["startedAt"]]
    all_starts = [m["startedAt"] for m in members if m["startedAt"]]
    default_name = "Game server" if g["kind"] == "pelican" else re.sub(r"[-_]+", " ", project).title()

    svc = {
        "id": sid, "name": o.get("name") or default_name, "host": host_id, "type": stype,
        "category": o.get("category") or CATEGORY.get(stype, "apps"), "image": primary["image"],
        "status": "healthy", "statusReason": None, "critical": bool(o.get("critical", False)),
        "placeholder": False,
        "uptimeSec": int((now - max(starts)) / 1000) if starts else 0,
        "cpu": round(sum(m["cpu"] or 0 for m in running), 2),
        "memMB": round(sum(m["memMB"] or 0 for m in running)),
        "memLimitMB": (round(sum(m["memLimit"] for m in members) / 1048576)
                       if all(m["memLimit"] for m in members) else None),
        "ports": sorted({p for m in members for p in m["ports"]}, key=lambda s: (int(s.split(":")[0]), s)),
        "url": o["url"] if "url" in o else (f"https://{hosts[0]}" if hosts else None),
        "urls": [f"https://{h}" for h in hosts],
        "lastRestart": max(all_starts) if all_starts else None,
        "description": o.get("description") or (
            f"Pelican game server container {members[0]['name'][:8]}." if g["kind"] == "pelican" else
            f"Docker compose project {project} ({len(members)} containers)." if g["kind"] == "compose" else
            f"Standalone container {members[0]['name']} ({primary['image']})."),
        "metrics": [{"key": "containers", "label": "Containers up", "value": f"{len(running)}/{len(members)}"}],
        "members": [{"name": m["name"], "service": m["cservice"], "image": m["image"], "state": m["state"],
                     "health": m["health"] if m["state"] == "running" else None, "cpu": m["cpu"],
                     "memMB": m["memMB"], "startedAt": m["startedAt"], "restarts": m["restarts"]}
                    for m in members],
        "checks": [],
        "_hosts": hosts,
        "_names": {m["name"] for m in members},
        "_selector": (f'{{project="{g["project"]}"}}' if g["project"] else f'{{container="{members[0]["name"]}"}}'),
    }

    # Container-level health -> status (Gatus checks are folded in later).
    reasons = []
    if not running:
        svc["status"] = "down"
        if len(members) == 1:
            m = members[0]
            who = label(m) if g["kind"] == "compose" else "Container"
            reasons.append(f"{who} stopped" + (f" (exit {m['exitCode']})" if m["exitCode"] is not None else ""))
        else:
            reasons.append(f"All {len(members)} containers stopped")
    else:
        for m in members:
            if m["state"] == "restarting":
                reasons.append(f"{label(m)} is restarting")
        unhealthy = [label(m) for m in running if m["health"] == "unhealthy"]
        if len(unhealthy) > 2:
            reasons.append(f"{len(unhealthy)} containers unhealthy ({', '.join(unhealthy[:3])}...)")
        else:
            reasons += [f"{n} is unhealthy" for n in unhealthy]
        stopped = [m for m in members if m["state"] not in ("running", "restarting") and expected_running(m)]
        if len(stopped) == 1 and len(members) > 1:
            m = stopped[0]
            reasons.append(f"{label(m)} stopped (exit {m['exitCode']})")
        elif stopped:
            names = ", ".join(label(m) for m in stopped[:3]) + ("..." if len(stopped) > 3 else "")
            reasons.append(f"{len(stopped)} of {len(members)} containers stopped ({names})")
        if reasons:
            svc["status"] = "degraded"
    svc["_reasons"] = reasons
    return svc


def build_zfs_service(pool, z, host_id, host, ov):
    sid = f"zfs-{kebab(pool)}"
    o = ov.get(sid, {})
    state = z["state"]
    status = "healthy" if state == "ONLINE" else "degraded" if state == "DEGRADED" else "down"
    reasons = [] if state == "ONLINE" else [f"Pool {pool} is {state}"]
    if status == "healthy" and (z["usedPct"] or 0) >= 90:
        status, reasons = "degraded", [f"Pool {pool} is {z['usedPct']:.0f}% full"]
    metrics = []
    if z["usedPct"] is not None:
        metrics += [{"key": "usedPct", "label": "Used", "value": z["usedPct"], "unit": "%"},
                    {"key": "usedTB", "label": "Used / size", "value": f"{z['usedTB']:.2f} / {z['sizeTB']:.2f} TB"}]
    metrics.append({"key": "health", "label": "Health", "value": state})
    up = host.get("uptimeSec") or 0
    return {
        "id": sid, "name": o.get("name") or f'ZFS pool "{pool}"', "host": host_id,
        "type": o.get("type") or "storage", "category": o.get("category") or "storage",
        "image": "OpenZFS (host)", "status": status, "statusReason": "; ".join(reasons) or None,
        "critical": bool(o.get("critical", True)), "placeholder": False, "uptimeSec": up, "cpu": 0, "memMB": 0,
        "memLimitMB": None, "ports": [], "url": o.get("url"), "urls": [], "lastRestart": now_ms() - up * 1000,
        "description": o.get("description") or f"ZFS storage pool {pool} on the host.",
        "metrics": metrics, "members": [], "checks": [],
        "_hosts": [], "_names": set(), "_selector": "", "_reasons": reasons,
    }


def check_failure_text(ch):
    return f"Gatus: {ch['name']} failing" + (f" (HTTP {ch['status']})" if ch.get("status") else "")


def assemble():
    now = now_ms()
    ov = overrides()
    with LOCK:
        containers = dict(S["containers"])
        gatus, pihole, host, zfs = list(S["gatus"]), S["pihole"], dict(S["host"]), dict(S["zfs"])
        dinfo = dict(S["docker_info"])
    host_id = dinfo.get("name") or "host"

    # --- services
    claimed = {h: sid for sid, o in ov.items() for h in o.get("hosts", [])}
    services = [build_service(sid, g, ov, host_id, now, claimed)
                for sid, g in group_containers(containers, ov).items()]
    services += [build_zfs_service(p, z, host_id, host, ov) for p, z in zfs.items()]
    by_id = {s["id"]: s for s in services}

    # --- Gatus checks -> services: explicit names in services.json, then Traefik host, then container name
    explicit = {name: sid for sid, o in ov.items() for name in o.get("checks", [])}
    host_map, name_map = {}, {}
    for s in services:
        for h in s["_hosts"]:
            host_map.setdefault(h, s["id"])
        for n in s["_names"]:
            name_map.setdefault(n, s["id"])
    unmapped_failing = []
    check_sid = {}
    for ch in gatus:
        sid = explicit.get(ch["name"]) or host_map.get(ch["hostname"]) or name_map.get(ch["hostname"])
        check_sid[ch["key"]] = sid if sid in by_id else None
        if sid in by_id:
            by_id[sid]["checks"].append(ch)
        elif not ch["success"]:
            unmapped_failing.append(ch)

    # --- source metrics, status from checks
    routers = sum(c["routers"] for c in containers.values() if c["state"] == "running")
    for s in services:
        extra = []
        if s["type"] == "dns" or s["id"] == "pihole":
            q = (pihole or {}).get("queries") or {}
            freq = q.get("frequency")
            extra = [{"key": "qpm", "label": "Queries/min", "value": round(freq * 60) if freq is not None else None},
                     {"key": "blockedPct", "label": "Blocked", "unit": "%",
                      "value": round(q["percent_blocked"], 1) if q.get("percent_blocked") is not None else None},
                     {"key": "domains", "label": "Blocklist domains",
                      "value": ((pihole or {}).get("gravity") or {}).get("domains_being_blocked")}]
        elif s["id"] == "traefik":
            extra = [{"key": "routers", "label": "Routers", "value": routers}]
        if extra:
            s["metrics"] = extra + s["metrics"]
        if s["checks"]:
            ok = sum(1 for c in s["checks"] if c["success"])
            s["metrics"].append({"key": "checks", "label": "Checks", "value": f"{ok}/{len(s['checks'])} checks passing"})
            failing = [c for c in s["checks"] if not c["success"]]
            s["_reasons"] += [check_failure_text(c) for c in failing]
            if failing and s["status"] == "healthy":
                s["status"] = "degraded"
        s["statusReason"] = ("; ".join(s["_reasons"]) or None) if s["status"] != "healthy" else None
        s["checks"] = [{k: c[k] for k in ("name", "group", "success", "lastCheck", "durationMs", "hostname")}
                       for c in s["checks"]]

    # --- hidden services, logs index
    with LOCK:
        S["log_selectors"] = {s["id"]: s["_selector"] for s in services}
        S["service_ids"] = set(by_id)
    services = [s for s in services if not ov.get(s["id"], {}).get("hidden")]
    services.sort(key=lambda s: (s["category"], s["name"].lower()))

    events_and_alerts(services, gatus, check_sid, unmapped_failing, host_id, now)

    # --- KPIs and hourly history
    pool = zfs.get(SRV_POOL) or next(iter(zfs.values()), None)
    q = (pihole or {}).get("queries") or {}
    kpis = {
        "servicesTotal": len(services),
        "servicesUp": sum(1 for s in services if s["status"] == "healthy"),
        "servicesDegraded": sum(1 for s in services if s["status"] == "degraded"),
        "servicesDown": sum(1 for s in services if s["status"] == "down"),
        "dnsQueries24h": q.get("total"), "dnsBlocked24h": q.get("blocked"),
        "dnsBlockedPct": round(q["percent_blocked"], 2) if q.get("percent_blocked") is not None else None,
        "zfsUsedPct": pool and pool["usedPct"], "zfsUsedTB": pool and pool["usedTB"],
        "zfsSizeTB": pool and pool["sizeTB"], "zfsHealth": pool and pool["state"],
    }
    kpis["deltas"] = history_deltas(kpis, now)

    hw = hardware_name()
    host_obj = {
        "id": host_id, "name": host_id, "role": "primary", "hardware": hw,
        "os": dinfo.get("os"), "runtime": f"Docker {dinfo['version']}" if dinfo.get("version") else None,
        "storage": "ZFS" if zfs else "ext4", "ip": LAN_IP, "cpu": host.get("cpu"),
        "memUsedMB": host.get("memUsedMB"), "memTotalMB": host.get("memTotalMB"),
        "uptimeSec": host.get("uptimeSec"), "placeholder": False,
        "cores": dinfo.get("cpus") or os.cpu_count(), "load": host.get("load"), "zfsArcMB": host.get("zfsArcMB"),
    }
    with LOCK:
        events = list(S["events"])
        alerts = [{k: v for k, v in a.items() if not k.startswith("_")}
                  for a in sorted(S["alerts"].values(), key=lambda a: (not a["active"], -a["ts"]))[:MAX_ALERTS]]
    snap = {"hosts": [host_obj],
            "services": [{k: v for k, v in s.items() if not k.startswith("_")} for s in services],
            "events": events, "alerts": alerts, "kpis": kpis, "generatedAt": now, "source": "live"}
    S["snapshot"] = json.dumps(snap, separators=(",", ":")).encode()
    save_events()


def events_and_alerts(services, gatus, check_sid, unmapped_failing, host_id, now):
    """Service/Gatus transitions -> events; non-healthy services and orphan failing checks -> alerts."""
    with LOCK:
        prev, prev_checks, alerts = S["prev_status"], S["prev_checks"], S["alerts"]
        first = not prev
        for s in services:
            old = prev.get(s["id"])
            if old and old != s["status"] and not first:
                msg = (f"{s['name']} recovered" if s["status"] == "healthy" else
                       f"{s['name']} is {s['status']}" + (f": {s['statusReason']}" if s["statusReason"] else ""))
                add_event({"id": f"s-{now}-{s['id']}", "ts": now, "serviceId": s["id"], "hostId": host_id,
                           "kind": "status", "from": old, "to": s["status"], "message": msg})
        S["prev_status"] = {s["id"]: s["status"] for s in services}
        if S["gatus_ok"]:
            for ch in gatus:
                old = prev_checks.get(ch["key"])
                if old is not None and old != ch["success"]:
                    add_event({"id": f"g-{now}-{kebab(ch['key'])}", "ts": now, "serviceId": check_sid.get(ch["key"]),
                               "hostId": host_id, "kind": "status",
                               "message": f"Gatus: {ch['name']} recovered" if ch["success"] else check_failure_text(ch)})
            S["prev_checks"] = {ch["key"]: ch["success"] for ch in gatus}

        # Alerts: key -> wanted (severity, message)
        wanted = {}
        for s in services:
            if s["status"] != "healthy":
                sev = "critical" if s["critical"] and s["status"] == "down" else "warning"
                msg = f"{s['name']} {s['status']}" + (f": {s['statusReason']}" if s["statusReason"] else "")
                wanted[s["id"]] = (s["id"], sev, msg)
        for ch in unmapped_failing:
            wanted["check:" + ch["key"]] = (None, "warning", check_failure_text(ch))
        active = {a["_key"]: a for a in alerts.values() if a["active"]}
        for key, (sid, sev, msg) in wanted.items():
            a = active.get(key)
            if a:
                a.update(severity=sev, message=msg)
            else:
                aid = f"a-{kebab(key)}-{now}"
                alerts[aid] = {"id": aid, "serviceId": sid, "severity": sev, "message": msg, "ts": now,
                               "active": True, "_key": key}
        for key, a in active.items():
            if key not in wanted:
                a.update(active=False, resolvedAt=now)
        for aid in [aid for aid, a in alerts.items() if not a["active"] and now - a["resolvedAt"] > ALERT_KEEP * 1000]:
            del alerts[aid]


# --- Hourly KPI history (persisted) -> 24 h deltas
HISTORY_FILE = os.path.join(DATA, "history.json")
_history = {"samples": None}


def history_deltas(kpis, now):
    if _history["samples"] is None:
        try:
            with open(HISTORY_FILE) as f:
                _history["samples"] = [x for x in json.load(f) if isinstance(x, dict) and "ts" in x]
        except (OSError, ValueError):
            _history["samples"] = []
    samples = _history["samples"]
    if not samples or now - samples[-1]["ts"] >= 3600 * 1000:
        samples.append({"ts": now, "servicesUp": kpis["servicesUp"], "dnsBlockedPct": kpis["dnsBlockedPct"],
                        "zfsUsedPct": kpis["zfsUsedPct"]})
        samples[:] = [x for x in samples if now - x["ts"] <= HISTORY_KEEP * 1000]
        try:
            tmp = HISTORY_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(samples, f)
            os.replace(tmp, HISTORY_FILE)
        except OSError as e:
            log("warn", "history not persisted", error=repr(e))
    # the sample closest to 24 h ago, if one is within 22-26 h
    day = 24 * 3600 * 1000
    near = [x for x in samples if abs((now - x["ts"]) - day) <= 2 * 3600 * 1000]
    base = min(near, key=lambda x: abs((now - x["ts"]) - day)) if near else None

    def delta(k, nd):
        if not base or base.get(k) is None or kpis.get(k) is None:
            return None
        return round(kpis[k] - base[k], nd)
    return {"servicesUp": delta("servicesUp", 0), "dnsBlockedPct": delta("dnsBlockedPct", 2),
            "zfsUsedPct": delta("zfsUsedPct", 2)}


# --- HTTP server
def healthz():
    t = time.time()
    return {"ok": True, "sources": {
        "docker": t - S["docker_ok"] < 60,
        "gatus": t - S["gatus_ok"] < 3 * GATUS_EVERY,
        "pihole": (t - S["pihole_ok"] < 3 * PIHOLE_EVERY) if PIHOLE_PASSWORD else None,
        "loki": bool(S["loki_ok"]),
        "zfs": bool(S["zfs"]),
    }}


class Handler(BaseHTTPRequestHandler):
    server_version = "homedeck-collector"
    sys_version = ""

    def send(self, code, body):
        data = body if isinstance(body, bytes) else json.dumps(body, separators=(",", ":")).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):  # noqa: N802
        try:
            url = urllib.parse.urlsplit(self.path)
            qs = dict(urllib.parse.parse_qsl(url.query))
            if url.path == "/api/homedeck.json":
                self.send(200, S["snapshot"])
            elif url.path == "/api/healthz":
                self.send(200, healthz())
            elif url.path == "/api/logs":
                try:
                    limit = max(1, min(200, int(qs.get("limit", 50))))
                except ValueError:
                    limit = 50
                self.send(*query_logs(qs.get("service", ""), limit))
            else:
                self.send(404, {"error": "not found"})
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:  # noqa: BLE001
            log("error", "request failed", path=self.path, error=repr(e))
            try:
                self.send(500, {"error": "internal error"})
            except OSError:
                pass

    def log_message(self, fmt, *args):  # quiet access log; errors are logged explicitly
        pass


def main():
    log("info", "homedeck collector starting", port=PORT, docker=DOCKER_URL, gatus=GATUS_URL,
        pihole=PIHOLE_URL if PIHOLE_PASSWORD else None, loki=LOKI_URL)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # PID 1: exit promptly on docker stop
    os.makedirs(DATA, exist_ok=True)
    load_events()
    loop("host", host_sample, HOST_EVERY)
    loop("docker", docker_poll, DOCKER_EVERY, wake=DOCKER_WAKE)
    loop("gatus", gatus_poll, GATUS_EVERY)
    loop("pihole", Pihole.poll, PIHOLE_EVERY)
    loop("loki", loki_poll, LOKI_EVERY)

    def start_events():  # after the first Docker poll, so services and image ids are known
        for _ in range(30):
            if S["docker_ok"]:
                break
            time.sleep(1)
        try:
            assemble()
        except Exception as e:  # noqa: BLE001
            log("error", "assemble failed", error=repr(e))
        docker_events_forever()
    threading.Thread(target=start_events, name="docker-events", daemon=True).start()
    loop("assemble", assemble, HOST_EVERY)

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()
