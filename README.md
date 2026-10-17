# HomeDeck

A themed dashboard for the homelab: an isometric starbase, Deep Space 9 with docked ships, and a full-screen LCARS display
(`/starbase`, `/ds9`, `/lcars`), fed live by a small read-only collector.

## Layout

- `web/homedeck.html`: the page (one file, built output) and `web/nginx.conf`, which serves it and proxies `/api/` to the collector.
- `collector/homedeck_collector.py`: Python standard library only. It polls Docker (through a read-only socket proxy), Gatus,
  Pi-hole, Loki, host `/proc` and ZFS, and serves `/api/homedeck.json`, `/api/logs` and `/api/healthz`. It reads an optional
  `services.json` next to itself; the homelab mounts its own there.
- `Dockerfile`: two targets, `web` and `collector`.

## How it is deployed

The homelab repo (`homedeck/docker-compose.yml`) builds both targets from this repo at a pinned commit, with `HOMEDECK_REF` in
its `homedeck/.env.example`. To ship a change: push here, then bump `HOMEDECK_REF` there to the new commit.

```bash
docker build --target web -t homedeck-web .
docker build --target collector -t homedeck-collector .
```
