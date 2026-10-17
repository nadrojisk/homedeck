# Two images from one file: `web` (nginx serving the page, proxying /api/) and `collector` (the read-only backend).
# Built by the homelab's deploy straight from this repo at a pinned commit (homelab: homedeck/docker-compose.yml).

FROM nginx:1.31-alpine@sha256:df221db836e1754089190208cee7eeda94f233197056426eda74a43ab1abeac2 AS web
COPY web/nginx.conf /etc/nginx/conf.d/default.conf
COPY web/homedeck.html /usr/share/nginx/html/index.html

FROM python:3.14-alpine@sha256:f6a589d43c42b9e7f7dc67a12d37132491f362859a5d750607710cc56da3bc72 AS collector
COPY collector/homedeck_collector.py /app/homedeck_collector.py
CMD ["python", "-u", "/app/homedeck_collector.py"]
