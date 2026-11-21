# Two images from one file: `web` (nginx serving the page, proxying /api/) and `collector` (the read-only backend).
# Built by the homelab's deploy straight from this repo at a pinned commit (homelab: homedeck/docker-compose.yml).

FROM nginx:1.31-alpine@sha256:df221db836e1754089190208cee7eeda94f233197056426eda74a43ab1abeac2 AS web
COPY web/nginx.conf /etc/nginx/conf.d/default.conf
COPY web/homedeck.html /usr/share/nginx/html/index.html

FROM python:3.15-alpine@sha256:0f9552bb28711333da16915ec146f5e6bb44a7fdac9ced15dc17c2e0206346dd AS collector
COPY collector/homedeck_collector.py /app/homedeck_collector.py
CMD ["python", "-u", "/app/homedeck_collector.py"]
