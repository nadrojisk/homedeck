# Two images from one file: `web` (nginx serving the page, proxying /api/) and `collector` (the read-only backend).
# Built by the homelab's deploy straight from this repo at a pinned commit (homelab: homedeck/docker-compose.yml).

FROM nginx:1.31-alpine@sha256:df221db836e1754089190208cee7eeda94f233197056426eda74a43ab1abeac2 AS web
COPY web/nginx.conf /etc/nginx/conf.d/default.conf
COPY web/homedeck.html /usr/share/nginx/html/index.html

FROM python:3.12-alpine@sha256:1b668429b3511ab407d8e00648891631b0b1a4d7e15e3ca70f38ab5b91ad4ab4 AS collector
COPY collector/homedeck_collector.py /app/homedeck_collector.py
CMD ["python", "-u", "/app/homedeck_collector.py"]
