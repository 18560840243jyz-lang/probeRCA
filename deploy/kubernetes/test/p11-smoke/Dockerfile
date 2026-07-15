FROM python:3.11-alpine@sha256:cf0253107a1e77e63c814a05428308965a47eb44a2d62fc828564c4a8c839fab
WORKDIR /app
COPY --chown=65534:65534 metrics_app.py /app/metrics_app.py
USER 65534
ENTRYPOINT ["python3", "/app/metrics_app.py"]
