FROM python:3.10-slim@sha256:e5300dc020a26a34a19337a57602955a2510e22abeb176edd6de6cd2cc927dd4

ARG CODE_REVISION
ARG SOURCE_FINGERPRINT
ARG SCHEMA_VERSION=generation_v5
ARG PIP_INDEX_URL=https://pypi.org/simple

LABEL org.opencontainers.image.revision="${CODE_REVISION}" \
      io.proberca.source-fingerprint="${SOURCE_FINGERPRINT}" \
      io.proberca.schema-version="${SCHEMA_VERSION}"

WORKDIR /app
COPY requirements/production.lock.txt /app/requirements/production.lock.txt
COPY requirements/build.lock.txt /app/requirements/build.lock.txt
RUN python3 -m pip install --no-cache-dir --require-hashes --timeout 60 --retries 3 \
      --index-url "${PIP_INDEX_URL}" -r /app/requirements/production.lock.txt \
 && python3 -m pip install --no-cache-dir --require-hashes --timeout 60 --retries 3 \
      --index-url "${PIP_INDEX_URL}" -r /app/requirements/build.lock.txt

COPY pyproject.toml /app/
COPY proberca /app/proberca
RUN python3 -m pip install --no-deps --no-build-isolation --no-cache-dir .

ENV PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PROBERCA_CODE_REVISION="${CODE_REVISION}" \
    PROBERCA_SOURCE_FINGERPRINT="${SOURCE_FINGERPRINT}" \
    PROBERCA_EXPECTED_SOURCE_FINGERPRINT="${SOURCE_FINGERPRINT}" \
    PROBERCA_SCHEMA_VERSION="${SCHEMA_VERSION}"

USER 65534
ENTRYPOINT ["python3", "-m", "proberca.cli.live"]
