# This docker file is s3-init image

# minio/mc saiu do Docker Hub
FROM pgsty/mc:RELEASE.2026-10-26T00-00-00Z AS mc

FROM python:3.13-slim

COPY --from=mc /usr/bin/mc /mc

RUN pip install "pyiceberg[s3fs,pyarrow]==0.10.0" \
    && chmod +x /mc
