# PPA Pipeline image: Dagster (webserver + daemon) + Sling + dbt-duckdb.
# Build context is the repo root:  docker build -t ppa-pipeline .
# Run via docker-compose.yml — never `docker run` bare (paths/secrets come from compose).

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Jakarta \
    DEBIAN_FRONTEND=noninteractive

# gcc: build any source-distribution wheels; libpq-dev: psycopg2 headers;
# curl: container HEALTHCHECK; tzdata: correct local time for the 04:00 schedule.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev curl tzdata \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/ppa

# Lean direct-only deps (see requirements.txt). Install first for layer caching —
# rebuilding after a code change reuses this layer.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ppa_dagster/ ./ppa_dagster/
COPY ppa_dbt/ ./ppa_dbt/
RUN pip install --no-cache-dir -e ./ppa_dagster

# Bake dbt packages (dbt_utils, dbt_expectations) so startup needs no network.
# The manifest itself is NOT baked — entrypoint.sh generates it with runtime env.
RUN cd ./ppa_dbt && dbt deps --profiles-dir .

# Code location importable without install path hacks; instance state dir.
ENV DAGSTER_HOME=/opt/dagster/dagster_home \
    PYTHONPATH=/opt/ppa/ppa_dagster/src

COPY dagster.yaml /opt/dagster/dagster_home/dagster.yaml
COPY workspace.yaml /opt/ppa/workspace.yaml
COPY entrypoint.sh /opt/ppa/entrypoint.sh
RUN chmod +x /opt/ppa/entrypoint.sh

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:3000/server_info || exit 1

CMD ["/opt/ppa/entrypoint.sh"]
