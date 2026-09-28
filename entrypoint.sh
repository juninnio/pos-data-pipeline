#!/bin/sh
# Container entrypoint: prepare dbt state, then run daemon + webserver.
set -e

# Generate the dbt manifest with the RUNTIME env (DBT_TARGET, DUCKDB_* from
# compose). DbtProjectComponent reads target/manifest.json at defs-load time,
# and the manifest embeds target-dependent values — so it must be built here,
# not baked into the image. `dbt deps` output is already baked (see Dockerfile).
# Fails fast: if parse errors, the container exits instead of boot-looping
# the code server with "manifest.json does not exist".
cd /opt/ppa/ppa_dbt
dbt parse --profiles-dir .

cd /opt/ppa
# daemon (schedules: ppa_daily; sensors: gate_dbt_on_ingest, telegram) in the
# background; webserver (UI :3000) in the foreground as PID 1.
# `dg dev` is intentionally NOT used here — it is a local-dev-only server.
dagster-daemon run -w /opt/ppa/workspace.yaml &
exec dagster-webserver -h 0.0.0.0 -p 3000 -w /opt/ppa/workspace.yaml
