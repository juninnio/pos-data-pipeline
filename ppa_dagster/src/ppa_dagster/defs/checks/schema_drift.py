import os
from pathlib import Path

import dagster as dg
import duckdb
import psycopg2

# (_sling_*) or pipeline-added cursor columns. Excluded from the diff.
IGNORED_COLUMNS = {
    "_sling_loaded_at"
}
TABLE_IGNORED_COLUMNS = {
    # Custom-SQL stream adds the join cursor to the target table.
    "sale_revision_items": {"revision_created_at"},
}
# Tables replicated by Sling (streams in sling_ingest/replication.yaml).

TABLES = [
    "categories",
    "products",
    "customers",
    "devices",
    "sales",
    "sale_revisions",
    "sale_revision_items",
    "payments",
    "payment_methods",
]


def _duckdb_path() -> str:
    """Resolve DUCKDB_DEV_DATABASE to an absolute path.

    Accepts both forms so the same code runs everywhere:
    - absolute (Docker: /data/ppa-dev.duckdb, old local .env) -> used as-is
    - relative (local: ../ppa-dev) -> resolved against the ppa_dagster/
      project dir, i.e. independent of the process CWD (dg dev, daemon,
      and `dbt run` all start from different directories).
    """
    raw = os.environ["DUCKDB_DEV_DATABASE"]
    p = Path(raw).expanduser()
    if not p.is_absolute():
        # schema_drift.py -> parents[4] == ppa_dagster/ project dir
        project_dir = Path(__file__).resolve().parents[4]
        p = (project_dir / p).resolve()
    return str(p)


@dg.asset(
    # Run after Sling ingestion so we compare fresh data.
    deps=[
        dg.AssetKey(["target", "public", "categories"]),
        dg.AssetKey(["target", "public", "products"]),
        dg.AssetKey(["target", "public", "customers"]),
        dg.AssetKey(["target", "public", "devices"]),
        dg.AssetKey(["target", "public", "sales"]),
        dg.AssetKey(["target", "public", "sale_revisions"]),
        dg.AssetKey(["target", "public", "payments"]),
        dg.AssetKey(["target", "public", "payment_methods"]),
        dg.AssetKey(["target", "public", "sale_revision_items"]),
    ],
    group_name="monitoring",
    description=(
        "Fails if Supabase columns drift from DuckDB raw tables. "
        "A rename shows as 1 added + 1 missing on the same table."
    ),
)
def schema_drift(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Compare Postgres public.* vs DuckDB raw.* column sets."""
    pg_conn = psycopg2.connect(
        host=os.environ["DBT_PROD_HOST"],
        user=os.environ["DB_SLING_USER"],
        password=os.environ["DB_SLING_PASSWORD"],
        dbname="postgres",
        port=5432,
    )
    # Same file Sling writes and dbt reads (see _duckdb_path).
    duck_conn = duckdb.connect(_duckdb_path())

    drift: dict[str, dict[str, list[str]]] = {}
    column_counts: dict[str, int] = {}
    try:
        with pg_conn.cursor() as cur:
            for table in TABLES:
                cur.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                    """,
                    (table,),
                )
                src = {row[0] for row in cur.fetchall()}
                dst = {
                    row[0]
                    for row in duck_conn.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = 'raw' AND table_name = ?",
                        [table],
                    ).fetchall()
                } - IGNORED_COLUMNS - TABLE_IGNORED_COLUMNS.get(table, set())
                column_counts[table] = len(dst)
                added, missing = sorted(src - dst), sorted(dst - src)
                if added or missing:
                    drift[table] = {
                        "added_in_postgres": added,
                        "missing_from_duckdb": missing,
                    }
    finally:
        pg_conn.close()
        duck_conn.close()

    if drift:
        context.log.error(f"Schema drift detected: {drift}")
        raise Exception(f"Schema drift detected: {drift}")

    context.log.info(f"Schema check clean for {len(TABLES)} tables")
    return dg.MaterializeResult(
        metadata={
            "tables_checked": dg.MetadataValue.int(len(TABLES)),
            "column_counts": dg.MetadataValue.json(column_counts),
        }
    )
