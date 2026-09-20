import dagster as dg

SLING_TABLES = ["categories","products","customers","devices","sales",
                "sale_revisions","payments","payment_methods","sale_revision_items"]
INGEST = dg.AssetSelection.keys(
    *[dg.AssetKey(["target", "public", t]) for t in SLING_TABLES],
    dg.AssetKey("schema_drift"),
)


ppa_daily_ingest = dg.define_asset_job(name="ppa_daily", selection=INGEST)

ppa_daily_schedule = dg.ScheduleDefinition(
    job=ppa_daily_ingest,
    cron_schedule="0 4 * * *",
    execution_timezone="Asia/Jakarta",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="Daily Supabase -> Sling -> schema_drift gate"
)


dbt_job = dg.define_asset_job(name="ppa_dbt_build", selection=dg.AssetSelection.all()- INGEST)