import dagster as dg

from ppa_dagster.defs.schedules.ppa_pipeline import dbt_job, ppa_daily_ingest


@dg.run_status_sensor(run_status=dg.DagsterRunStatus.SUCCESS,
                      monitored_jobs=[ppa_daily_ingest],
                      request_job=dbt_job,
                      default_status=dg.DefaultSensorStatus.RUNNING)
def gate_dbt_on_ingest(context):
    return dg.RunRequest()