import json
import os
import urllib.request

import dagster as dg

from ppa_dagster.defs.schedules.ppa_pipeline import SLING_TABLES, dbt_job, ppa_daily_ingest


def _send_telegram(text: str) -> None:
    """Post a message via the Telegram Bot API (stdlib only)."""
    # Read inside the body so missing creds can't break defs loading —
    # the sensor only errors if it actually fires.
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    payload = json.dumps({"chat_id": chat_id, "text": text}).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=15)


def _fmt_duration(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "n/a"
    total = int(seconds)
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def _run_duration_seconds(context) -> float | None:
    """Run wall time from run stats (DagsterRun itself carries no timestamps)."""
    try:
        stats = context.instance.get_run_stats(context.dagster_run.run_id)
        if stats.start_time and stats.end_time:
            return stats.end_time - stats.start_time
    except Exception as exc:
        context.log.warning(f"telegram sensor: could not fetch run stats: {exc}")
    return None


def _fetch_run_records(context, of_types: set) -> list:
    try:
        return context.instance.get_records_for_run(
            run_id=context.dagster_run.run_id, of_type=of_types
        ).records
    except Exception as exc:  # never let alert enrichment break the sensor
        context.log.warning(f"telegram sensor: could not fetch run records: {exc}")
        return []


def _materialized_names(records: list) -> list[str]:
    """Short asset names from ASSET_MATERIALIZATION records."""
    names: list[str] = []
    for record in records:
        try:
            event = record.event_log_entry.dagster_event
            if event.event_type_value != "ASSET_MATERIALIZATION":
                continue
            names.append(event.event_specific_data.materialization.asset_key.path[-1])
        except Exception:
            continue
    return names


def _summarize_checks(records: list) -> tuple[int, int, list[str]]:
    """Return (passed, failed, failed_check_names) for ASSET_CHECK_EVALUATION records."""
    passed = failed = 0
    failed_names: list[str] = []
    for record in records:
        try:
            event = record.event_log_entry.dagster_event
            if event.event_type_value != "ASSET_CHECK_EVALUATION":
                continue
            evaluation = event.event_specific_data
            if evaluation.passed:
                passed += 1
            else:
                failed += 1
                failed_names.append(
                    f"{evaluation.asset_key.path[-1]}.{evaluation.check_name}"
                )
        except Exception:
            continue
    return passed, failed, failed_names


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[ppa_daily_ingest],
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def sling_success_note(context: dg.RunStatusSensorContext) -> None:
    """Telegram summary for Sling ingest: tables materialized + duration."""
    run = context.dagster_run
    names = _materialized_names(
        _fetch_run_records(context, {dg.DagsterEventType.ASSET_MATERIALIZATION})
    )
    tables = sorted(set(names) & set(SLING_TABLES))
    body = "\n".join(
        [
            "Sling ingest succeeded",
            f"job: {run.job_name}",
            f"run: {run.run_id[:8]}",
            f"duration: {_fmt_duration(_run_duration_seconds(context))}",
            f"tables: {len(tables)}",
            *[f"- {name}" for name in tables],
        ]
    )
    _send_telegram(body)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[dbt_job],
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def dbt_success_note(context: dg.RunStatusSensorContext) -> None:
    """Telegram summary for dbt: models materialized + test results."""
    run = context.dagster_run
    records = _fetch_run_records(
        context,
        {
            dg.DagsterEventType.ASSET_MATERIALIZATION,
            dg.DagsterEventType.ASSET_CHECK_EVALUATION,
        },
    )
    models = sorted(set(_materialized_names(records)))
    passed, failed, _ = _summarize_checks(records)
    body = "\n".join(
        [
            "dbt build succeeded",
            f"job: {run.job_name}",
            f"run: {run.run_id[:8]}",
            f"duration: {_fmt_duration(_run_duration_seconds(context))}",
            f"models: {len(models)}",
            f"tests: {passed} passed, {failed} failed",
            *[f"- {name}" for name in models],
        ]
    )
    _send_telegram(body)


@dg.run_failure_sensor(default_status=dg.DefaultSensorStatus.RUNNING)
def telegram_fail_alert(context: dg.RunFailureSensorContext) -> None:
    """Alert on Telegram for any run failure.

    Turn this sensor ON in Dagster UI > Automation > Sensors.
    """
    run = context.dagster_run
    failed_steps = [event.step_key for event in context.get_step_failure_events()]
    records = _fetch_run_records(
        context,
        {
            dg.DagsterEventType.ASSET_MATERIALIZATION,
            dg.DagsterEventType.ASSET_CHECK_EVALUATION,
        },
    )
    models = _materialized_names(records)
    passed, failed, failed_names = _summarize_checks(records)
    extra = [
        f"duration: {_fmt_duration(_run_duration_seconds(context))}",
        f"materialized before failure: {len(set(models))}",
        f"tests: {passed} passed, {failed} failed",
    ]
    if failed_names:
        extra.append("failed tests: " + ", ".join(failed_names[:10]))
    message = str(getattr(context.failure_event, "message", ""))[:500]
    _send_telegram(
        "\n".join(
            [
                "Dagster run failed",
                f"job: {run.job_name}",
                f"run: {run.run_id[:8]}",
                f"failed steps: {', '.join(failed_steps) if failed_steps else 'unknown'}",
                *extra,
                message,
            ]
        )
    )
