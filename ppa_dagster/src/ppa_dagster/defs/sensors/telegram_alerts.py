import json
import os
import urllib.request

import dagster as dg


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


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def telegram_success_note(context: dg.RunStatusSensorContext) -> None:
    """Notify on Telegram for any successful run."""
    run = context.dagster_run
    _send_telegram(
        "Dagster run succeeded\n"
        f"job: {run.job_name}\n"
        f"run: {run.run_id}"
    )


@dg.run_failure_sensor(default_status=dg.DefaultSensorStatus.RUNNING)
def telegram_fail_alert(context: dg.RunFailureSensorContext) -> None:
    """Alert on Telegram for any run failure.

    Turn this sensor ON in Dagster UI > Automation > Sensors.
    """
    run = context.dagster_run
    failed_steps = [event.step_key for event in context.get_step_failure_events()]
    _send_telegram(
        "Dagster run failed\n"
        f"job: {run.job_name}\n"
        f"run: {run.run_id}\n"
        f"failed steps: {', '.join(failed_steps) if failed_steps else 'unknown'}\n"
        f"{context.failure_event.message}"
    )
