"""
orchestration/pipeline_flow.py

Prefect 2 flow that orchestrates the full ETL pipeline:
  ingest → validate → transform → audit log

Run manually:    python orchestration/pipeline_flow.py
Schedule it:     prefect deployment build ... (see README)
Monitor at:      http://localhost:4200  (Prefect UI)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime
import pandas as pd
from loguru import logger
from sqlalchemy import text

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.schedules import CronSchedule
from datetime import timedelta

from config import get_engine, test_connection
from ingestion.fetch_api import NYC311Client, upsert_raw_batch, RAW_COLUMNS
from validation.validate import validate_raw_dataframe
from transform.transform import run_transform


# ─────────────────────────────────────────
# TASKS  (each @task gets retry logic + logging in Prefect UI)
# ─────────────────────────────────────────

@task(
    name="check-db-connection",
    retries=3,
    retry_delay_seconds=10,
)
def task_check_db():
    log = get_run_logger()
    if not test_connection():
        raise RuntimeError("Cannot connect to PostgreSQL. Check .env and docker-compose.")
    log.info("Database connection OK")


@task(
    name="fetch-from-api",
    retries=2,
    retry_delay_seconds=30,
)
def task_fetch(days_back: int) -> pd.DataFrame:
    """Fetch all pages from the NYC 311 API and return a combined DataFrame."""
    log = get_run_logger()
    client = NYC311Client()
    all_rows = []

    for page in client.fetch_all(days_back=days_back):
        all_rows.extend(page)

    df = pd.DataFrame(all_rows)

    # Ensure all expected columns exist
    for col in RAW_COLUMNS:
        if col not in df.columns:
            df[col] = None

    log.info(f"Fetched {len(df)} total rows from API")
    return df


@task(name="validate-data")
def task_validate(df: pd.DataFrame):
    """Run validation checks. Fails the task if critical checks don't pass."""
    log = get_run_logger()
    report = validate_raw_dataframe(df)

    log.info(report.summary())

    # Hard failure: if more than 50% of rows were dropped, something is wrong
    if report.total_rows > 0:
        drop_pct = report.failed_rows / report.total_rows
        if drop_pct > 0.5:
            raise ValueError(
                f"Validation dropped {drop_pct:.0%} of rows — exceeds 50% threshold. "
                f"Investigate raw data quality."
            )

    return report


@task(name="save-raw-to-db")
def task_save_raw(df: pd.DataFrame) -> int:
    """Upsert the raw validated DataFrame into raw.service_requests."""
    log = get_run_logger()
    engine = get_engine()
    loaded = upsert_raw_batch(df, engine)
    log.info(f"Saved {loaded} raw rows to database")
    return loaded


@task(name="transform-and-load-marts")
def task_transform(clean_df: pd.DataFrame) -> dict:
    """Run cleaning, staging load, and mart load."""
    log = get_run_logger()
    result = run_transform(clean_df)
    log.info(f"Transform result: {result}")
    return result


@task(name="write-pipeline-audit-log")
def task_audit_log(
    run_start: datetime,
    status: str,
    rows_fetched: int,
    rows_loaded: int,
    rows_failed_validation: int,
    error_message: str = None,
):
    """Write a row to raw.pipeline_runs for observability."""
    engine = get_engine()
    sql = text("""
        INSERT INTO raw.pipeline_runs
            (run_started_at, run_finished_at, status,
             rows_fetched, rows_loaded, rows_failed_validation, error_message)
        VALUES
            (:started, NOW(), :status,
             :fetched, :loaded, :failed_val, :error)
    """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "started":    run_start,
            "status":     status,
            "fetched":    rows_fetched,
            "loaded":     rows_loaded,
            "failed_val": rows_failed_validation,
            "error":      error_message,
        })


# ─────────────────────────────────────────
# MAIN FLOW
# ─────────────────────────────────────────

@flow(
    name="nyc-311-etl-pipeline",
    description="Ingest → Validate → Transform → Load NYC 311 service requests",
    # Remove the schedule= parameter to run manually; add it for automation:
    # schedule=CronSchedule(cron="0 6 * * *", timezone="America/New_York"),
)
def nyc_311_pipeline(days_back: int = 7):
    """
    Full ETL pipeline for NYC 311 data.

    Args:
        days_back: how many days of data to pull on each run (default 7)

    Flow steps:
        1. Verify DB connectivity
        2. Fetch from NYC Open Data API (paginated)
        3. Validate schema + business rules
        4. Save raw data to PostgreSQL
        5. Clean + transform into star schema
        6. Write audit log
    """
    run_start = datetime.utcnow()
    rows_fetched = 0
    rows_loaded = 0
    rows_failed = 0

    try:
        # Step 1 — DB health check
        task_check_db()

        # Step 2 — Ingest
        raw_df = task_fetch(days_back=days_back)
        rows_fetched = len(raw_df)

        # Step 3 — Validate
        report = task_validate(raw_df)
        rows_failed = report.failed_rows
        clean_df = report.clean_df

        # Step 4 — Save raw
        task_save_raw(raw_df)

        # Step 5 — Transform + load marts
        transform_result = task_transform(clean_df)
        rows_loaded = transform_result.get("fact_rows", 0)

        # Step 6 — Audit log (success)
        task_audit_log(
            run_start=run_start,
            status="success",
            rows_fetched=rows_fetched,
            rows_loaded=rows_loaded,
            rows_failed_validation=rows_failed,
        )

        logger.success(
            f"Pipeline complete — {rows_fetched} fetched, "
            f"{rows_failed} failed validation, {rows_loaded} loaded to marts"
        )

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        task_audit_log(
            run_start=run_start,
            status="failed",
            rows_fetched=rows_fetched,
            rows_loaded=rows_loaded,
            rows_failed_validation=rows_failed,
            error_message=str(e),
        )
        raise


# ─────────────────────────────────────────
# SCHEDULED DEPLOYMENT HELPER
# ─────────────────────────────────────────

def deploy_scheduled():
    """
    Call this once to register a daily 6am schedule in Prefect.
    Usage:  python orchestration/pipeline_flow.py --deploy
    """
    from prefect.deployments import Deployment
    from prefect.server.schemas.schedules import CronSchedule

    deployment = Deployment.build_from_flow(
        flow=nyc_311_pipeline,
        name="daily-6am-et",
        schedule=CronSchedule(cron="0 6 * * *", timezone="America/New_York"),
        parameters={"days_back": 1},   # pull only yesterday on scheduled runs
    )
    deployment.apply()
    print("Deployment registered. View at http://localhost:4200")


if __name__ == "__main__":
    import sys
    if "--deploy" in sys.argv:
        deploy_scheduled()
    else:
        # Manual run — pull 7 days of data
        nyc_311_pipeline(days_back=7)
