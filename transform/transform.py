"""
transform/transform.py

Three stages:
  1. clean_raw()     — read raw.service_requests, apply type casting & derivations
  2. load_staging()  — upsert into staging.stg_service_requests
  3. load_marts()    — populate dim + fact tables (star schema)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import numpy as np
from sqlalchemy import text
from loguru import logger

from config import get_engine


# ─────────────────────────────────────────
# STEP 1 — CLEAN RAW → STAGING
# ─────────────────────────────────────────

def clean_raw(df: pd.DataFrame) -> pd.DataFrame:
    """
    Take a raw DataFrame and return a clean staging DataFrame.
    - Parse timestamps
    - Normalise text fields
    - Derive resolution_hours and is_overdue
    """
    out = pd.DataFrame()

    # Primary key
    out["unique_key"] = df["unique_key"].astype(str).str.strip()

    # Timestamps — coerce bad values to NaT, make timezone-aware (UTC)
    out["created_date"] = pd.to_datetime(df["created_date"], errors="coerce", utc=True)
    out["closed_date"]  = pd.to_datetime(df.get("closed_date"),  errors="coerce", utc=True)
    due_date            = pd.to_datetime(df.get("due_date"),      errors="coerce", utc=True)

    # Text normalisation
    out["agency_code"]     = df.get("agency",      pd.Series(dtype=str)).str.upper().str.strip()
    out["agency_name"]     = df.get("agency_name", pd.Series(dtype=str)).str.strip()
    out["complaint_type"]  = df.get("complaint_type", pd.Series(dtype=str)).str.strip()
    out["descriptor"]      = df.get("descriptor",     pd.Series(dtype=str)).str.strip()
    out["status"]          = df.get("status",          pd.Series(dtype=str)).str.strip()
    out["open_channel"]    = df.get("open_data_channel_type", pd.Series(dtype=str)).str.upper().str.strip()

    # Borough — upper-case, fill unknown
    out["borough"]      = (
        df.get("borough", pd.Series(dtype=str))
        .str.strip()
        .str.upper()
        .replace("", np.nan)
        .fillna("Unspecified")
    )
    out["incident_zip"] = df.get("incident_zip", pd.Series(dtype=str)).str.strip().str[:5]

    # Coordinates
    out["latitude"]  = pd.to_numeric(df.get("latitude"),  errors="coerce")
    out["longitude"] = pd.to_numeric(df.get("longitude"), errors="coerce")

    # Derived: resolution time in hours
    both_present = out["created_date"].notna() & out["closed_date"].notna()
    out["resolution_hours"] = np.where(
        both_present,
        (out["closed_date"] - out["created_date"]).dt.total_seconds() / 3600,
        np.nan,
    )
    # Cap outliers at 8760 hours (1 year) — data entry errors
    out["resolution_hours"] = out["resolution_hours"].clip(lower=0, upper=8760)

    # Derived: is_overdue (closed later than due_date)
    out["is_overdue"] = np.where(
        out["closed_date"].notna() & due_date.notna(),
        out["closed_date"] > due_date,
        False,
    )

    # Drop rows with no primary key
    out = out[out["unique_key"].notna() & (out["unique_key"] != "nan")]

    logger.info(f"Cleaned {len(out)} rows from {len(df)} raw rows")
    return out


def load_staging(df: pd.DataFrame, engine) -> int:
    """Upsert clean rows into staging.stg_service_requests."""
    if df.empty:
        return 0

    rows = df.to_dict(orient="records")

    sql = text("""
        INSERT INTO staging.stg_service_requests (
            unique_key, created_date, closed_date, agency_code, agency_name,
            complaint_type, descriptor, borough, incident_zip, status,
            open_channel, latitude, longitude, resolution_hours, is_overdue
        ) VALUES (
            :unique_key, :created_date, :closed_date, :agency_code, :agency_name,
            :complaint_type, :descriptor, :borough, :incident_zip, :status,
            :open_channel, :latitude, :longitude, :resolution_hours, :is_overdue
        )
        ON CONFLICT (unique_key) DO UPDATE SET
            status           = EXCLUDED.status,
            closed_date      = EXCLUDED.closed_date,
            resolution_hours = EXCLUDED.resolution_hours,
            is_overdue       = EXCLUDED.is_overdue,
            loaded_at        = NOW()
    """)

    with engine.begin() as conn:
        conn.execute(sql, rows)

    logger.info(f"Upserted {len(rows)} rows into staging.stg_service_requests")
    return len(rows)


# ─────────────────────────────────────────
# STEP 2 — STAGING → MART (star schema)
# ─────────────────────────────────────────

def load_dim_agency(engine) -> None:
    """Upsert any new agencies found in staging."""
    sql = text("""
        INSERT INTO marts.dim_agency (agency_code, agency_name)
        SELECT DISTINCT agency_code, agency_name
        FROM staging.stg_service_requests
        WHERE agency_code IS NOT NULL
        ON CONFLICT (agency_code) DO UPDATE
            SET agency_name = EXCLUDED.agency_name
    """)
    with engine.begin() as conn:
        conn.execute(sql)
    logger.debug("dim_agency refreshed")


def load_dim_complaint_type(engine) -> None:
    """Insert any new complaint types (category defaults to 'Other')."""
    sql = text("""
        INSERT INTO marts.dim_complaint_type (complaint_type, category)
        SELECT DISTINCT complaint_type, 'Other'
        FROM staging.stg_service_requests
        WHERE complaint_type IS NOT NULL
        ON CONFLICT (complaint_type) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(sql)
    logger.debug("dim_complaint_type refreshed")


def load_fact_table(engine) -> int:
    """
    Populate marts.fct_requests from staging, joining to dimension tables.
    Adds date part columns for fast dashboard filtering.
    """
    sql = text("""
        INSERT INTO marts.fct_requests (
            request_id, created_date, closed_date,
            agency_id, complaint_type_id, borough_id,
            status, open_channel,
            resolution_hours, is_overdue,
            latitude, longitude,
            created_year, created_month, created_day_of_week
        )
        SELECT
            s.unique_key,
            s.created_date,
            s.closed_date,
            a.agency_id,
            ct.complaint_type_id,
            b.borough_id,
            s.status,
            s.open_channel,
            s.resolution_hours,
            s.is_overdue,
            s.latitude,
            s.longitude,
            EXTRACT(YEAR  FROM s.created_date)::INT,
            EXTRACT(MONTH FROM s.created_date)::INT,
            EXTRACT(DOW   FROM s.created_date)::INT  -- 0=Sunday
        FROM staging.stg_service_requests s
        LEFT JOIN marts.dim_agency         a  ON s.agency_code    = a.agency_code
        LEFT JOIN marts.dim_complaint_type ct ON s.complaint_type = ct.complaint_type
        LEFT JOIN marts.dim_borough        b  ON s.borough        = b.borough
        ON CONFLICT (request_id) DO UPDATE SET
            status           = EXCLUDED.status,
            closed_date      = EXCLUDED.closed_date,
            resolution_hours = EXCLUDED.resolution_hours,
            is_overdue       = EXCLUDED.is_overdue,
            loaded_at        = NOW()
    """)
    with engine.begin() as conn:
        result = conn.execute(sql)
    count = result.rowcount
    logger.info(f"Upserted {count} rows into marts.fct_requests")
    return count


def run_transform(raw_df: pd.DataFrame) -> dict:
    """
    Full transform pipeline entry point.
    Accepts the validated raw DataFrame, cleans it, loads staging, loads marts.
    """
    engine = get_engine()

    # Clean
    clean_df = clean_raw(raw_df)

    # Staging
    staging_count = load_staging(clean_df, engine)

    # Dims first, then fact
    load_dim_agency(engine)
    load_dim_complaint_type(engine)
    fact_count = load_fact_table(engine)

    logger.success(
        f"Transform complete — {staging_count} staging rows, {fact_count} fact rows"
    )
    return {
        "staging_rows": staging_count,
        "fact_rows": fact_count,
    }


if __name__ == "__main__":
    # Test with a small synthetic DataFrame
    sample = pd.DataFrame([{
        "unique_key": "TEST001",
        "created_date": "2024-03-01T10:00:00",
        "closed_date":  "2024-03-02T14:30:00",
        "due_date":     "2024-03-03T10:00:00",
        "agency": "NYPD",
        "agency_name": "New York City Police Department",
        "complaint_type": "Noise - Residential",
        "descriptor": "Loud Music/Party",
        "borough": "MANHATTAN",
        "status": "Closed",
        "open_data_channel_type": "PHONE",
        "latitude": "40.7580",
        "longitude": "-73.9855",
        "incident_zip": "10036",
    }])
    result = run_transform(sample)
    print(result)
