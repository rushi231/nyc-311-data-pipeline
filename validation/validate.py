"""
validation/validate.py

Two-layer validation:
  1. Pandera schema — enforces column types, nullable rules, allowed values
  2. Business-rule checks — null rates, date logic, value distributions

Returns a ValidationReport with pass/fail per check and a clean DataFrame.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dataclasses import dataclass, field
from typing import Optional
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
from loguru import logger



# PANDERA SCHEMA DEFINITION

ALLOWED_BOROUGHS = {
    "MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND", "Unspecified"
}

ALLOWED_STATUSES = {"Open", "Closed", "Pending", "In Progress", "Assigned"}

raw_schema = DataFrameSchema(
    columns={
        "unique_key": Column(
            str,
            nullable=False,
            checks=Check(lambda s: s.str.len() > 0, error="unique_key must not be empty"),
        ),
        "created_date": Column(str, nullable=False),
        "agency": Column(str, nullable=True),
        "complaint_type": Column(str, nullable=True),
        "borough": Column(
            str,
            nullable=True,
            checks=Check(
                lambda s: s.dropna().isin(ALLOWED_BOROUGHS | {"Unspecified"}),
                error=f"borough must be one of {ALLOWED_BOROUGHS}",
            ),
        ),
        "status": Column(
            str,
            nullable=True,
        ),
        "latitude": Column(str, nullable=True),
        "longitude": Column(str, nullable=True),
    },
    coerce=True,           # try to cast to declared types
    strict=False,          # allow extra columns
)



# VALIDATION REPORT

@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class ValidationReport:
    total_rows: int
    passed_rows: int
    failed_rows: int
    checks: list[CheckResult] = field(default_factory=list)
    clean_df: Optional[pd.DataFrame] = None

    @property
    def overall_passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def summary(self) -> str:
        lines = [
            f"Validation {'PASSED' if self.overall_passed else 'FAILED'}",
            f"  Rows: {self.total_rows} total, {self.passed_rows} clean, {self.failed_rows} dropped",
        ]
        for c in self.checks:
            icon = "✓" if c.passed else "✗"
            lines.append(f"  {icon} {c.name}: {c.detail}")
        return "\n".join(lines)


# ─────────────────────────────────────────
# INDIVIDUAL CHECKS
# ─────────────────────────────────────────

def check_no_duplicate_keys(df: pd.DataFrame) -> CheckResult:
    dupes = df["unique_key"].duplicated().sum()
    return CheckResult(
        name="no_duplicate_unique_keys",
        passed=dupes == 0,
        detail=f"{dupes} duplicate keys found",
    )


def check_null_rate(df: pd.DataFrame, column: str, max_null_pct: float = 0.1) -> CheckResult:
    null_pct = df[column].isnull().mean()
    return CheckResult(
        name=f"null_rate_{column}",
        passed=null_pct <= max_null_pct,
        detail=f"{null_pct:.1%} null (threshold {max_null_pct:.0%})",
    )


def check_date_logic(df: pd.DataFrame) -> CheckResult:
    """closed_date should be >= created_date when both are present."""
    has_both = df["created_date"].notna() & df["closed_date"].notna()
    if not has_both.any():
        return CheckResult(name="date_logic", passed=True, detail="no closed records to check")

    sub = df[has_both].copy()
    try:
        created = pd.to_datetime(sub["created_date"], errors="coerce", utc=True)
        closed  = pd.to_datetime(sub["closed_date"],  errors="coerce", utc=True)
        invalid = (closed < created).sum()
        return CheckResult(
            name="date_logic_closed_after_created",
            passed=invalid == 0,
            detail=f"{invalid} records where closed_date < created_date",
        )
    except Exception as e:
        return CheckResult(name="date_logic_closed_after_created", passed=False, detail=str(e))


def check_valid_boroughs(df: pd.DataFrame) -> CheckResult:
    unknown = df["borough"].dropna()
    unknown = unknown[~unknown.isin(ALLOWED_BOROUGHS)]
    count = len(unknown)
    return CheckResult(
        name="valid_borough_values",
        passed=count == 0,
        detail=f"{count} unrecognised borough values: {unknown.unique().tolist()[:5]}",
    )


def check_coordinate_ranges(df: pd.DataFrame) -> CheckResult:
    """NYC roughly: lat 40.4–40.9, lon -74.3–-73.7"""
    issues = 0
    if "latitude" in df.columns and "longitude" in df.columns:
        lat = pd.to_numeric(df["latitude"], errors="coerce").dropna()
        lon = pd.to_numeric(df["longitude"], errors="coerce").dropna()
        issues += ((lat < 40.4) | (lat > 40.9)).sum()
        issues += ((lon < -74.3) | (lon > -73.7)).sum()
    return CheckResult(
        name="coordinate_ranges_nyc",
        passed=issues == 0,
        detail=f"{issues} coordinates outside NYC bounding box",
    )


# ─────────────────────────────────────────
# MAIN VALIDATE FUNCTION
# ─────────────────────────────────────────

def validate_raw_dataframe(df: pd.DataFrame) -> ValidationReport:
    """
    Run all validation checks on a raw DataFrame.
    Returns a ValidationReport with a clean_df (rows that passed).
    """
    total = len(df)
    checks: list[CheckResult] = []
    drop_mask = pd.Series(False, index=df.index)

    # 1. Pandera schema validation
    try:
        raw_schema.validate(df, lazy=True)
        checks.append(CheckResult(name="pandera_schema", passed=True, detail="all columns valid"))
    except pa.errors.SchemaErrors as e:
        failed_idx = e.failure_cases["index"].dropna().astype(int).unique()
        drop_mask.iloc[failed_idx] = True
        checks.append(CheckResult(
            name="pandera_schema",
            passed=False,
            detail=f"{len(failed_idx)} rows failed schema ({len(e.failure_cases)} violations)",
        ))

    # 2. Drop rows missing unique_key (can't load without PK)
    missing_pk = df["unique_key"].isnull() | (df["unique_key"].str.strip() == "")
    drop_mask |= missing_pk
    checks.append(CheckResult(
        name="unique_key_present",
        passed=missing_pk.sum() == 0,
        detail=f"{missing_pk.sum()} rows missing unique_key (dropped)",
    ))

    # 3. Business rule checks (informational — don't drop rows)
    checks.append(check_no_duplicate_keys(df))
    checks.append(check_null_rate(df, "complaint_type", max_null_pct=0.05))
    checks.append(check_null_rate(df, "borough", max_null_pct=0.15))
    checks.append(check_date_logic(df))
    checks.append(check_valid_boroughs(df))
    checks.append(check_coordinate_ranges(df))

    clean_df = df[~drop_mask].copy()
    failed_rows = drop_mask.sum()

    report = ValidationReport(
        total_rows=total,
        passed_rows=len(clean_df),
        failed_rows=int(failed_rows),
        checks=checks,
        clean_df=clean_df,
    )

    logger.info(report.summary())
    return report


if __name__ == "__main__":
    # Quick smoke test with dummy data
    sample = pd.DataFrame([
        {
            "unique_key": "12345", "created_date": "2024-01-01T10:00:00",
            "closed_date": "2024-01-02T10:00:00", "agency": "NYPD",
            "complaint_type": "Noise - Residential", "borough": "MANHATTAN",
            "status": "Closed", "latitude": "40.75", "longitude": "-73.99",
        },
        {
            "unique_key": "", "created_date": "2024-01-01T10:00:00",  # bad: empty key
            "closed_date": None, "agency": "DEP",
            "complaint_type": "HEAT/HOT WATER", "borough": "BROOKLYN",
            "status": "Open", "latitude": None, "longitude": None,
        },
    ])
    report = validate_raw_dataframe(sample)
    print(report.summary())
    print(f"\nClean rows: {len(report.clean_df)}")
