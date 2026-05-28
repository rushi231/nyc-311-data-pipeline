"""
tests/test_transform.py

Unit tests for the validation and transform layers.
Run:  pytest tests/ -v --cov=. --cov-report=term-missing
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
import pandas as pd
import numpy as np

from validation.validate import (
    validate_raw_dataframe,
    check_no_duplicate_keys,
    check_null_rate,
    check_date_logic,
    check_valid_boroughs,
    check_coordinate_ranges,
)
from transform.transform import clean_raw


# ─────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────

@pytest.fixture
def valid_row():
    return {
        "unique_key": "10001",
        "created_date": "2024-01-15T08:00:00",
        "closed_date":  "2024-01-16T10:30:00",
        "due_date":     "2024-01-20T08:00:00",
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
    }


@pytest.fixture
def sample_df(valid_row):
    return pd.DataFrame([
        valid_row,
        {**valid_row, "unique_key": "10002", "borough": "BROOKLYN", "status": "Open"},
        {**valid_row, "unique_key": "10003", "borough": "QUEENS",   "latitude": "40.72"},
    ])


# ─────────────────────────────────────────
# VALIDATION TESTS
# ─────────────────────────────────────────

class TestDuplicateKeyCheck:
    def test_no_duplicates_passes(self, sample_df):
        result = check_no_duplicate_keys(sample_df)
        assert result.passed

    def test_duplicate_fails(self, sample_df):
        duped = pd.concat([sample_df, sample_df.iloc[[0]]], ignore_index=True)
        result = check_no_duplicate_keys(duped)
        assert not result.passed
        assert "1 duplicate" in result.detail


class TestNullRateCheck:
    def test_low_null_rate_passes(self, sample_df):
        result = check_null_rate(sample_df, "complaint_type", max_null_pct=0.1)
        assert result.passed

    def test_high_null_rate_fails(self, sample_df):
        df = sample_df.copy()
        df["complaint_type"] = None  # 100% null
        result = check_null_rate(df, "complaint_type", max_null_pct=0.05)
        assert not result.passed


class TestDateLogicCheck:
    def test_valid_dates_pass(self, sample_df):
        result = check_date_logic(sample_df)
        assert result.passed

    def test_closed_before_created_fails(self, valid_row):
        bad = pd.DataFrame([{
            **valid_row,
            "created_date": "2024-01-20T00:00:00",
            "closed_date":  "2024-01-01T00:00:00",  # closed BEFORE created
        }])
        result = check_date_logic(bad)
        assert not result.passed


class TestBoroughCheck:
    def test_valid_boroughs_pass(self, sample_df):
        result = check_valid_boroughs(sample_df)
        assert result.passed

    def test_unknown_borough_fails(self, valid_row):
        df = pd.DataFrame([{**valid_row, "borough": "NEW JERSEY"}])
        result = check_valid_boroughs(df)
        assert not result.passed


class TestCoordinateCheck:
    def test_valid_nyc_coords_pass(self, sample_df):
        result = check_coordinate_ranges(sample_df)
        assert result.passed

    def test_out_of_range_coords_fail(self, valid_row):
        df = pd.DataFrame([{**valid_row, "latitude": "34.05", "longitude": "-118.24"}])
        result = check_coordinate_ranges(df)
        assert not result.passed


class TestFullValidation:
    def test_clean_df_drops_missing_pk(self, valid_row):
        df = pd.DataFrame([
            valid_row,
            {**valid_row, "unique_key": ""},   # empty PK — should be dropped
            {**valid_row, "unique_key": None},  # null PK — should be dropped
        ])
        report = validate_raw_dataframe(df)
        assert len(report.clean_df) == 1
        assert report.failed_rows == 2

    def test_valid_data_all_pass(self, sample_df):
        report = validate_raw_dataframe(sample_df)
        assert len(report.clean_df) == len(sample_df)
        assert report.failed_rows == 0


# ─────────────────────────────────────────
# TRANSFORM TESTS
# ─────────────────────────────────────────

class TestCleanRaw:
    def test_basic_cleaning(self, valid_row):
        df = pd.DataFrame([valid_row])
        clean = clean_raw(df)
        assert len(clean) == 1
        assert clean["unique_key"].iloc[0] == "10001"

    def test_timestamps_parsed(self, valid_row):
        df = pd.DataFrame([valid_row])
        clean = clean_raw(df)
        assert pd.api.types.is_datetime64_any_dtype(clean["created_date"])
        assert clean["created_date"].dt.tz is not None  # timezone-aware

    def test_resolution_hours_computed(self, valid_row):
        df = pd.DataFrame([valid_row])
        clean = clean_raw(df)
        # 2024-01-15 08:00 to 2024-01-16 10:30 = 26.5 hours
        assert abs(clean["resolution_hours"].iloc[0] - 26.5) < 0.1

    def test_resolution_hours_null_when_open(self, valid_row):
        row = {**valid_row, "closed_date": None, "status": "Open"}
        df = pd.DataFrame([row])
        clean = clean_raw(df)
        assert pd.isna(clean["resolution_hours"].iloc[0])

    def test_negative_resolution_clamped_to_zero(self, valid_row):
        row = {
            **valid_row,
            "created_date": "2024-01-20T00:00:00",
            "closed_date":  "2024-01-15T00:00:00",  # before created
        }
        df = pd.DataFrame([row])
        clean = clean_raw(df)
        assert clean["resolution_hours"].iloc[0] == 0.0

    def test_resolution_over_one_year_clamped(self, valid_row):
        row = {
            **valid_row,
            "created_date": "2020-01-01T00:00:00",
            "closed_date":  "2024-01-01T00:00:00",
        }
        df = pd.DataFrame([row])
        clean = clean_raw(df)
        assert clean["resolution_hours"].iloc[0] == 8760.0  # capped at 1 year

    def test_borough_normalised_to_upper(self, valid_row):
        df = pd.DataFrame([{**valid_row, "borough": "brooklyn"}])
        clean = clean_raw(df)
        assert clean["borough"].iloc[0] == "BROOKLYN"

    def test_missing_borough_filled_unspecified(self, valid_row):
        df = pd.DataFrame([{**valid_row, "borough": None}])
        clean = clean_raw(df)
        assert clean["borough"].iloc[0] == "Unspecified"

    def test_missing_pk_rows_dropped(self, valid_row):
        df = pd.DataFrame([
            valid_row,
            {**valid_row, "unique_key": ""},
        ])
        clean = clean_raw(df)
        assert len(clean) == 1

    def test_is_overdue_true_when_closed_late(self, valid_row):
        row = {
            **valid_row,
            "closed_date": "2024-01-25T00:00:00",
            "due_date":    "2024-01-20T00:00:00",  # closed 5 days late
        }
        df = pd.DataFrame([row])
        clean = clean_raw(df)
        assert clean["is_overdue"].iloc[0] == True

    def test_is_overdue_false_when_on_time(self, valid_row):
        df = pd.DataFrame([valid_row])  # due 2024-01-20, closed 2024-01-16
        clean = clean_raw(df)
        assert clean["is_overdue"].iloc[0] == False

    def test_latitude_longitude_cast_to_float(self, valid_row):
        df = pd.DataFrame([valid_row])
        clean = clean_raw(df)
        assert clean["latitude"].dtype == float
        assert clean["longitude"].dtype == float

    def test_bad_coordinates_become_nan(self, valid_row):
        df = pd.DataFrame([{**valid_row, "latitude": "not_a_number"}])
        clean = clean_raw(df)
        assert pd.isna(clean["latitude"].iloc[0])
