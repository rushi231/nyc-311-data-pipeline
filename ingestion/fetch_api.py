"""
ingestion/fetch_api.py

Fetches NYC 311 service requests from the Socrata Open Data API.
- Paginates through results in configurable batch sizes
- Retries failed requests with exponential backoff
- Saves raw JSON rows to raw.service_requests in PostgreSQL
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime, timedelta
from typing import Generator
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from loguru import logger
import pandas as pd
from sqlalchemy import text

from config import config, get_engine



class NYC311Client:
    """
    Thin wrapper around the NYC Open Data (Socrata) REST API.
    Docs: https://dev.socrata.com/foundry/data.cityofnewyork.us/erm2-nwe9
    """

    BASE_URL = config.NYC_API_BASE_URL

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "X-App-Token": config.NYC_API_APP_TOKEN,
        })

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def _get_page(self, offset: int, limit: int, created_after: str) -> list[dict]:
        """Fetch a single page of results. Retries on network errors."""
        params = {
            "$limit":  limit,
            "$offset": offset,
            "$order":  "created_date DESC",
            "$where":  f"created_date > '{created_after}'",
        }
        response = self.session.get(self.BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def fetch_all(
        self,
        days_back: int = 7,
        batch_size: int = None,
        max_pages: int = None,
    ) -> Generator[list[dict], None, None]:
        """
        Generator — yields one page of records at a time.

        Args:
            days_back:  how far back to pull (default 7 days)
            batch_size: rows per API request
            max_pages:  hard cap on pages (safety valve)

        Yields:
            list of raw record dicts
        """
        batch_size = batch_size or config.BATCH_SIZE
        max_pages  = max_pages  or config.MAX_PAGES

        created_after = (datetime.utcnow() - timedelta(days=days_back)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        logger.info(
            f"Fetching records created after {created_after} "
            f"(batch_size={batch_size}, max_pages={max_pages})"
        )

        for page_num in range(max_pages):
            offset = page_num * batch_size
            logger.debug(f"Fetching page {page_num + 1} (offset={offset})")

            rows = self._get_page(offset, batch_size, created_after)

            if not rows:
                logger.info(f"No more results at page {page_num + 1}. Done.")
                break

            logger.info(f"Page {page_num + 1}: received {len(rows)} rows")
            yield rows

            if len(rows) < batch_size:
                # Last partial page — no point fetching another
                break



# Columns we want to keep from the API (ignore the noisy geo-coded extras)
RAW_COLUMNS = [
    "unique_key", "created_date", "closed_date", "agency", "agency_name",
    "complaint_type", "descriptor", "location_type", "incident_zip", "city",
    "borough", "status", "due_date", "resolution_description",
    "community_board", "open_data_channel_type", "park_facility_name",
    "latitude", "longitude",
]


def upsert_raw_batch(df: pd.DataFrame, engine) -> int:
    """
    Upsert a batch of raw rows into raw.service_requests.
    Uses INSERT ... ON CONFLICT DO UPDATE so re-runs are idempotent.

    Returns number of rows affected.
    """
    if df.empty:
        return 0

    # Keep only known columns; fill missing ones with None
    for col in RAW_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[RAW_COLUMNS].copy()

    rows = df.to_dict(orient="records")

    upsert_sql = text("""
        INSERT INTO raw.service_requests (
            unique_key, created_date, closed_date, agency, agency_name,
            complaint_type, descriptor, location_type, incident_zip, city,
            borough, status, due_date, resolution_description,
            community_board, open_data_channel_type, park_facility_name,
            latitude, longitude
        ) VALUES (
            :unique_key, :created_date, :closed_date, :agency, :agency_name,
            :complaint_type, :descriptor, :location_type, :incident_zip, :city,
            :borough, :status, :due_date, :resolution_description,
            :community_board, :open_data_channel_type, :park_facility_name,
            :latitude, :longitude
        )
        ON CONFLICT (unique_key) DO UPDATE SET
            status              = EXCLUDED.status,
            closed_date         = EXCLUDED.closed_date,
            resolution_description = EXCLUDED.resolution_description,
            raw_loaded_at       = NOW()
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, rows)

    return len(rows)


def run_ingestion(days_back: int = 7) -> dict:
    """
    Main ingestion entry point.
    Returns a summary dict with rows fetched / loaded / errors.
    """
    client = NYC311Client()
    engine = get_engine()
    total_fetched = 0
    total_loaded  = 0

    try:
        for page in client.fetch_all(days_back=days_back):
            df = pd.DataFrame(page)
            loaded = upsert_raw_batch(df, engine)
            total_fetched += len(page)
            total_loaded  += loaded

        logger.success(
            f"Ingestion complete — fetched {total_fetched}, loaded {total_loaded}"
        )
        return {"status": "success", "fetched": total_fetched, "loaded": total_loaded}

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        return {"status": "failed", "fetched": total_fetched, "loaded": total_loaded, "error": str(e)}


if __name__ == "__main__":
    result = run_ingestion(days_back=7)
    print(result)
