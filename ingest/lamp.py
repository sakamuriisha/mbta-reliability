"""Daily actual stop events from MBTA LAMP.

LAMP publishes one Parquet file per service day with every stop a subway train
made: when it arrived, the actual gap to the previous train (headway), the
scheduled gap, travel and dwell times. The raw layer keeps LAMP's columns as
published; cleaning happens in dbt.
"""

from __future__ import annotations

import io
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .config import LAMP_DAY_URL, SUBWAY_ROUTES


class NotPublishedYet(Exception):
    pass


def normalize(table: pa.Table, service_day: date, source_url: str) -> pa.Table:
    """Keep the subway routes, add a DATE column to partition on, and record lineage."""
    table = table.filter(pc.is_in(table["route_id"], pa.array(SUBWAY_ROUTES)))
    n = table.num_rows
    table = table.append_column("service_day", pa.array([service_day] * n, pa.date32()))
    table = table.append_column("source_url", pa.array([source_url] * n, pa.string()))
    table = table.append_column(
        "loaded_at", pa.array([datetime.now(timezone.utc)] * n, pa.timestamp("us", tz="UTC")))
    return table


def fetch_day(session, service_day: date) -> pa.Table:
    url = LAMP_DAY_URL.format(date=service_day.isoformat())
    resp = session.get(url, timeout=120)
    if resp.status_code in (403, 404):
        raise NotPublishedYet(f"LAMP has no file for {service_day} yet")
    resp.raise_for_status()
    return normalize(pq.read_table(io.BytesIO(resp.content)), service_day, url)
