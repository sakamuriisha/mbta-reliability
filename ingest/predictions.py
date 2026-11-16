"""Snapshot of live countdown predictions from the MBTA V3 API.

Each poll stores every current prediction for the subway lines, with the
scheduled time attached. Later, dbt joins these snapshots to LAMP's actual
arrivals to measure how accurate the countdown clocks were.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pyarrow as pa

from .config import MBTA_API_BASE, MBTA_API_KEY, SUBWAY_ROUTES

SCHEMA = pa.schema([
    ("poll_id", pa.string()),
    ("polled_at", pa.timestamp("us", tz="UTC")),
    ("prediction_id", pa.string()),
    ("route_id", pa.string()),
    ("direction_id", pa.int8()),
    ("trip_id", pa.string()),
    ("stop_id", pa.string()),
    ("vehicle_id", pa.string()),
    ("stop_sequence", pa.int32()),
    ("predicted_arrival", pa.timestamp("us", tz="UTC")),
    ("predicted_departure", pa.timestamp("us", tz="UTC")),
    ("arrival_uncertainty_s", pa.int32()),
    ("departure_uncertainty_s", pa.int32()),
    ("scheduled_arrival", pa.timestamp("us", tz="UTC")),
    ("scheduled_departure", pa.timestamp("us", tz="UTC")),
    ("schedule_relationship", pa.string()),
    ("status", pa.string()),
    ("update_type", pa.string()),
    ("revenue", pa.string()),
])


def _ts(value: str | None) -> datetime | None:
    """MBTA times are ISO 8601 with a local offset; store them as UTC."""
    if not value:
        return None
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _rel_id(item: dict, name: str) -> str | None:
    data = (item.get("relationships", {}).get(name) or {}).get("data")
    return data["id"] if data else None


def flatten(payload: dict, polled_at: datetime, poll_id: str) -> pa.Table:
    """Turn one JSON:API response into rows matching SCHEMA."""
    schedules = {i["id"]: i["attributes"] for i in payload.get("included", []) if i["type"] == "schedule"}
    rows = []
    for p in payload.get("data", []):
        a = p["attributes"]
        sched = schedules.get(_rel_id(p, "schedule")) or {}
        rows.append({
            "poll_id": poll_id,
            "polled_at": polled_at,
            "prediction_id": p["id"],
            "route_id": _rel_id(p, "route"),
            "direction_id": a.get("direction_id"),
            "trip_id": _rel_id(p, "trip"),
            "stop_id": _rel_id(p, "stop"),
            "vehicle_id": _rel_id(p, "vehicle"),
            "stop_sequence": a.get("stop_sequence"),
            "predicted_arrival": _ts(a.get("arrival_time")),
            "predicted_departure": _ts(a.get("departure_time")),
            "arrival_uncertainty_s": a.get("arrival_uncertainty"),
            "departure_uncertainty_s": a.get("departure_uncertainty"),
            "scheduled_arrival": _ts(sched.get("arrival_time")),
            "scheduled_departure": _ts(sched.get("departure_time")),
            "schedule_relationship": a.get("schedule_relationship"),
            "status": a.get("status"),
            "update_type": a.get("update_type"),
            "revenue": a.get("revenue"),
        })
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def poll(session) -> pa.Table:
    params = {"filter[route]": ",".join(SUBWAY_ROUTES), "include": "schedule"}
    headers = {"x-api-key": MBTA_API_KEY} if MBTA_API_KEY else {}
    resp = session.get(f"{MBTA_API_BASE}/predictions", params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    polled_at = datetime.now(timezone.utc)
    return flatten(resp.json(), polled_at, poll_id=uuid.uuid4().hex)
