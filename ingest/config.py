"""Settings shared by the loaders. Override with environment variables."""

import os

# Subway lines tracked. Green Line branches are separate routes in MBTA data.
SUBWAY_ROUTES = ["Red", "Orange", "Blue", "Green-B", "Green-C", "Green-D", "Green-E"]

MBTA_API_BASE = "https://api-v3.mbta.com"
# Optional. Without a key the API allows 20 requests/minute, which is plenty
# for one poll every few minutes. A free key raises it to 1,000.
MBTA_API_KEY = os.environ.get("MBTA_API_KEY") or None

# LAMP: the MBTA's published record of every actual subway stop event, one file per service day.
LAMP_INDEX_URL = "https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/index.csv"
LAMP_DAY_URL = "https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/{date}-subway-on-time-performance-v1.parquet"

GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET", "mbta_raw")
LOCAL_DATA_DIR = os.environ.get("LOCAL_DATA_DIR", "data")

# Raw table names (same in BigQuery and the local Parquet store).
PREDICTIONS_TABLE = "predictions"
STOP_EVENTS_TABLE = "lamp_stop_events"

USER_AGENT = "mbta-reliability-pipeline (portfolio project)"
