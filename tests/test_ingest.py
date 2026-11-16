import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from ingest import lamp, predictions
from ingest.config import SUBWAY_ROUTES
from ingest.sinks import LocalSink

FIXTURES = Path(__file__).parent / "fixtures"


class PredictionTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads((FIXTURES / "predictions_sample.json").read_text())
        self.polled_at = datetime(2026, 11, 16, 18, 0, tzinfo=timezone.utc)
        self.table = predictions.flatten(self.payload, self.polled_at, "poll1")

    def test_one_row_per_prediction_with_schema(self):
        self.assertEqual(self.table.num_rows, len(self.payload["data"]))
        self.assertEqual(self.table.schema, predictions.SCHEMA)

    def test_schedule_is_joined_and_times_are_utc(self):
        rows = self.table.to_pylist()
        with_sched = [r for r in rows if r["scheduled_arrival"] or r["scheduled_departure"]]
        self.assertEqual(len(with_sched), 5)
        for r in rows:
            for col in ("predicted_arrival", "predicted_departure", "scheduled_arrival"):
                if r[col] is not None:
                    self.assertEqual(r[col].utcoffset().total_seconds(), 0)

    def test_offset_conversion(self):
        self.assertEqual(predictions._ts("2026-11-16T13:57:00-05:00"),
                         datetime(2026, 11, 16, 18, 57, tzinfo=timezone.utc))
        self.assertIsNone(predictions._ts(None))

    def test_empty_response(self):
        self.assertEqual(predictions.flatten({"data": []}, self.polled_at, "p").num_rows, 0)


class LampTests(unittest.TestCase):
    def setUp(self):
        raw = pq.read_table(FIXTURES / "lamp_sample.parquet")
        self.raw_routes = set(raw["route_id"].to_pylist())
        self.table = lamp.normalize(raw, date(2026, 11, 14), "https://example/lamp.parquet")

    def test_keeps_only_subway_routes(self):
        self.assertIn("Mattapan", self.raw_routes)
        self.assertTrue(set(self.table["route_id"].to_pylist()) <= set(SUBWAY_ROUTES))
        self.assertEqual(self.table.num_rows, 17)

    def test_adds_partition_and_lineage_columns(self):
        for col in ("service_day", "source_url", "loaded_at"):
            self.assertIn(col, self.table.column_names)
        self.assertEqual(set(self.table["service_day"].to_pylist()), {date(2026, 11, 14)})


class LocalSinkTests(unittest.TestCase):
    def test_replace_overwrites_day_and_append_adds(self):
        raw = pq.read_table(FIXTURES / "lamp_sample.parquet")
        with tempfile.TemporaryDirectory() as tmp:
            sink = LocalSink(tmp)
            day = date(2026, 11, 14)
            sink.write("lamp_stop_events", raw, day, replace=True)
            sink.write("lamp_stop_events", raw, day, replace=True)
            part = Path(tmp) / "lamp_stop_events" / "dt=2026-11-14"
            self.assertEqual(len(list(part.glob("*.parquet"))), 1)
            sink.write("predictions", raw, day)
            sink.write("predictions", raw, day)
            self.assertEqual(len(list((Path(tmp) / "predictions" / "dt=2026-11-14").glob("*.parquet"))), 2)


if __name__ == "__main__":
    unittest.main()
