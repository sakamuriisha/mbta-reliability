"""Where raw data lands: local Parquet files (for development) or BigQuery.

Both sinks partition by day. `replace=True` overwrites that day's partition,
which makes daily LAMP loads safe to re-run. Predictions are append-only:
each poll is a new snapshot, and dbt removes any duplicate polls.
"""

from __future__ import annotations

import io
import shutil
import uuid
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

PARTITION_FIELD = {"predictions": "polled_at", "lamp_stop_events": "service_day"}


class LocalSink:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def write(self, table_name: str, table: pa.Table, day: date, replace: bool = False) -> str:
        part = self.root / table_name / f"dt={day.isoformat()}"
        if replace and part.exists():
            shutil.rmtree(part)
        part.mkdir(parents=True, exist_ok=True)
        path = part / f"{uuid.uuid4().hex[:12]}.parquet"
        pq.write_table(table, path)
        return str(path)


class BigQuerySink:
    def __init__(self, project: str, dataset: str, location: str = "US"):
        from google.cloud import bigquery

        self.bq = bigquery
        self.client = bigquery.Client(project=project)
        self.dataset = f"{project}.{dataset}"
        ds = bigquery.Dataset(self.dataset)
        ds.location = location
        self.client.create_dataset(ds, exists_ok=True, timeout=30)

    def _table_exists(self, table_id: str) -> bool:
        from google.api_core.exceptions import NotFound
        try:
            self.client.get_table(table_id)
            return True
        except NotFound:
            return False

    def write(self, table_name: str, table: pa.Table, day: date, replace: bool = False) -> str:
        bq = self.bq
        table_id = f"{self.dataset}.{table_name}"
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        config = bq.LoadJobConfig(
            source_format=bq.SourceFormat.PARQUET,
            time_partitioning=bq.TimePartitioning(type_=bq.TimePartitioningType.DAY,
                                                  field=PARTITION_FIELD[table_name]),
        )
        if replace and self._table_exists(table_id):
            # Partition decorator + truncate replaces just this day (no DML needed,
            # which the BigQuery sandbox doesn't allow).
            destination = f"{table_id}${day.strftime('%Y%m%d')}"
            config.write_disposition = bq.WriteDisposition.WRITE_TRUNCATE
        else:
            destination = table_id
            config.write_disposition = bq.WriteDisposition.WRITE_APPEND
            config.schema_update_options = [bq.SchemaUpdateOption.ALLOW_FIELD_ADDITION] \
                if self._table_exists(table_id) else None

        job = self.client.load_table_from_file(buf, destination, job_config=config)
        job.result(timeout=300)
        return f"{destination} (+{job.output_rows} rows)"
