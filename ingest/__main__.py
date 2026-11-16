"""Command line entry point.

  python -m ingest poll                       # one predictions snapshot
  python -m ingest lamp                       # LAMP for the last 2 service days
  python -m ingest lamp --days-back 55        # backfill
  python -m ingest lamp --date 2026-11-14     # one day
Add --sink bigquery (with GCP_PROJECT set) to load into BigQuery instead of ./data.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from . import config, lamp, predictions
from .http import make_session
from .sinks import BigQuerySink, LocalSink

BOSTON = ZoneInfo("America/New_York")


def make_sink(kind: str):
    if kind == "bigquery":
        if not config.GCP_PROJECT:
            sys.exit("error: set GCP_PROJECT to use --sink bigquery")
        from google.api_core.exceptions import Forbidden
        try:
            return BigQuerySink(config.GCP_PROJECT, config.BQ_DATASET)
        except Forbidden as exc:
            sys.exit(f"error: BigQuery refused access ({exc.message}).\n"
                     "Grant the service account 'BigQuery Data Editor' and 'BigQuery Job User' "
                     f"on project {config.GCP_PROJECT} (IAM & Admin -> IAM -> Grant access).")
    return LocalSink(config.LOCAL_DATA_DIR)


def cmd_poll(args) -> int:
    sink = make_sink(args.sink)
    table = predictions.poll(make_session())
    if table.num_rows == 0:
        print("no predictions returned (overnight shutdown?); nothing written")
        return 0
    day = table["polled_at"][0].as_py().date()
    print(f"{table.num_rows} predictions -> {sink.write(config.PREDICTIONS_TABLE, table, day)}")
    return 0


def cmd_lamp(args) -> int:
    sink = make_sink(args.sink)
    today = datetime.now(BOSTON).date()
    if args.date:
        days = [date.fromisoformat(args.date)]
    else:
        days = [today - timedelta(days=n) for n in range(args.days_back, 0, -1)]

    session = make_session()
    loaded = missing = 0
    for day in days:
        try:
            table = lamp.fetch_day(session, day)
        except lamp.NotPublishedYet as exc:
            print(f"{day}: skipped ({exc})")
            missing += 1
            continue
        where = sink.write(config.STOP_EVENTS_TABLE, table, day, replace=True)
        print(f"{day}: {table.num_rows} stop events -> {where}")
        loaded += 1
    print(f"done: {loaded} day(s) loaded, {missing} not published yet")
    return 0 if loaded or args.date is None else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m ingest")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("poll", help="store one snapshot of live predictions")
    p.add_argument("--sink", choices=["local", "bigquery"], default="local")
    p.set_defaults(func=cmd_poll)

    l = sub.add_parser("lamp", help="load LAMP actual stop events")
    l.add_argument("--sink", choices=["local", "bigquery"], default="local")
    l.add_argument("--date", help="one service day, YYYY-MM-DD")
    l.add_argument("--days-back", type=int, default=2,
                   help="load the N service days before today (default 2, to catch late files)")
    l.set_defaults(func=cmd_lamp)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
