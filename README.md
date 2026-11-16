# MBTA Subway Reliability Pipeline

An always-on data pipeline that measures how reliable Boston's subway is and how
accurate its countdown clocks are, built on free MBTA data, GitHub Actions,
BigQuery, and dbt.

> **Status:** Phase 1 (ingestion) is done. Models, forecast, and dashboard are in progress (see [Roadmap](#roadmap)).

## Questions this project answers

1. **How long do riders actually wait, compared with how long they should?** This is measured
   per line, station, and hour as the actual gap between trains against the scheduled gap.
2. **How wrong is "Arriving in 4 min"?** This compares live countdown predictions with when
   the train actually arrived, broken down by how far ahead the prediction was made.
3. **Can we forecast next-hour reliability by line?**

## Architecture

```
MBTA V3 API (live predictions) --every 10 min--> GitHub Actions --\
                                                                   +--> BigQuery raw --> dbt staging/marts --> Looker Studio
MBTA LAMP (actual stop events) --daily-------->  GitHub Actions --/          |
                                                                             +--> forecast (next-hour excess wait)
```

| Source | What it is | Loaded |
|---|---|---|
| [MBTA V3 API](https://www.mbta.com/developers/v3-api) `/predictions` | Live countdown predictions, with the scheduled time attached | Snapshot every 10 min into `mbta_raw.predictions` |
| [MBTA LAMP](https://performancedata.mbta.com/) subway performance | Every actual stop a train made, with actual and scheduled headways, travel and dwell times | Daily, plus a one-time 55-day backfill, into `mbta_raw.lamp_stop_events` |

## Design decisions

- **Headways, not "minutes late".** Subway lines run on frequency ("every 6–8 min"), not
  timetables, so schedule adherence says little about what riders experience. The core
  metric is excess wait: actual gap between trains minus the scheduled gap.
- **Two sources, each for what it's good at.** Polling every few minutes can't pin down
  exact arrival times, but LAMP records them. Polling is used for what only it captures:
  what the countdown clocks said in advance.
- **Raw layer stays raw.** Loaders only type, filter to subway routes, and add lineage
  columns (`source_url`, `loaded_at`). All cleaning lives in dbt, where it's tested.
- **Re-runnable loads.** Each LAMP day is written by replacing that day's partition, so
  re-running a day never duplicates it. This works without SQL `DELETE`, which the BigQuery
  sandbox doesn't allow. Prediction snapshots carry a `poll_id`, so dbt can remove duplicates.
- **Free tier on purpose.** The BigQuery sandbox needs no credit card but deletes data older
  than 60 days. The backfill is therefore 55 days, and LAMP history can be re-loaded at any time.

## Run it locally (no Google account needed)

```bash
pip install -r requirements.txt
python -m ingest poll                    # one predictions snapshot -> data/predictions/
python -m ingest lamp --days-back 3      # last 3 service days     -> data/lamp_stop_events/
python -m unittest discover -s tests -t .
```

## Turn on the always-on pipeline

1. **Google Cloud project.** At [console.cloud.google.com](https://console.cloud.google.com),
   create a project and note its project ID. Without billing, BigQuery runs in the free sandbox.
2. **Service account.** Go to *IAM & Admin → Service Accounts → Create*, name it `mbta-loader`,
   and grant **BigQuery Data Editor** and **BigQuery Job User**. Then under *Keys → Add key → JSON*,
   download the key file. Keep it out of this repo; `.gitignore` blocks common key file names.
3. **GitHub settings.** In the repo, go to *Settings → Secrets and variables → Actions*:
   - Secret `GCP_SA_KEY`: the full contents of the JSON key file
   - Variable `GCP_PROJECT`: your project ID
   - Optional secret `MBTA_API_KEY`: a free key from the MBTA, which raises rate limits
4. **Backfill.** Go to *Actions → Load LAMP stop events → Run workflow*, with days back = `55`.

From then on, predictions load every 10 minutes and LAMP loads daily. The repo must be
public for free unlimited Actions minutes. GitHub pauses scheduled workflows after 60 days
without repo activity.

## Roadmap

- [x] **Phase 1, ingestion:** live predictions and LAMP loaders, GitHub Actions schedules, tests
- [ ] **Phase 2, dbt models:** staging, `fct_headways`, `fct_prediction_accuracy`, line/stop/hour marts, dbt tests
- [ ] **Phase 3, forecast:** next-hour excess wait by line, with a seasonal-naive baseline first
- [ ] **Phase 4, output:** Looker Studio dashboard plus a findings write-up

## Data notes

- Early finding: LAMP files for Nov 13–15, 2026 contain only 11 Green-C stop events,
  while the live API showed Green-C trains running. Phase 2 will test for this and track it.
- Trip IDs starting with `NONREV-` are non-revenue moves and will be excluded in staging.
