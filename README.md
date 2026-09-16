# Serverless Weather Data Pipeline

An automated data pipeline on Google Cloud that collects weather data every hour,
stores it, and turns it into a clean table that analysts can query.

Built with Python, SQL, and Google Cloud Platform.

---

## What problem does this solve?

Useful data usually lives somewhere inconvenient — behind someone else's API, in a
format nobody can query, arriving continuously. Someone has to collect it on a
schedule, store it reliably, and reshape it into something a business can actually
use.

That collect → store → reshape process is called a **data pipeline**, and it is the
foundation of any reporting or analytics work. This project builds one end to end.

It runs entirely **serverless**, meaning there is no server to maintain or pay for
while idle. The code wakes up once an hour, does its job in a couple of seconds,
and shuts down.

---

## How it works

```
   Open-Meteo API                  Google Cloud Storage            BigQuery
  (public weather data)             (raw file archive)         (analytics database)
         │                                  │                          │
         │   1. fetch hourly readings       │                          │
         ▼                                  │                          │
  ┌──────────────┐    2. save raw file      │                          │
  │    Cloud     │ ────────────────────────▶│                          │
  │   Function   │                          │                          │
  │   (Python)   │    3. load into database │                          │
  └──────────────┘ ─────────────────────────┴─────────────────────────▶ │
         ▲                                                             │
         │                                            4. SQL transform │
  ┌──────────────┐                                                     ▼
  │    Cloud     │                                           ┌──────────────────┐
  │  Scheduler   │  triggers every hour, on the hour          │ Clean, queryable │
  └──────────────┘                                           │      table       │
                                                             └──────────────────┘
```

**Step by step:**

1. **Cloud Scheduler** acts as an alarm clock. Every hour it pings the function.
2. **Cloud Function** (the Python code in [`main.py`](main.py)) calls the weather
   API, converts the response into a table, and saves it as a **Parquet** file — a
   compressed format built for analytics, ~5 KB per run.
3. That raw file is archived in **Cloud Storage**. Keeping the original untouched
   means any bug in later steps can be fixed by reprocessing, without re-fetching
   history.
4. The file is loaded into **BigQuery**, Google's analytics database.
5. A **SQL view** ([`transform.sql`](transform.sql)) cleans the data on read —
   deduplicating it and adding a Fahrenheit column.

This design is known as **ELT** (Extract, Load, Transform): land the raw data
first, transform it afterwards inside the database. It is the current industry
standard, because the raw archive stays available for re-processing.

---

## The interesting engineering problem

The weather API doesn't return one reading — it returns a **7-day hourly forecast**,
168 rows, every single time it is called.

Running hourly means the same future hour gets reported over and over, each time
with a slightly revised prediction. Naively appending would produce 168 conflicting
rows for every hour of the week.

The SQL view solves this. It ranks every row for a given hour by how recently it was
collected, and keeps only the most recent:

```sql
ROW_NUMBER() OVER (
  PARTITION BY timestamp        -- group all rows describing the same hour
  ORDER BY extracted_at DESC    -- newest collection first
) AS rn
...
WHERE rn = 1                    -- keep only the newest
```

The result is exactly one row per hour, always reflecting the latest available
forecast — while every superseded version stays in the archive.

---

## Verification

Cloud pipelines are slow and costly to debug by deploying repeatedly, so the logic
is tested locally first. [`validate_local.py`](validate_local.py) runs the full
pipeline with the cloud calls removed.

```
$ python validate_local.py

1. EXTRACT
  HTTP 200          rows returned: 168          timezone: GMT

3. PARQUET SCHEMA -> BIGQUERY TYPE MAPPING
  timestamp        timestamp[ns, tz=UTC]    -> TIMESTAMP
  temperature_2m   double                   -> FLOAT64
  humidity         int64                    -> INT64
  extracted_at     timestamp[us, tz=UTC]    -> TIMESTAMP

4. SIMULATED transform.sql (two runs, overlapping forecasts)
  staging rows           : 336
  distinct timestamps    : 168
  rows surfaced by view  : 168
  duplicate metric_time  : 0

5. UNIT CONVERSION CHECK
  OK      0.0 C ->   32.00 F
  OK   -40.0 C ->  -40.00 F

RESULT: local validation PASSED
```

This checks the two things most likely to break silently: that every column arrives
in the database as the **correct data type** (a timestamp stored as text will not
sort or filter correctly), and that the deduplication really does collapse 336 rows
to 168 with no duplicates.

---

## Running cost

Effectively **$0/month**. At 720 runs a month the whole pipeline sits inside Google
Cloud's always-free tier — roughly 4 MB of storage and a few seconds of compute per
day.

Cost was a design constraint, not an afterthought. Serverless functions bill per
invocation rather than per hour of uptime, and the SQL transform is a *view* rather
than a copied table, so it stores no duplicate data.

---

## Tech stack

| Component | Technology | Role |
|---|---|---|
| Ingestion | Python 3.11, `requests` | Calls the weather API |
| Data handling | `pandas`, `pyarrow` | Builds and encodes the Parquet file |
| Compute | Cloud Functions (gen 2) | Runs the code, serverless |
| Raw storage | Cloud Storage | Archives the original files |
| Warehouse | BigQuery | Stores and queries the data |
| Transformation | BigQuery SQL | Deduplicates, converts units |
| Orchestration | Cloud Scheduler | Hourly trigger |
| Deployment | `gcloud` CLI, bash | One-command reproducible setup |

---

## Repository contents

| File | Purpose |
|---|---|
| [`main.py`](main.py) | The Cloud Function — extract, archive, load |
| [`transform.sql`](transform.sql) | SQL view that deduplicates and enriches |
| [`deploy.sh`](deploy.sh) | Provisions and deploys the whole pipeline |
| [`validate_local.py`](validate_local.py) | Local test harness, no cloud needed |
| [`requirements.txt`](requirements.txt) | Python dependencies, version-pinned |

---

## Deployment

All infrastructure is created by a single script — no manual console clicking, so
the environment can be rebuilt identically at any time.

```bash
bash deploy.sh
```

It enables the required Google Cloud services, creates the storage bucket and
database, deploys the function, schedules it hourly, and builds the SQL view.
Re-running it is safe: existing resources are detected and skipped.

Requires the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install), or
[Cloud Shell](https://shell.cloud.google.com), which has it preinstalled.

**Current status:** the pipeline is deployment-ready and its logic is verified
locally, but it has not yet been run against a live Google Cloud project. All
resource names and the target region (`australia-southeast1`, Sydney) are
configured at the top of [`deploy.sh`](deploy.sh).

---

## Design decisions worth noting

**Timestamps are stored as real UTC timestamps, not text.** The API returns
`"2026-09-16T00:00"` as a string. Loading that directly would give a text column
that happens to *look* sortable. Converting to a proper timezone-aware timestamp
before upload means BigQuery stores it as a real `TIMESTAMP`, so date filtering and
sorting behave correctly.

**The raw archive is kept separate from the database.** Transformations run on read,
so a mistake in the SQL is a one-line fix rather than a data-loss incident.

**Temporary files are explicitly cleaned up.** A Cloud Function's `/tmp` directory
is held in memory, not on disk. Files left behind accumulate against the memory
limit across reuses, so [`main.py`](main.py) deletes each Parquet file after upload
in a `finally` block.

**Every dependency is pinned to an exact version,** so a future release of pandas or
pyarrow cannot silently change the output format.

---

## Possible extensions

- Partition the database table by date, so queries scan only the days they need
- Add data quality checks (missing hours, readings outside plausible bounds)
- Track forecast accuracy by comparing old predictions against what happened
- Replace public access on the function with service-account authentication
