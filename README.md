
# Real-Time Open-Source Data Platform

![CI](https://github.com/azeddinshr/open-source-data-streaming-platform/actions/workflows/ci.yml/badge.svg)

A production-style data engineering platform built around GitHub public activity data.

The project implements two complementary data pipelines:

- **Streaming pipeline** for near real-time GitHub events using Redpanda and Flink SQL.
- **Batch pipeline** for historical GitHub Archive data using Spark and Parquet.

Both pipelines load data into PostgreSQL, where dbt builds the analytical models used by Metabase. Apache Airflow orchestrates the batch workflow, while automated tests and GitHub Actions validate the code and data transformations.

The project focuses on practical data engineering patterns such as event streaming, batch processing, idempotent ingestion, data validation, orchestration, incremental transformations, and data quality testing.

---

## Architecture

![Architecture](docs/images/architecture.png)

The platform follows two ingestion paths that converge in PostgreSQL:

**Streaming**

GitHub Events API → Python → Redpanda → Flink SQL → PostgreSQL

**Batch**

GH Archive → Python Downloader → Spark → Parquet → PostgreSQL

The resulting data is transformed with **dbt** and exposed through **Metabase** for analytics.

**Apache Airflow** orchestrates the batch pipeline, while **pytest**, **Ruff**, and **GitHub Actions** provide automated testing and code quality checks.

---

## Technology Stack

| Layer | Technologies |
|---|---|
| Data sources | GitHub Events API, GH Archive |
| Streaming | Redpanda, Kafka API, Flink SQL |
| Batch processing | Apache Spark 3.5, Parquet |
| Programming | Python 3.13 |
| Storage / Warehouse | PostgreSQL 16 |
| Transformation | dbt |
| Orchestration | Apache Airflow 3 |
| BI / Analytics | Metabase |
| Testing | pytest |
| Code quality | Ruff |
| CI/CD | GitHub Actions |
| Infrastructure | Docker Compose |
| Package management | uv |

---

## Project Workflow

### 1. Streaming Pipeline

The streaming pipeline collects recent activity from the GitHub Events API and processes it continuously.

```text
GitHub Events API
       ↓
Python Producer
       ↓
Redpanda
       ↓
 ┌─────┴─────┐
 ↓           ↓
Consumer   Flink SQL
 ↓           ↓
PostgreSQL PostgreSQL
```

### GitHub Events API

The Python producer polls the GitHub Events API for new public activity.

The ingestion process includes:

- GitHub token authentication
- Conditional requests using `ETag`
- Respect for GitHub's `X-Poll-Interval`
- Rate-limit handling and backoff
- Event de-duplication
- Graceful shutdown
- Idempotent message production

The producer publishes each event to the `github-events` Redpanda topic.

### Redpanda

Redpanda acts as the streaming backbone between producers and downstream consumers.

The project uses two topics:

- `github-events` — main event stream
- `github-events-dlq` — dead-letter topic for malformed or invalid messages

Redpanda provides the event buffering and decoupling layer between ingestion and processing.

### PostgreSQL Raw Consumer

A Python consumer reads events from `github-events` and loads them into the PostgreSQL `raw` schema.

The loader is designed to be idempotent:

- PostgreSQL transactions are committed before Kafka offsets are committed.
- Duplicate records are ignored using `ON CONFLICT DO NOTHING`.
- Invalid messages are redirected to the dead-letter topic.

This provides reliable at-least-once ingestion without creating duplicate database records.

### Flink SQL

Flink consumes the same Redpanda event stream independently of the PostgreSQL consumer.

The Flink job processes events using event-time windows and produces one-minute aggregates grouped by:

- repository
- GitHub event type

The resulting streaming metrics are written to PostgreSQL in the `streaming` schema.

This creates a near real-time analytical path alongside the raw event ingestion path.

---

# 2. Batch Pipeline

The batch pipeline processes historical GitHub activity using GH Archive.

```text
GH Archive
    ↓
Python Downloader
    ↓
Apache Spark
    ↓
Parquet
    ↓
PostgreSQL
    ↓
dbt
```

### GH Archive

GH Archive provides hourly archives of public GitHub activity.

The downloader retrieves the required hourly archive files and stores them locally before processing.

The downloader supports:

- Hour-based processing
- Resumable downloads
- Temporary files
- Atomic file replacement
- Safe re-processing of existing hours

### Apache Spark

Spark reads the GH Archive files and parses the event data using an explicit schema.

The processing step:

1. Reads the compressed GH Archive data.
2. Parses the event records.
3. Validates the input structure.
4. Separates rejected records.
5. Writes the valid records as Parquet.

The Parquet files are partitioned by archive hour.

This provides an efficient intermediate format for downstream processing and allows individual hours to be reprocessed independently.

### Parquet

Parquet is used as the batch processing format between Spark and PostgreSQL.

The data is organized by archive hour, allowing the pipeline to process specific time ranges without rewriting unrelated data.

### PostgreSQL

The batch pipeline loads the processed Parquet data into PostgreSQL staging tables and then merges it into the `raw.gharchive_events` table.

The merge process is idempotent, allowing the same archive hour to be processed multiple times without creating duplicate records.

---

# 3. Data Warehouse and Transformation

Both ingestion paths ultimately feed PostgreSQL.

The database is organized into separate schemas:

```text
raw
 │
 ├── github_events
 └── gharchive_events

staging
 │
 └── cleaned / structured models

streaming
 │
 └── real-time aggregates

marts
 │
 └── analytical models
```

dbt is responsible for transforming the raw data into analytical models.

The dbt layer includes:

- Staging models for parsing and cleaning raw events
- Incremental fact models
- Repository activity models
- Unified models combining streaming and batch sources
- Reconciliation models
- Data quality tests
- Freshness checks

The resulting models provide a consistent analytical layer for downstream dashboards.

---

# 4. Source Reconciliation

An important part of the project is the ability to compare the two ingestion paths.

The streaming and batch pipelines collect GitHub activity from different sources:

```text
GitHub Events API ──→ Streaming Pipeline
                           │
                           │
                           ▼
                       PostgreSQL
                           ▲
                           │
GH Archive ─────────→ Batch Pipeline
```

dbt models combine and reconcile the data from both sources.

This allows the platform to compare:

- Event counts
- Event types
- Repository activity
- Event timestamps
- Source coverage

The reconciliation layer helps identify differences between the near real-time stream and the historical archive.

---

# 5. Workflow Orchestration

Apache Airflow orchestrates the hourly batch workflow through the `gharchive_hourly` DAG.

```text
Download GH Archive
        ↓
   Spark processing
        ↓
   Parquet output
        ↓
 PostgreSQL loading
        ↓
      Merge
        ↓
    dbt build
        ↓
 Freshness checks
```

The DAG supports:

- Hourly scheduling
- Task retries
- Backfills
- Idempotent task execution
- Dependency management
- Data freshness validation

This makes the batch pipeline reproducible and allows historical periods to be reprocessed safely.

---

# 6. Data Quality and Reliability

The project implements several production-oriented data engineering patterns.

### Idempotent ingestion

Both streaming and batch ingestion are designed so that retrying a pipeline does not create duplicate records.

### Dead-letter queue

Malformed streaming messages are redirected to the `github-events-dlq` topic instead of stopping the consumer.

### Late data handling

The transformation layer accounts for events that can arrive after their original event time.

### Data validation

Spark validates incoming archive records and separates rejected data.

### Incremental processing

dbt incremental models process new data without rebuilding the entire dataset.

### Freshness checks

Airflow runs freshness validation as part of the batch workflow.

---

# 7. Testing and CI

The project includes automated Python tests covering the main ingestion components.

```text
pytest
  │
  ├── Producer tests
  ├── Consumer tests
  └── Downloader tests
```

Ruff is used for Python linting and code quality.

GitHub Actions runs the automated checks on every push.

The CI pipeline includes:

1. Python environment setup
2. Dependency installation
3. Ruff checks
4. pytest
5. PostgreSQL test database setup
6. dbt project validation
7. dbt build using seeded test data

This ensures that both application code and transformation logic are validated before changes are merged.

---

# 8. Project Structure

```text
open-source-data-streaming-platform/
│
├── ingestion/
│   ├── producer.py
│   ├── raw_consumer.py
│   └── Dockerfile
│
├── streaming/
│   ├── flink SQL jobs
│   └── startup scripts
│
├── batch/
│   ├── download_gharchive.py
│   ├── spark_gharchive_to_parquet.py
│   ├── spark_parquet_to_postgres.py
│   └── SQL merge scripts
│
├── dbt/
│   ├── models/
│   ├── tests/
│   └── sources/
│
├── orchestration/
│   ├── Dockerfile
│   └── dags/
│       └── gharchive_hourly.py
│
├── infrastructure/
│   └── sql/
│
├── tests/
│   ├── test_producer.py
│   ├── test_consumer.py
│   ├── test_downloader.py
│   └── fixtures/
│
├── docs/
│   └── images/
│       ├── architecture.png
│       └── dashboard.png
│
├── docker-compose.yml
├── pyproject.toml
├── Makefile
└── README.md
```

---

# 9. Quick Start

### Requirements

- Docker
- Docker Compose
- uv
- Git

### Start the platform

```bash
cp .env.example .env
make setup
make up
```

Add a GitHub token to `.env`.

The token does not require additional GitHub scopes for public event ingestion.

### Services

| Service | Port |
|---|---:|
| Redpanda Console | 8080 |
| Flink | 8081 |
| Airflow | 8085 |
| PostgreSQL | 5432 |
| Metabase | 3000 |

The exact service configuration is defined in `docker-compose.yml`.

### Run a historical backfill

```bash
make backfill \
  FROM=2026-10-12T11:00:00+00:00 \
  TO=2026-10-12T13:00:00+00:00
```

This downloads the selected GH Archive hours, processes them with Spark, loads them into PostgreSQL, and runs the downstream transformation workflow.

---

## Dashboard

![Dashboard](docs/images/dashboard.png)

The Metabase dashboard provides analytical views built from the dbt models, including batch-derived activity metrics and near real-time streaming metrics.

---

## Summary

This project brings together the main components of a modern data engineering platform:

**Ingestion**

GitHub Events API + GH Archive

**Streaming**

Redpanda + Flink SQL

**Batch**

Spark + Parquet

**Storage**

PostgreSQL

**Transformation**

dbt

**Orchestration**

Airflow

**Analytics**

Metabase

**Engineering quality**

pytest + Ruff + GitHub Actions

The result is a local, reproducible data platform that demonstrates both **real-time streaming and batch data engineering workflows** using open-source technologies.
```
