# 🚋 Warsaw Transit Data Platform

A data engineering project that continuously collects **real-time GPS positions
of all Warsaw public transport vehicles** (ZTM buses and trams) from the
[UM Warszawa Open Data API](https://api.um.warszawa.pl) and stores them in an
S3-compatible data lake, following the **medallion architecture**
(bronze → silver → gold).

> 🚧 Work in progress – Stage 1 (ingestion) is complete. See the [roadmap](#roadmap).

## Architecture

```
┌──────────────┐     ┌──────────────────────┐     ┌──────────────────────┐
│   ZTM API    │────▶│  Python ingestion    │────▶│  MinIO (S3)          │
│ (every 30 s) │     │  Docker container    │     │  bronze bucket       │
└──────────────┘     │  retry + validation  │     │  raw JSON Lines      │
                     └──────────────────────┘     └──────────────────────┘
```

Locally the data lake runs on **MinIO**, which speaks the same API as **AWS S3**.
The same code will run against AWS S3 by changing a single environment variable.

## Tech stack

| Area | Technology |
|---|---|
| Language | Python 3.12 |
| Data source | UM Warszawa REST API |
| Data lake | MinIO (S3-compatible), boto3 |
| Containerization | Docker, Docker Compose |
| Data format | JSON Lines, Hive-style partitioning |

## How to run

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- A free API key from [api.um.warszawa.pl](https://api.um.warszawa.pl) (register an account)

### Steps

1. Clone the repository:
   ```bash
   git clone https://github.com/<mati290>/<REPO_NAME>.git
   cd <ZTM_data_processing>
   ```

2. Create your `.env` file from the template and fill in the values:
   ```bash
   cp .env.example .env
   ```

3. Start MinIO:
   ```bash
   docker compose up -d minio
   ```

4. Open the MinIO console at http://localhost:9001, log in with the
   credentials from `.env` and create a bucket named `bronze`.

5. Start the ingestion:
   ```bash
   docker compose up -d --build
   docker compose logs -f ingestion
   ```

6. After ~30 seconds new files appear in the `bronze` bucket.

To stop everything: `docker compose down` (data is kept in a Docker volume).

## Data layout (bronze layer)

```
s3://bronze/
└── vehicle_type=bus/
    └── date=2026-10-02/
        └── hour=08/
            └── positions_20261002T083205.jsonl
└── vehicle_type=tram/
    └── ...
```

Each line is one vehicle position, stored exactly as returned by the API,
plus an `_ingested_at` metadata field:

```json
{"Lines": "180", "Lon": 21.01, "VehicleNumber": "1234", "Time": "2026-10-02 10:32:01", "Lat": 52.23, "Brigade": "3", "_ingested_at": "2026-10-02T08:32:05+00:00"}
```

## Design decisions

- **Raw data in bronze is never modified.** Only an `_ingested_at` field is
  added (the underscore marks fields added by the pipeline, not the source).
  If a bug is found in later transformations, silver can always be rebuilt
  from the original data.
- **All ingestion timestamps and partitions are in UTC** to avoid DST
  ambiguity: in local time the hour 02:00–03:00 occurs twice in October and
  doesn't exist in March. Note: the API's `Time` field is in Warsaw local
  time – this is handled in the silver layer.
- **Hive-style partitioning** (`key=value` folders by vehicle type, date and
  hour) allows query engines like Spark, Athena or DuckDB to skip irrelevant
  data (partition pruning).
- **JSON Lines format** – one record per line, so files can be processed
  line by line and in parallel without loading everything into memory.
- **Validating API responses.** The API returns HTTP 200 even on errors and
  puts an error message in `result` instead of a list. The pipeline detects
  this and fails loudly instead of silently saving empty or invalid files.
- **Retry with exponential backoff** (2 s, 4 s) for transient network and API
  errors. A single failed cycle is logged and doesn't stop the long-running
  process.
- **Empty batches are skipped** to avoid creating empty files.
- **Configuration via environment variables** (12-factor app). Secrets live
  in `.env`, which is excluded from Git and from the Docker image
  (`.dockerignore`).
- **Pinned image versions.** In September 2026 MinIO removed its images from
  Docker Hub, which broke projects pulling `minio/minio`. This project uses an
  alternative registry and pinned versions – a real-world example of
  supply-chain risk in dependencies.
- **Docker layer caching:** dependencies are installed before the code is
  copied, so code changes rebuild the image in seconds.

## Known limitations

- Polling every 30 s produces thousands of small files per day (~1 GB/day).
  This "small files problem" will be solved in Stage 2 by compacting bronze
  into Parquet in the silver layer.
- The `bronze` bucket is currently created manually.

## Roadmap

- [x] **Stage 1** – ingestion from the ZTM API to the bronze layer, Docker
- [ ] **Stage 2** – Apache Airflow: bronze → silver (deduplication, data
      cleaning, Parquet)
- [ ] **Stage 3** – dbt models: average speeds, delays vs. timetable
- [ ] **Stage 4** – Apache Kafka + Spark Structured Streaming
- [ ] **Stage 5** – dashboard (Metabase)
- [ ] **Stage 6** – deployment to AWS with Terraform
- [ ] Tests and CI with GitHub Actions

## Data source

Data provided by Miasto Stołeczne Warszawa via
[api.um.warszawa.pl](https://api.um.warszawa.pl).
