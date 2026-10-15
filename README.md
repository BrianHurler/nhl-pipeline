# NHL Data Pipeline

Python data-engineering repository for collecting NHL API data, transforming it into reusable Parquet tables, and publishing datasets to AWS S3 for hockey analytics projects.

## Goals

- Maintain a reproducible source-to-Parquet workflow.
- Separate data acquisition from Parquet preparation and cloud publication.
- Supply consistent datasets to independent xG modeling, other research, and reporting repositories.
- Favor simple, well-commented Python; see [AGENTS.md](AGENTS.md).

## Repository layout

```text
nhl-pipeline/
├── README.md
├── AGENTS.md
├── run_all.py                # Master pipeline runner (planned)
├── scripts/                  # Data acquisition and preparation scripts
├── raw-api-data/             # Downloaded NHL API data
└── aws-parquet-data/         # Generated Parquet datasets for AWS S3
```

- **`run_all.py`:** Top-level entry point to execute the pipeline scripts in order.
- **`scripts/`:** Python scripts for downloading, processing, and publishing NHL datasets.
- **`raw-api-data/`:** Local storage for original downloaded NHL API responses.
- **`aws-parquet-data/`:** Local storage for generated Parquet datasets intended for AWS S3.


## Pipeline Workflow

1. Download NHL API responses into `raw-api-data/`.
2. Parse and standardize source data into consistently typed tables.
3. Build reusable, analysis-ready Parquet datasets in `aws-parquet-data/`.
4. Publish Parquet outputs to AWS S3.
5. Read the published datasets from separate modeling and reporting repositories.

## Development and security

Follow [AGENTS.md](AGENTS.md): prioritize short, readable, mostly inline Python with concise comments above logical blocks. Use AWS profiles or environment variables for credentials; never commit access keys, tokens, or local secret files.

## Architecture Plan

The pipeline will use **Python + DuckDB + Parquet + AWS S3** to maintain reusable NHL datasets with minimal infrastructure and operating cost.

- **Historical coverage:** Discover and retrieve all seasons and game types supported by the relevant NHL API endpoints. Document differences in historical endpoint availability.
- **Incremental ingestion:** Compare expected API records with the saved raw archive, download missing records, and refresh recent games that may have been corrected. After the initial historical backfill, nightly runs should process only new or changed source records, including anything missed during downtime.
- **Raw archive:** Preserve original JSON responses in `raw-api-data/` and durable cloud storage so transformations can be reproduced.
- **Transformations:** Use Python for API retrieval and orchestration; use DuckDB SQL where it simplifies joins, deduplication, and incremental table preparation.
- **Analytical datasets:** Maintain **one logical dataset per table** across all seasons. Use season-partitioned Parquet files for large tables (such as plays, shots, and shifts) and single Parquet files for small reference tables. Store local outputs in `aws-parquet-data/` and publish datasets to S3.
- **Updates and publication:** Rebuild only affected partitions where practical, preserve historical records, and avoid exposing partially updated datasets. Treat S3 as the shared source of truth for published data.
- **Execution:** Keep a top-level `run_all.py` that runs independent scripts from `scripts/` in dependency order. Prefer mostly inline, readable Python over unnecessary abstractions.
- **Portability and scheduling:** Run the same pipeline on macOS or AWS EC2 Linux using relative project paths and standard AWS credential configuration. Schedule nightly production runs on EC2.
- **Scope:** Remove exploratory analyses and redundant QA from the production pipeline, while retaining essential checks that prevent silent data corruption or loss. Downstream models and reporting belong in separate repositories.

This is the target architecture; implementation will proceed as the existing preparation scripts are reviewed and migrated.

## Nightly raw-data ingestion

Run the downloader in **nightly mode** on EC2 after the historical S3 backfill:

```bash
python -u scripts/01_download_nhl_data.py --nightly --workers 8
```

- Searches the league-wide NHL game catalogs for seasons overlapping the **last 180 calendar days** (configurable with `--lookback-days`). It downloads gamecenter and shift-chart feeds only for completed games in that date window.
- Compares expected object keys with the existing S3 archive. Already archived game data and supplementary records are skipped, while missing files are downloaded and uploaded.
- Refreshes the small league-wide reference catalogs and participating-team rosters to discover newly available games and players. It otherwise uses a **missing-only** policy; corrections to previously archived game feeds or season statistics are **not** refreshed automatically in nightly mode.
- Limits supplementary jobs (players, standings, club stats, logs and reports) to participating teams, season types, and game dates in that window. Historical drafts/playoff bracket backfill remains part of the regular mode.
- Leaves the historical backfill unchanged: run `python -u scripts/01_download_nhl_data.py --start-year 2016 --workers 8` without `--nightly` when intentionally scanning every season.

The downloader requires `NHL_S3_BUCKET` and supports an optional `NHL_S3_PREFIX`; on EC2, use an attached instance IAM role instead of static AWS keys. Schedule only after verifying the nightly command and failure logs interactively. Parquet transformation and publishing will be added to the nightly orchestrator separately.
