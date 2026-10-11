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

- **`run_all.py` (planned):** Top-level entry point to execute the pipeline scripts in order.
- **`scripts/`:** Python scripts for downloading, processing, and publishing NHL datasets.
- **`raw-api-data/`:** Local storage for original downloaded NHL API responses.
- **`aws-parquet-data/`:** Local storage for generated Parquet datasets intended for AWS S3.

The two data directories are for data, not source code. Avoid committing large generated datasets or API archives to Git.

## Pipeline Workflow

1. Download NHL API responses into `raw-api-data/`.
2. Parse and standardize source data into consistently typed tables.
3. Build reusable, analysis-ready Parquet datasets in `aws-parquet-data/`.
4. Publish Parquet outputs to AWS S3.
5. Read the published datasets from separate modeling and reporting repositories.

## Development and security

Follow [AGENTS.md](AGENTS.md): prioritize short, readable, mostly inline Python with concise comments above logical blocks. Use AWS profiles or environment variables for credentials; never commit access keys, tokens, or local secret files.
