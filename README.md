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
├── raw-api-data/
│   └── .gitkeep
└── aws-parquet-data/
    └── .gitkeep
```

**`raw-api-data/`** will hold scripts for retrieving and organizing original NHL API responses.

**`aws-parquet-data/`** will hold scripts for processing NHL data into Parquet tables and publishing those tables to S3.

These are the only two planned top-level *code folders*. They will hold scripts; large raw data and generated Parquet files should not be checked into Git.

## Intended workflow

1. Download raw NHL API responses.
2. Parse and standardize fields into typed tables.
3. Produce documented, analysis-ready Parquet datasets.
4. Upload outputs to S3.
5. Read these datasets from separate modeling and reporting repositories.

Previous pipeline work used `s3://nhl-data-bucket1/nhl-data-research/`. Final cloud paths will be reviewed during migration.

## Status

**Initial setup.** The directory placeholders and project coding guidelines are in place. No preparation scripts have been migrated or modified yet. The pipeline is not runnable from this repository yet.

## Local setup

```bash
cd ~/Documents/GitHub
git clone https://github.com/BrianHurler/nhl-pipeline.git
cd nhl-pipeline
code .
```

## Development and security

Follow [AGENTS.md](AGENTS.md): prioritize short, readable, mostly inline Python, with concise comments above logical blocks. Use AWS profiles or environment variables for credentials; never commit access keys, tokens, or local secret files.
