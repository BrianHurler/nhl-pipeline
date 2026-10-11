# %% [markdown]
# 15_build_stats_report_tables.py
#
# PURPOSE
# -------
# Convert raw NHL Stats REST report JSON downloaded by script 14 into processed
# Parquet tables.
#
# Each unique entity/report pair becomes one processed table, for example:
# - stats_skater_summary.parquet
# - stats_goalie_advanced.parquet
# - stats_team_powerplay.parquet

# %%
# =========================================================
# 1. Import libraries
# =========================================================
#
# WHAT:
# Load JSON, filesystem, and pandas tools.
#
# WHY:
# We need to find cached report files, flatten their rows, combine seasons/game
# types, and write typed Parquet outputs.
# =========================================================

import json
from pathlib import Path

import boto3

import pandas as pd

print("\nSection 1: Libraries loaded.")


# %%
# =========================================================
# 2. Configure input and output paths
# =========================================================
#
# WHAT:
# Locate the game manifest, raw Stats REST archive, and processed output folder.
#
# WHY:
# The manifest defines which season/game-type combinations should be searched.
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

S3_BUCKET = "nhl-data-bucket1"
S3_REGION = "us-east-1"
S3_PREFIX = "nhl-data-research"
s3 = boto3.client("s3", region_name=S3_REGION)
MANIFEST_FILE = PROJECT_ROOT / "data" / "processed" / "game_manifest.csv"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

print("\nSection 2: Stats REST table paths configured.")


# %%
# =========================================================
# 3. Load the manifest and determine season/game-type scope
# =========================================================
#
# WHAT:
# Read the manifest and retain unique season_id + game_type combinations.
#
# WHY:
# Raw report files are organized by these two dimensions.
#
# PYTHON NOTES:
# - drop_duplicates(): turns game-level rows into unique report scopes.
# =========================================================

if not MANIFEST_FILE.exists():
    raise FileNotFoundError(
        f"Missing manifest: {MANIFEST_FILE}\n"
        "Run scripts/data_prep/03_build_game_manifest.py first."
    )

manifest = pd.read_csv(
    MANIFEST_FILE,
    dtype={"season_id": str},
)

season_types = (
    manifest[
        [
            "season_id",
            "game_type",
        ]
    ]
    .drop_duplicates()
    .sort_values(
        [
            "season_id",
            "game_type",
        ]
    )
    .reset_index(drop=True)
)

print(
    "Season/game-type combinations: "
    f"{len(season_types):,}"
)
print("\nSection 3: Stats REST season/game-type scope created.")


# %%
# =========================================================
# 4. Read and group raw report files by entity/report
# =========================================================
#
# WHAT:
# Walk the expected raw directory tree, flatten every report's data rows, attach
# season/game-type context, and group matching reports together in a dictionary.
#
# WHY:
# We want all seasons/game types of the same report to end up in one processed
# table rather than one table per source file.
#
# PYTHON NOTES:
# - dictionary key (entity, report): identifies one eventual table.
# - dict.setdefault(key, []): creates a list the first time a report appears.
# - pd.json_normalize(): flattens nested Stats REST records.
# =========================================================

report_frames = {}
files_read = 0
files_without_rows = 0

for row in season_types.itertuples(
    index=False
):
    season_id = str(row.season_id)
    game_type = int(row.game_type)

    base_dir = (
        RAW_DIR
        / season_id
        / "stats_reports"
    )

    if not base_dir.exists():
        continue

    entity_dirs = sorted(
        path
        for path in base_dir.iterdir()
        if path.is_dir()
    )

    for entity_dir in entity_dirs:
        entity = entity_dir.name

        game_type_dir = (
            entity_dir
            / str(game_type)
        )

        if not game_type_dir.exists():
            continue

        report_paths = sorted(
            game_type_dir.glob("*.json")
        )

        for path in report_paths:
            report = path.stem

            with path.open(
                "r",
                encoding="utf-8",
            ) as file:
                data = json.load(file)

            rows = (
                data.get("data", [])
                or []
            )

            files_read += 1

            if not rows:
                files_without_rows += 1
                continue

            report_df = pd.json_normalize(
                rows,
                sep="_",
            )

            report_df.insert(
                0,
                "season_id",
                season_id,
            )

            report_df.insert(
                1,
                "game_type",
                game_type,
            )

            report_key = (
                entity,
                report,
            )

            report_frames.setdefault(
                report_key,
                []
            )

            report_frames[
                report_key
            ].append(
                report_df
            )

print(f"Raw report files read: {files_read:,}")
print(
    "Raw report files with no rows: "
    f"{files_without_rows:,}"
)
print(
    "Unique entity/report tables discovered: "
    f"{len(report_frames):,}"
)

print("\nSection 4: Raw Stats REST reports grouped by table.")


# %%
# =========================================================
# 5. Combine and save each processed report table
# =========================================================
#
# WHAT:
# Concatenate all DataFrames belonging to each entity/report pair and write one
# Parquet file per pair.
#
# WHY:
# Downstream analysis is easier when one report table spans the full available
# season/game-type universe.
#
# PYTHON NOTES:
# - sorted(report_frames.items()): processes table names deterministically.
# - pd.concat(..., sort=False): preserves the union of available columns.
# =========================================================

tables_written = []

for (
    entity,
    report,
), frames in sorted(
    report_frames.items()
):
    report_table = pd.concat(
        frames,
        ignore_index=True,
        sort=False,
    )

    table_name = (
        f"stats_{entity}_{report}"
    )

    output_file = (
        OUTPUT_DIR
        / f"{table_name}.parquet"
    )

    report_table.to_parquet(
        output_file,
        index=False,
    )

    s3_key = (
        f"{S3_PREFIX}/"
        f"{output_file.relative_to(PROJECT_ROOT).as_posix()}"
    )

    s3.upload_file(
        str(output_file),
        S3_BUCKET,
        s3_key,
    )

    print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")

    tables_written.append(
        table_name
    )

    print(
        f"{table_name}: "
        f"{len(report_table):,} rows -> "
        f"{output_file}"
    )

print("\nSection 5: Stats REST report tables combined and saved.")


# %%
# =========================================================
# 6. Print Stats REST table-build summary
# =========================================================
#
# WHAT:
# Report how many raw files were read and how many processed tables were written.
#
# WHY:
# This provides a quick completeness check before script 10 curates the tables.
# =========================================================

print("\nStats report table build complete.")
print(f"Raw report files read: {files_read:,}")
print(f"Files with no rows: {files_without_rows:,}")
print(f"Tables written: {len(tables_written):,}")

print("\nSection 6: Stats REST table-build summary complete.")
