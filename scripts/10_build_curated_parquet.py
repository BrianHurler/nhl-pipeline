# %% [markdown]
# 10_build_curated_parquet.py
#
# PURPOSE
# -------
# Build the curated Parquet warehouse from processed project tables.
#
# The curated layer standardizes storage into table directories and partitions
# large season-based tables by season_id.
#
# OUTPUT GRAIN
# ------------
# The grain depends on each source table. This script changes storage layout;
# it does not intentionally change the analytical grain of any source table.

# %%
# =========================================================
# 1. Import libraries
# =========================================================
#
# WHAT:
# Load filesystem, directory-removal, and pandas tools.
#
# WHY:
# We need to recreate curated table directories and write partitioned Parquet.
#
# PYTHON NOTES:
# - shutil.rmtree(): recursively removes an existing directory.
# - pandas: reads processed tables and writes Parquet.
# =========================================================

from pathlib import Path

import boto3
import shutil

import pandas as pd

print("\nSection 1: Libraries loaded.")


# %%
# =========================================================
# 2. Configure processed and curated warehouse paths
# =========================================================
#
# WHAT:
# Define the source processed-data directory and destination curated directory.
#
# WHY:
# The processed layer contains build outputs; curated is the query-ready layer.
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

S3_BUCKET = "nhl-data-bucket1"
S3_REGION = "us-east-1"
S3_PREFIX = "nhl-data-research"
s3 = boto3.client("s3", region_name=S3_REGION)
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
CURATED_DIR = PROJECT_ROOT / "data" / "curated"

CURATED_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

print("\nSection 2: Curated warehouse paths configured.")


# %%
# =========================================================
# 3. Define the reusable partition-writing function
# =========================================================
#
# WHAT:
# Create one function that replaces a curated table directory and writes either
# season partitions or one unpartitioned Parquet file.
#
# WHY:
# This is genuinely reusable behavior repeated across dozens of tables. Keeping
# it in one function reduces the risk that tables are written inconsistently.
#
# PYTHON NOTES:
# - function parameters: let the same logic handle many tables.
# - groupby(partition_col): divides a DataFrame into one chunk per season.
# - pd.isna(): lets missing partition values use an "unknown" folder.
# - Hive-style folders such as season_id=20252026 are friendly to engines such
#   as DuckDB and other Parquet readers.
# =========================================================

def write_partitioned(
    df,
    table_name,
    partition_col="season_id",
):
    table_dir = (
        CURATED_DIR
        / table_name
    )

    if table_dir.exists():
        shutil.rmtree(
            table_dir
        )

    table_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    should_partition = (
        partition_col
        and partition_col in df.columns
    )

    if should_partition:
        grouped_table = df.groupby(
            partition_col,
            dropna=False,
        )

        for value, chunk in grouped_table:
            value_text = (
                "unknown"
                if pd.isna(value)
                else str(value)
            )

            partition_dir = (
                table_dir
                / f"{partition_col}={value_text}"
            )

            partition_dir.mkdir(
                parents=True,
                exist_ok=True,
            )

            output_file = partition_dir / "part-000.parquet"

            chunk.to_parquet(
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

    else:
        output_file = table_dir / "part-000.parquet"

        df.to_parquet(
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

    print(
        f"{table_name}: "
        f"{len(df):,} rows -> {table_dir}"
    )

print("\nSection 3: Reusable Parquet partition writer defined.")


# %%
# =========================================================
# 4. Load and write the two core CSV fact tables
# =========================================================
#
# WHAT:
# Curate the game manifest as `games` and the processed plays CSV as `plays`.
#
# WHY:
# These are foundational warehouse tables and currently originate as CSV rather
# than processed Parquet files.
#
# PYTHON NOTES:
# - low_memory=False: asks pandas to infer CSV column types in one pass rather
#   than chunking inference, which reduces mixed-type warnings.
# =========================================================

manifest_file = (
    PROCESSED_DIR
    / "game_manifest.csv"
)

plays_file = (
    PROCESSED_DIR
    / "plays.csv"
)

if not manifest_file.exists():
    raise FileNotFoundError(
        f"Missing {manifest_file}"
    )

if not plays_file.exists():
    raise FileNotFoundError(
        f"Missing {plays_file}"
    )

games = pd.read_csv(
    manifest_file,
    dtype={"season_id": str},
)

write_partitioned(
    games,
    "games",
)

plays = pd.read_csv(
    plays_file,
    dtype={"season_id": str},
    low_memory=False,
)

write_partitioned(
    plays,
    "plays",
)

print("\nSection 4: Core games and plays tables curated.")


# %%
# =========================================================
# 5. Define the known processed Parquet tables
# =========================================================
#
# WHAT:
# Map each expected processed table to its desired partition column.
#
# WHY:
# Not every table has season_id. Dimension/reference tables are often small and
# naturally unpartitioned, while larger facts benefit from season partitions.
#
# PYTHON NOTES:
# - dictionary values of None mean "write this table without partitioning."
# =========================================================

base_tables = {
    "shifts": "season_id",
    "skater_game_stats": "season_id",
    "goalie_game_stats": "season_id",
    "team_game_stats": "season_id",
    "rosters": "season_id",
    "players": None,
    "teams": None,
    "standings_daily": None,
    "player_game_logs": "season_id",
    "club_skater_season_stats": "season_id",
    "club_goalie_season_stats": "season_id",
    "franchises": None,
    "seasons": None,
    "component_seasons": None,
    "countries": None,
    "drafts": None,
    "stat_glossary": None,
    "standings_seasons": None,
    "draft_picks": "season_id",
    "playoff_series": "season_id",
}

print("\nSection 5: Curated table partition plan defined.")


# %%
# =========================================================
# 6. Curate the known processed Parquet tables
# =========================================================
#
# WHAT:
# Loop through the table plan, read any source that exists, normalize season_id
# to text, and write it using the shared partition function.
#
# WHY:
# Some supplemental sources may not have been built yet. Missing optional tables
# should be reported and skipped rather than crashing the entire warehouse build.
#
# PYTHON NOTES:
# - dict.items(): loops over table/partition pairs.
# - continue: skips directly to the next loop iteration.
# =========================================================

curated_base_tables = []
skipped_base_tables = []

for table_name, partition_col in base_tables.items():
    source_file = (
        PROCESSED_DIR
        / f"{table_name}.parquet"
    )

    if not source_file.exists():
        print(
            f"SKIP {table_name}: "
            f"source not found ({source_file})"
        )
        skipped_base_tables.append(
            table_name
        )
        continue

    table_df = pd.read_parquet(
        source_file
    )

    if "season_id" in table_df.columns:
        table_df["season_id"] = (
            table_df["season_id"]
            .astype(str)
        )

    write_partitioned(
        table_df,
        table_name,
        partition_col,
    )

    curated_base_tables.append(
        table_name
    )

print("\nSection 6: Known processed Parquet tables curated.")


# %%
# =========================================================
# 7. Curate dynamically generated Stats REST report tables
# =========================================================
#
# WHAT:
# Find every processed stats_*.parquet file and copy it into curated storage.
#
# WHY:
# Script 15 can generate many report tables whose exact names are not practical
# to hard-code here.
#
# PYTHON NOTES:
# - Path.glob("stats_*.parquet"): finds matching files.
# - Path.stem: returns a filename without its extension.
# =========================================================

stats_sources = sorted(
    PROCESSED_DIR.glob(
        "stats_*.parquet"
    )
)

curated_stats_tables = []

for source_file in stats_sources:
    table_name = source_file.stem

    table_df = pd.read_parquet(
        source_file
    )

    if "season_id" in table_df.columns:
        table_df["season_id"] = (
            table_df["season_id"]
            .astype(str)
        )
        partition_col = "season_id"
    else:
        partition_col = None

    write_partitioned(
        table_df,
        table_name,
        partition_col,
    )

    curated_stats_tables.append(
        table_name
    )

print("\nSection 7: Stats REST report tables curated.")


# %%
# =========================================================
# 8. Print curated-warehouse QA summary
# =========================================================
#
# WHAT:
# Report how many known and dynamic tables were written or skipped.
#
# WHY:
# The build should make table coverage visible before the dedicated QA script.
# =========================================================

print("\nCurated parquet build complete.")
print(f"Warehouse root: {CURATED_DIR}")
print(
    "Known processed tables curated: "
    f"{len(curated_base_tables):,}"
)
print(
    "Known processed tables skipped: "
    f"{len(skipped_base_tables):,}"
)
print(
    "Stats REST tables curated: "
    f"{len(curated_stats_tables):,}"
)

if skipped_base_tables:
    print("\nSkipped known tables:")
    for table_name in skipped_base_tables:
        print(f"  {table_name}")

print("\nSection 8: Curated-warehouse build summary complete.")
