# %% [markdown]
# 07_build_shifts_table.py
#
# PURPOSE
# -------
# Combine the raw game-by-game shift-chart JSON files into one processed
# shift-record table.
#
# UNIT OF OBSERVATION
# -------------------
# One row in `shifts` = one NHL-native shift-chart record for one player.

# %%
# =========================================================
# 1. Import libraries
# =========================================================
#
# WHAT:
# Load JSON, filesystem, and pandas tools.
#
# WHY:
# We need to read raw JSON files and combine their records into one DataFrame.
#
# PYTHON NOTES:
# - json.load(): parses an open JSON file.
# - pathlib.Path: handles file paths.
# - pandas: normalizes JSON and combines DataFrames.
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
# Define the manifest, raw-data directory, processed output, and schema output.
#
# WHY:
# This makes the script's dependencies and products explicit.
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

S3_BUCKET = "nhl-data-bucket1"
S3_REGION = "us-east-1"
S3_PREFIX = "nhl-data-research"
s3 = boto3.client("s3", region_name=S3_REGION)
MANIFEST_FILE = PROJECT_ROOT / "data" / "processed" / "game_manifest.csv"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_FILE = OUTPUT_DIR / "shifts.parquet"
SCHEMA_FILE = OUTPUT_DIR / "shifts_schema.csv"

print("\nSection 2: Shift-table paths configured.")


# %%
# =========================================================
# 3. Load and prepare the game manifest
# =========================================================
#
# WHAT:
# Read the manifest and retain one row per game.
#
# WHY:
# The manifest supplies both the game list and metadata we attach to each shift.
#
# PYTHON NOTES:
# - dtype={"season_id": str}: keeps season IDs as text.
# - drop_duplicates(): protects against repeated game keys.
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

manifest = (
    manifest
    .drop_duplicates(subset=["season_id", "game_id"])
    .reset_index(drop=True)
)

print(f"Games in manifest: {len(manifest):,}")
print("\nSection 3: Game manifest loaded and prepared.")


# %%
# =========================================================
# 4. Read and flatten every available shift file
# =========================================================
#
# WHAT:
# Open each game's shift JSON, flatten its records, add game context, and store
# the per-game DataFrame for later concatenation.
#
# WHY:
# The raw feed is game-by-game; analysis is easier with one reusable shift table.
#
# PYTHON NOTES:
# - DataFrame.itertuples(): loops over manifest rows.
# - pd.json_normalize(): flattens nested JSON records.
# - DataFrame.insert(): places game metadata at the front of the table.
# - list.append(): collects each game's DataFrame.
# =========================================================

frames = []
files_read = 0
files_missing = 0
files_failed = 0

for row in manifest.itertuples(index=False):
    season_id = str(row.season_id)
    game_id = int(row.game_id)

    input_file = (
        RAW_DIR
        / season_id
        / "shifts"
        / f"{game_id}.json"
    )

    if not input_file.exists():
        files_missing += 1
        continue

    try:
        with input_file.open("r", encoding="utf-8") as file:
            data = json.load(file)

        records = data.get("data", [])

        if not records:
            print(f"WARNING: no shift records for game_id={game_id}")
            continue

        game_shifts = pd.json_normalize(
            records,
            sep="_",
        )

        game_shifts.insert(0, "season_id", season_id)
        game_shifts.insert(1, "season", row.season)
        game_shifts.insert(2, "game_id", game_id)
        game_shifts.insert(3, "game_type", row.game_type)
        game_shifts.insert(4, "game_date", row.game_date)
        game_shifts.insert(5, "away_team_id", row.away_team_id)
        game_shifts.insert(6, "away_team", row.away_team)
        game_shifts.insert(7, "home_team_id", row.home_team_id)
        game_shifts.insert(8, "home_team", row.home_team)

        frames.append(game_shifts)
        files_read += 1

    except (OSError, ValueError, TypeError) as error:
        files_failed += 1
        print(f"ERROR reading game_id={game_id}: {error}")

print("\nSection 4: Raw shift files read and flattened.")


# %%
# =========================================================
# 5. Combine and sort the shift records
# =========================================================
#
# WHAT:
# Stack all per-game DataFrames and sort using the best available time/player
# fields.
#
# WHY:
# This produces one stable table while preserving all columns observed across
# games.
#
# PYTHON NOTES:
# - pd.concat(..., sort=False): stacks rows while retaining the union of columns.
# - list comprehension: keeps only sort columns present in the data.
# =========================================================

if not frames:
    raise RuntimeError(
        "No shift files were successfully read. "
        "Run scripts/data_prep/06_download_shift_charts.py first."
    )

shifts = pd.concat(
    frames,
    ignore_index=True,
    sort=False,
)

candidate_sort_columns = [
    "season_id",
    "game_date",
    "game_id",
    "period",
    "startTime",
    "playerId",
]

sort_columns = [
    column
    for column in candidate_sort_columns
    if column in shifts.columns
]

if sort_columns:
    shifts = (
        shifts
        .sort_values(sort_columns)
        .reset_index(drop=True)
    )

print("\nSection 5: Shift records combined and sorted.")


# %%
# =========================================================
# 6. Save the processed shift table
# =========================================================
#
# WHAT:
# Write the combined shift DataFrame to Parquet.
#
# WHY:
# Parquet preserves types and is compact/fast for later analysis.
#
# PYTHON NOTES:
# - DataFrame.to_parquet(): writes a columnar analytical file.
# =========================================================

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

shifts.to_parquet(
    OUTPUT_FILE,
    index=False,
)

s3_key = (
    f"{S3_PREFIX}/"
    f"{OUTPUT_FILE.relative_to(PROJECT_ROOT).as_posix()}"
)

s3.upload_file(
    str(OUTPUT_FILE),
    S3_BUCKET,
    s3_key,
)

print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")
print("\nSection 6: Processed shift table saved.")


# %%
# =========================================================
# 7. Build and save the shift schema profile
# =========================================================
#
# WHAT:
# Record each column's dtype, non-null count, and completeness percentage.
#
# WHY:
# API fields can be sparse. A profile makes table structure/completeness visible.
#
# PYTHON NOTES:
# - Series.notna().sum(): counts populated values.
# - explicit loop: intentionally keeps the calculation easy to inspect.
# =========================================================

schema_rows = []

for column in shifts.columns:
    non_null_count = int(
        shifts[column]
        .notna()
        .sum()
    )

    non_null_pct = (
        non_null_count
        / len(shifts)
        * 100
    )

    schema_rows.append(
        {
            "column": column,
            "dtype": str(shifts[column].dtype),
            "non_null_count": non_null_count,
            "non_null_pct": round(non_null_pct, 2),
        }
    )

schema_df = pd.DataFrame(schema_rows)

schema_df.to_csv(
    SCHEMA_FILE,
    index=False,
)

print("\nSection 7: Shift schema profile built and saved.")


# %%
# =========================================================
# 8. Run QA and print the build summary
# =========================================================
#
# WHAT:
# Report source-file coverage, table size, and duplicate NHL shift IDs.
#
# WHY:
# The NHL-native `id` should uniquely identify shift records. Duplicate IDs are
# therefore a useful integrity check.
#
# PYTHON NOTES:
# - Series.duplicated(): marks repeated values after the first occurrence.
# =========================================================

print("\nShift table build complete.")
print(f"Shift files read: {files_read}")
print(f"Manifest games missing raw shifts: {files_missing}")
print(f"Files that failed to parse: {files_failed}")
print(f"Shift rows: {len(shifts):,}")
print(f"Columns: {len(shifts.columns)}")
print(f"Output: {OUTPUT_FILE}")
print(f"Schema profile: {SCHEMA_FILE}")

if "id" in shifts.columns:
    duplicate_ids = int(
        shifts["id"]
        .duplicated()
        .sum()
    )
    print(f"Duplicate NHL shift ids: {duplicate_ids}")
else:
    duplicate_ids = None
    print(
        "WARNING: NHL shift id field not present; "
        "uniqueness was not checked."
    )

print("\nSection 8: Shift-table QA summary complete.")
