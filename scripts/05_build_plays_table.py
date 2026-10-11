# %% [markdown]
# 05_build_plays_table.py
#
# PURPOSE
# -------
# Build one processed play-by-play table from the raw game JSON downloaded by
# script 04.
#
# The script reads every available raw play-by-play file, flattens nested event
# fields, attaches stable game metadata from the manifest, combines all games,
# writes plays.csv, and creates a simple schema/profile file.
#
# UNIT OF OBSERVATION
# -------------------
# One row in `plays_df` = one play-by-play event within one NHL game.

# %%
# =========================================================
# 1. Import libraries
# =========================================================
#
# WHAT:
# Load JSON parsing, filesystem, and tabular-data tools.
#
# WHY:
# Raw game files are JSON on disk; the final output is a combined pandas table.
#
# PYTHON NOTES:
# - json.load(): parses JSON directly from an open file.
# - pathlib.Path: constructs platform-safe file paths.
# - pandas: flattens, combines, profiles, and writes DataFrames.
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
# Locate the manifest, raw-data directory, processed-data directory, plays CSV,
# and schema/profile CSV.
#
# WHY:
# Keeping paths in one section makes the script's inputs and outputs obvious.
#
# PYTHON NOTES:
# - Path(__file__).resolve().parents[2] finds the repository root.
# - "/" joins Path objects without hard-coding OS-specific separators.
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

S3_BUCKET = "nhl-data-bucket1"
S3_REGION = "us-east-1"
S3_PREFIX = "nhl-data-research"
s3 = boto3.client("s3", region_name=S3_REGION)
MANIFEST_FILE = PROJECT_ROOT / "data" / "processed" / "game_manifest.csv"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_FILE = OUTPUT_DIR / "plays.csv"
SCHEMA_FILE = OUTPUT_DIR / "plays_schema.csv"

print("\nSection 2: Input and output paths set.")

# %%
# =========================================================
# 3. Load and validate the game manifest
# =========================================================
#
# WHAT:
# Read the manifest and confirm that all game-level fields needed below exist.
#
# WHY:
# The raw play events do not necessarily repeat all useful game metadata.
# The manifest supplies season, date, game type, and team context that we want
# attached to every play.
#
# PYTHON NOTES:
# - pd.read_csv(): reads the manifest into a DataFrame.
# - set subtraction: finds required columns that are absent.
# - drop_duplicates(): guarantees one manifest row per season/game.
# =========================================================

if not MANIFEST_FILE.exists():
    raise FileNotFoundError(
        f"Game manifest not found: {MANIFEST_FILE}\n"
        "Run scripts/data_prep/03_build_game_manifest.py first."
    )

manifest = pd.read_csv(
    MANIFEST_FILE,
    dtype={"season_id": str},
)

required_columns = {
    "season_id",
    "season",
    "game_id",
    "game_type",
    "game_date",
    "away_team_id",
    "away_team",
    "home_team_id",
    "home_team",
}

missing_columns = required_columns - set(manifest.columns)

if missing_columns:
    raise ValueError(
        "Game manifest is missing required columns: "
        + ", ".join(sorted(missing_columns))
    )

manifest = (
    manifest
    .drop_duplicates(subset=["season_id", "game_id"])
    .reset_index(drop=True)
)

print("\nSection 3: Game manifest loaded and validated.")

# %%
# =========================================================
# 4. Read and flatten every available raw play-by-play file
# =========================================================
#
# WHAT:
# Loop through manifest games, open each raw play-by-play JSON file, flatten its
# event records, attach game metadata, and append the game DataFrame to a list.
#
# WHY:
# Raw JSON is preserved game-by-game, but most analysis is easier with one
# combined event table. Attaching game metadata here makes each event row
# self-contained enough for later filtering and joins.
#
# PYTHON NOTES:
# - DataFrame.itertuples(): iterates over manifest rows.
# - with ... open(): safely opens/closes each JSON file.
# - json.load(): parses the file into Python dictionaries/lists.
# - pd.json_normalize(): flattens nested event dictionaries into columns.
# - DataFrame.insert(): adds a column at a specific position.
# - list.append(): stores each game's DataFrame for the final concat.
#
# DATA NOTE:
# Different event types can contain different nested `details` fields.
# json_normalize() therefore produces a union of event fields within each game,
# and pd.concat(sort=False) later preserves the union across all games.
# =========================================================

play_frames = []
files_read = 0
files_missing = 0
files_failed = 0

print(f"Games in manifest: {len(manifest)}")

for row in manifest.itertuples(index=False):
    season_id = str(row.season_id)
    game_id = int(row.game_id)

    input_file = (
        RAW_DIR
        / season_id
        / "play_by_play"
        / f"{game_id}.json"
    )

    if not input_file.exists():
        files_missing += 1
        continue

    try:
        with input_file.open("r", encoding="utf-8") as file:
            data = json.load(file)

        plays = data.get("plays", [])

        if not plays:
            print(f"WARNING: no plays found for game_id={game_id}")
            continue

        game_plays = pd.json_normalize(
            plays,
            sep="_",
        )

        # Insert stable game context at the front of the table so it remains
        # easy to identify the season/game/team for every individual event.
        game_plays.insert(0, "season_id", season_id)
        game_plays.insert(1, "season", row.season)
        game_plays.insert(2, "game_id", game_id)
        game_plays.insert(3, "game_type", row.game_type)
        game_plays.insert(4, "game_date", row.game_date)
        game_plays.insert(5, "away_team_id", row.away_team_id)
        game_plays.insert(6, "away_team", row.away_team)
        game_plays.insert(7, "home_team_id", row.home_team_id)
        game_plays.insert(8, "home_team", row.home_team)

        play_frames.append(game_plays)
        files_read += 1

    except (OSError, ValueError, TypeError) as error:
        files_failed += 1
        print(f"ERROR reading game_id={game_id}: {error}")

print("\nSection 4: Raw play-by-play files read and flattened.")

# %%
# =========================================================
# 5. Combine all game-level DataFrames into one plays table
# =========================================================
#
# WHAT:
# Stack the per-game DataFrames, preserve every discovered column, and sort
# events into a stable chronological/game order when those sort fields exist.
#
# WHY:
# A single event table is the reusable analytical layer needed by most later
# hockey questions.
#
# PYTHON NOTES:
# - pd.concat(): vertically stacks all DataFrames in `play_frames`.
# - ignore_index=True: creates one clean row index for the combined table.
# - sort=False: preserves the union of columns without alphabetically sorting
#   them during concatenation.
# - list comprehension: keeps only sort fields that actually exist.
# =========================================================

if not play_frames:
    raise RuntimeError(
        "No play-by-play files were successfully read. "
        "Run scripts/data_prep/04_download_raw_games.py first."
    )

plays_df = pd.concat(
    play_frames,
    ignore_index=True,
    sort=False,
)

candidate_sort_columns = [
    "season_id",
    "game_date",
    "game_id",
    "sortOrder",
    "eventId",
]

sort_columns = [
    column
    for column in candidate_sort_columns
    if column in plays_df.columns
]

if sort_columns:
    plays_df = (
        plays_df
        .sort_values(sort_columns)
        .reset_index(drop=True)
    )

print("\nSection 5: Game-level play DataFrames combined.")

# %%
# =========================================================
# 6. Save the combined plays table
# =========================================================
#
# WHAT:
# Create the processed-data directory if needed and write plays.csv.
#
# WHY:
# This CSV becomes a reusable processed layer so later scripts do not need to
# reopen and flatten every raw JSON response.
#
# PYTHON NOTES:
# - mkdir(parents=True, exist_ok=True): safely creates missing directories.
# - DataFrame.to_csv(index=False): writes rows/columns without pandas' index.
# =========================================================

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

plays_df.to_csv(
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
print("\nSection 6: Combined plays table saved.")

# %%
# =========================================================
# 7. Build a simple schema / completeness profile
# =========================================================
#
# WHAT:
# For every column, record its pandas dtype, non-null count, and percent non-null.
#
# WHY:
# NHL play-by-play is semi-structured: many fields only apply to certain event
# types. This profile helps us distinguish common fields from sparse ones before
# using them in analysis.
#
# PYTHON NOTES:
# - DataFrame.notna(): returns True where a value is present.
# - Series.sum(): counts True values because True behaves like 1.
# - DataFrame.dtypes / Series.dtype: describes pandas' inferred storage type.
# - round(): limits the displayed completeness percentage to two decimals.
#
# LEARNING NOTE:
# This is deliberately written as an explicit loop instead of hiding the
# profiling logic in a helper function.
# =========================================================

schema_rows = []
row_count = len(plays_df)

for column in plays_df.columns:
    non_null_count = int(
        plays_df[column]
        .notna()
        .sum()
    )

    if row_count > 0:
        non_null_pct = non_null_count / row_count * 100
    else:
        non_null_pct = 0.0

    schema_record = {
        "column": column,
        "dtype": str(plays_df[column].dtype),
        "non_null_count": non_null_count,
        "non_null_pct": round(non_null_pct, 2),
    }

    schema_rows.append(schema_record)

schema_df = pd.DataFrame(schema_rows)

schema_df.to_csv(
    SCHEMA_FILE,
    index=False,
)

print("\nSection 7: Schema completeness profile built and saved.")

# %%
# =========================================================
# 8. Run QA checks and print a build summary
# =========================================================
#
# WHAT:
# Compare manifest coverage with plays-table coverage, report parse/missing-file
# counts, summarize rows by season, and check event-key duplicates.
#
# WHY:
# Successful file creation does not guarantee a complete or logically sound
# table. These checks make missing games and duplicate event keys visible.
#
# PYTHON NOTES:
# - drop_duplicates().shape[0]: counts distinct season/game combinations.
# - groupby(...).size(): counts play rows by season.
# - DataFrame.duplicated(subset=...): identifies repeated composite keys.
#
# KEY ASSUMPTION:
# Within one game, `eventId` is expected to identify a play event. Therefore
# (season_id, game_id, eventId) should normally be unique.
# =========================================================

manifest_game_count = len(manifest)

plays_game_count = (
    plays_df[["season_id", "game_id"]]
    .drop_duplicates()
    .shape[0]
)

rows_by_season = (
    plays_df
    .groupby("season", dropna=False)
    .size()
    .reset_index(name="play_rows")
)

print("\nBuild complete.")
print(f"Play-by-play files read: {files_read}")
print(f"Manifest games missing raw play-by-play: {files_missing}")
print(f"Files that failed to parse: {files_failed}")
print(f"Games represented in plays table: {plays_game_count}")
print(f"Games in manifest: {manifest_game_count}")
print(f"Total play rows: {len(plays_df):,}")
print(f"Total columns discovered: {len(plays_df.columns)}")
print(f"Plays table: {OUTPUT_FILE}")
print(f"Schema profile: {SCHEMA_FILE}")

print("\nRows by season:")
print(rows_by_season)

if "eventId" in plays_df.columns:
    duplicate_events = plays_df.duplicated(
        subset=["season_id", "game_id", "eventId"]
    ).sum()

    print(
        "\nDuplicate (season_id, game_id, eventId) rows: "
        f"{duplicate_events}"
    )

print("\nFirst five rows:")
print(plays_df.head())

print("\nSection 8: Plays-table QA summary complete.")
