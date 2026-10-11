# %% [markdown]
# 13_build_supplemental_tables.py
#
# PURPOSE
# -------
# Convert supplemental raw JSON downloaded by script 12 into reusable processed
# Parquet tables.
#
# The outputs include:
# - standings_daily
# - player_game_logs
# - club_skater_season_stats
# - club_goalie_season_stats
# - reference dimensions
# - draft_picks
# - playoff_series

# %%
# =========================================================
# 1. Import libraries
# =========================================================
#
# WHAT:
# Load JSON, filesystem, and pandas tools.
#
# WHY:
# The script reads cached raw JSON and combines it into processed tables.
# =========================================================

import json
from pathlib import Path

import boto3

import pandas as pd

print("\nSection 1: Libraries loaded.")


# %%
# =========================================================
# 2. Configure project paths
# =========================================================
#
# WHAT:
# Locate the game manifest, raw archive, and processed output directory.
#
# WHY:
# The manifest defines the relevant season universe; raw files provide source
# records; processed is where the reusable Parquet tables are written.
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

print("\nSection 2: Supplemental table paths configured.")


# %%
# =========================================================
# 3. Load the manifest and determine seasons in scope
# =========================================================
#
# WHAT:
# Read the manifest and extract unique NHL season IDs.
#
# WHY:
# Several raw supplemental directories are organized by season.
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

seasons = sorted(
    manifest["season_id"]
    .astype(str)
    .unique()
)

print(f"Seasons in scope: {len(seasons):,}")
print("\nSection 3: Manifest loaded and season list created.")


# %%
# =========================================================
# 4. Define the reusable table-writing function
# =========================================================
#
# WHAT:
# Combine a list of DataFrames, optionally deduplicate on a key, and write one
# Parquet file.
#
# WHY:
# This substantial operation repeats across several supplemental table families.
# Keeping it in one function prevents inconsistent combine/write behavior.
#
# PYTHON NOTES:
# - pd.concat(..., sort=False): preserves the union of columns.
# - subset filtering: only uses dedupe keys that actually exist.
# =========================================================

def write_table(
    table_name,
    frames,
    dedupe_columns=None,
):
    if not frames:
        print(
            f"SKIP {table_name}: "
            "no rows"
        )
        return pd.DataFrame()

    table_df = pd.concat(
        frames,
        ignore_index=True,
        sort=False,
    )

    if dedupe_columns:
        existing_dedupe_columns = [
            column
            for column in dedupe_columns
            if column in table_df.columns
        ]

        if existing_dedupe_columns:
            table_df = (
                table_df
                .drop_duplicates(
                    subset=existing_dedupe_columns
                )
                .reset_index(drop=True)
            )

    output_file = (
        OUTPUT_DIR
        / f"{table_name}.parquet"
    )

    table_df.to_parquet(
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
        f"{len(table_df):,} rows -> "
        f"{output_file}"
    )

    return table_df

print("\nSection 4: Reusable supplemental table writer defined.")


# %%
# =========================================================
# 5. Build daily standings snapshots
# =========================================================
#
# WHAT:
# Read every saved standings-date JSON file and attach snapshot_date.
#
# WHY:
# This creates historical team-state records that can be joined to games without
# leaking future/end-of-season standings information.
#
# PYTHON NOTES:
# - Path.glob("*.json"): finds all standings snapshot files.
# - pd.json_normalize(): flattens nested team standings records.
# =========================================================

standings_frames = []

standings_dir = (
    RAW_DIR
    / "reference"
    / "standings"
)

if standings_dir.exists():
    standings_paths = sorted(
        standings_dir.glob("*.json")
    )

    for path in standings_paths:
        with path.open(
            "r",
            encoding="utf-8",
        ) as file:
            data = json.load(file)

        rows = (
            data.get("standings", [])
            or []
        )

        if not rows:
            continue

        standings_snapshot = pd.json_normalize(
            rows,
            sep="_",
        )

        standings_snapshot.insert(
            0,
            "snapshot_date",
            path.stem,
        )

        standings_frames.append(
            standings_snapshot
        )

standings_daily = write_table(
    "standings_daily",
    standings_frames,
    [
        "snapshot_date",
        "teamAbbrev_default",
    ],
)

print("\nSection 5: Daily standings table built.")


# %%
# =========================================================
# 6. Build player game-log table
# =========================================================
#
# WHAT:
# Read cached player game logs across season/game-type directories and combine
# them into one table.
#
# WHY:
# This gives us an official NHL player-game source that can complement and
# validate boxscore-derived player tables.
# =========================================================

player_log_frames = []

for season_id in seasons:
    base_dir = (
        RAW_DIR
        / season_id
        / "player_game_logs"
    )

    if not base_dir.exists():
        continue

    game_type_dirs = sorted(
        path
        for path in base_dir.iterdir()
        if path.is_dir()
    )

    for game_type_dir in game_type_dirs:
        game_type = int(
            game_type_dir.name
        )

        for path in sorted(
            game_type_dir.glob("*.json")
        ):
            with path.open(
                "r",
                encoding="utf-8",
            ) as file:
                data = json.load(file)

            rows = (
                data.get("gameLog", [])
                or []
            )

            if not rows:
                continue

            player_games = pd.json_normalize(
                rows,
                sep="_",
            )

            player_games.insert(
                0,
                "season_id",
                season_id,
            )

            player_games.insert(
                1,
                "game_type",
                game_type,
            )

            player_games.insert(
                2,
                "player_id",
                int(path.stem),
            )

            player_log_frames.append(
                player_games
            )

player_game_logs = write_table(
    "player_game_logs",
    player_log_frames,
    [
        "season_id",
        "game_type",
        "player_id",
        "gameId",
    ],
)

print("\nSection 6: Player game-log table built.")


# %%
# =========================================================
# 7. Build club skater and goalie season tables
# =========================================================
#
# WHAT:
# Read each cached club-stats response and split its skaters and goalies into
# separate processed tables.
#
# WHY:
# Skater and goalie season aggregates have different fields and analytical uses.
# =========================================================

club_skater_frames = []
club_goalie_frames = []

for season_id in seasons:
    base_dir = (
        RAW_DIR
        / season_id
        / "club_stats"
    )

    if not base_dir.exists():
        continue

    game_type_dirs = sorted(
        path
        for path in base_dir.iterdir()
        if path.is_dir()
    )

    for game_type_dir in game_type_dirs:
        game_type = int(
            game_type_dir.name
        )

        for path in sorted(
            game_type_dir.glob("*.json")
        ):
            team = path.stem

            with path.open(
                "r",
                encoding="utf-8",
            ) as file:
                data = json.load(file)

            skater_rows = (
                data.get("skaters", [])
                or []
            )

            if skater_rows:
                skater_stats = pd.json_normalize(
                    skater_rows,
                    sep="_",
                )

                skater_stats.insert(
                    0,
                    "season_id",
                    season_id,
                )
                skater_stats.insert(
                    1,
                    "game_type",
                    game_type,
                )
                skater_stats.insert(
                    2,
                    "team",
                    team,
                )

                club_skater_frames.append(
                    skater_stats
                )

            goalie_rows = (
                data.get("goalies", [])
                or []
            )

            if goalie_rows:
                goalie_stats = pd.json_normalize(
                    goalie_rows,
                    sep="_",
                )

                goalie_stats.insert(
                    0,
                    "season_id",
                    season_id,
                )
                goalie_stats.insert(
                    1,
                    "game_type",
                    game_type,
                )
                goalie_stats.insert(
                    2,
                    "team",
                    team,
                )

                club_goalie_frames.append(
                    goalie_stats
                )

club_skater_season_stats = write_table(
    "club_skater_season_stats",
    club_skater_frames,
    [
        "season_id",
        "game_type",
        "team",
        "playerId",
    ],
)

club_goalie_season_stats = write_table(
    "club_goalie_season_stats",
    club_goalie_frames,
    [
        "season_id",
        "game_type",
        "team",
        "playerId",
    ],
)

print("\nSection 7: Club skater and goalie season tables built.")


# %%
# =========================================================
# 8. Build global reference tables
# =========================================================
#
# WHAT:
# Convert cached Stats REST/api-web reference JSON files into Parquet tables.
#
# WHY:
# These small dimensions help interpret IDs, seasons, countries, drafts, and
# other metadata in the larger warehouse.
#
# PYTHON NOTES:
# - dictionary values define source filename plus the JSON key containing rows.
# =========================================================

reference_specs = {
    "franchises": (
        "franchises.json",
        "data",
    ),
    "seasons": (
        "seasons.json",
        "data",
    ),
    "component_seasons": (
        "component_seasons.json",
        "data",
    ),
    "countries": (
        "countries.json",
        "data",
    ),
    "drafts": (
        "drafts.json",
        "data",
    ),
    "stat_glossary": (
        "glossary.json",
        "data",
    ),
    "standings_seasons": (
        "standings_seasons.json",
        "seasons",
    ),
}

reference_tables_written = []

for table_name, (
    file_name,
    row_key,
) in reference_specs.items():
    input_file = (
        RAW_DIR
        / "reference"
        / file_name
    )

    if not input_file.exists():
        print(
            f"SKIP {table_name}: "
            f"{input_file} not found"
        )
        continue

    with input_file.open(
        "r",
        encoding="utf-8",
    ) as file:
        data = json.load(file)

    if isinstance(data, dict):
        rows = (
            data.get(row_key, [])
            or []
        )
    else:
        rows = []

    if not rows:
        print(
            f"SKIP {table_name}: "
            "no rows"
        )
        continue

    reference_df = pd.json_normalize(
        rows,
        sep="_",
    )

    output_file = (
        OUTPUT_DIR
        / f"{table_name}.parquet"
    )

    reference_df.to_parquet(
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

    reference_tables_written.append(
        table_name
    )

    print(
        f"{table_name}: "
        f"{len(reference_df):,} rows -> "
        f"{output_file}"
    )

print("\nSection 8: Global reference tables built.")


# %%
# =========================================================
# 9. Build the draft-picks table
# =========================================================
#
# WHAT:
# Combine all cached draft-round responses and attach season/draft-year context.
#
# WHY:
# One consolidated draft-pick table is easier to join to player-development
# and roster research than separate files for each round.
# =========================================================

draft_pick_frames = []

for season_id in seasons:
    base_dir = (
        RAW_DIR
        / season_id
        / "draft_picks"
    )

    if not base_dir.exists():
        continue

    for path in sorted(
        base_dir.glob("round_*.json")
    ):
        with path.open(
            "r",
            encoding="utf-8",
        ) as file:
            data = json.load(file)

        rows = (
            data.get("picks", [])
            or data.get("data", [])
            or []
        )

        if not rows:
            continue

        draft_round = pd.json_normalize(
            rows,
            sep="_",
        )

        draft_round.insert(
            0,
            "season_id",
            season_id,
        )

        draft_round.insert(
            1,
            "draft_year",
            int(season_id[-4:]),
        )

        draft_pick_frames.append(
            draft_round
        )

draft_picks = write_table(
    "draft_picks",
    draft_pick_frames,
)

print("\nSection 9: Draft-picks table built.")


# %%
# =========================================================
# 10. Build the playoff-series table
# =========================================================
#
# WHAT:
# Flatten saved playoff-bracket series structures for each season.
#
# WHY:
# Series context is difficult to infer reliably from individual game rows alone.
# =========================================================

playoff_frames = []

for season_id in seasons:
    input_file = (
        RAW_DIR
        / season_id
        / "playoffs"
        / "bracket.json"
    )

    if not input_file.exists():
        continue

    with input_file.open(
        "r",
        encoding="utf-8",
    ) as file:
        data = json.load(file)

    rows = (
        data.get("series", [])
        or data.get("seriesGroups", [])
        or []
    )

    if not rows:
        continue

    playoff_series_season = pd.json_normalize(
        rows,
        sep="_",
    )

    playoff_series_season.insert(
        0,
        "season_id",
        season_id,
    )

    playoff_frames.append(
        playoff_series_season
    )

playoff_series = write_table(
    "playoff_series",
    playoff_frames,
)

print("\nSection 10: Playoff-series table built.")


# %%
# =========================================================
# 11. Print supplemental-table build summary
# =========================================================
#
# WHAT:
# Report key table sizes and how many global reference tables were written.
#
# WHY:
# This provides a quick sanity check before these tables enter the curated layer.
# =========================================================

print("\nSupplemental table build complete.")
print(f"Standings rows: {len(standings_daily):,}")
print(f"Player game-log rows: {len(player_game_logs):,}")
print(f"Club skater rows: {len(club_skater_season_stats):,}")
print(f"Club goalie rows: {len(club_goalie_season_stats):,}")
print(f"Draft-pick rows: {len(draft_picks):,}")
print(f"Playoff-series rows: {len(playoff_series):,}")
print(
    "Reference tables written: "
    f"{len(reference_tables_written):,}"
)

print("\nSection 11: Supplemental-table build summary complete.")
