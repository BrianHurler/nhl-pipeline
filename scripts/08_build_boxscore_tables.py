# %% [markdown]
# 08_build_boxscore_tables.py
#
# PURPOSE
# -------
# Convert raw gamecenter boxscore JSON into three reusable processed tables:
#
# 1. skater_game_stats
# 2. goalie_game_stats
# 3. team_game_stats
#
# UNIT OF OBSERVATION
# -------------------
# skaters: one skater-game
# goalies: one goalie-game
# teams:   one team-game (normally two rows per game)

# %%
# =========================================================
# 1. Import libraries
# =========================================================
#
# WHAT:
# Load JSON, filesystem, and pandas tools.
#
# WHY:
# We need to read saved boxscore JSON, flatten player records, and write Parquet.
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
# Define the manifest/raw directories and the three processed output files.
#
# WHY:
# The script creates different analytical grains from the same boxscore source.
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

S3_BUCKET = "nhl-data-bucket1"
S3_REGION = "us-east-1"
S3_PREFIX = "nhl-data-research"
s3 = boto3.client("s3", region_name=S3_REGION)
MANIFEST_FILE = PROJECT_ROOT / "data" / "processed" / "game_manifest.csv"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"

SKATER_FILE = OUTPUT_DIR / "skater_game_stats.parquet"
GOALIE_FILE = OUTPUT_DIR / "goalie_game_stats.parquet"
TEAM_FILE = OUTPUT_DIR / "team_game_stats.parquet"

print("\nSection 2: Boxscore input and output paths configured.")


# %%
# =========================================================
# 3. Load and prepare the game manifest
# =========================================================
#
# WHAT:
# Read one row per game from game_manifest.csv.
#
# WHY:
# The manifest supplies stable season, date, home/away, team, and opponent
# context that gets attached to boxscore records.
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
# 4. Parse raw boxscores into skater, goalie, and team records
# =========================================================
#
# WHAT:
# Loop through boxscore files and build three lists of record dictionaries.
#
# WHY:
# Player and team statistics live at different grains, so keeping separate
# outputs avoids one giant table with repeated/inapplicable fields.
#
# PYTHON NOTES:
# - pd.json_normalize(record).iloc[0].to_dict(): flattens one nested record.
# - dictionary unpacking {**a, **b}: combines dictionaries explicitly.
# - nested loops: walk home/away sides, position groups, and players.
#
# LEARNING NOTE:
# Game/team context is attached in-line rather than hidden behind a small helper
# so you can see exactly which fields enter each output record.
# =========================================================

skater_rows = []
goalie_rows = []
team_rows = []

files_read = 0
files_missing = 0
files_failed = 0

for row in manifest.itertuples(index=False):
    season_id = str(row.season_id)
    game_id = int(row.game_id)

    input_file = (
        RAW_DIR
        / season_id
        / "boxscore"
        / f"{game_id}.json"
    )

    if not input_file.exists():
        files_missing += 1
        continue

    try:
        with input_file.open("r", encoding="utf-8") as file:
            data = json.load(file)

        player_stats = data.get(
            "playerByGameStats",
            {},
        )

        side_info = {
            "awayTeam": {
                "team_id": row.away_team_id,
                "team": row.away_team,
                "opponent_team_id": row.home_team_id,
                "opponent_team": row.home_team,
                "home_away": "away",
            },
            "homeTeam": {
                "team_id": row.home_team_id,
                "team": row.home_team,
                "opponent_team_id": row.away_team_id,
                "opponent_team": row.away_team,
                "home_away": "home",
            },
        }

        for side_key, info in side_info.items():
            side = (
                player_stats.get(side_key, {})
                or {}
            )

            for position_group in ["forwards", "defense"]:
                players_in_group = (
                    side.get(position_group, [])
                    or []
                )

                for player in players_in_group:
                    player_flat = (
                        pd.json_normalize(
                            player,
                            sep="_",
                        )
                        .iloc[0]
                        .to_dict()
                    )

                    player_context = {
                        "season_id": season_id,
                        "season": row.season,
                        "game_id": game_id,
                        "game_type": row.game_type,
                        "game_date": row.game_date,
                        "team_id": info["team_id"],
                        "team": info["team"],
                        "opponent_team_id": info["opponent_team_id"],
                        "opponent_team": info["opponent_team"],
                        "home_away": info["home_away"],
                        "position_group": position_group,
                    }

                    skater_record = {
                        **player_context,
                        **player_flat,
                    }

                    skater_rows.append(
                        skater_record
                    )

            goalies_on_side = (
                side.get("goalies", [])
                or []
            )

            for goalie in goalies_on_side:
                goalie_flat = (
                    pd.json_normalize(
                        goalie,
                        sep="_",
                    )
                    .iloc[0]
                    .to_dict()
                )

                goalie_context = {
                    "season_id": season_id,
                    "season": row.season,
                    "game_id": game_id,
                    "game_type": row.game_type,
                    "game_date": row.game_date,
                    "team_id": info["team_id"],
                    "team": info["team"],
                    "opponent_team_id": info["opponent_team_id"],
                    "opponent_team": info["opponent_team"],
                    "home_away": info["home_away"],
                }

                goalie_record = {
                    **goalie_context,
                    **goalie_flat,
                }

                goalie_rows.append(
                    goalie_record
                )

        # Team-game records begin with stable game/team context.
        away_record = {
            "season_id": season_id,
            "season": row.season,
            "game_id": game_id,
            "game_type": row.game_type,
            "game_date": row.game_date,
            "team_id": row.away_team_id,
            "team": row.away_team,
            "opponent_team_id": row.home_team_id,
            "opponent_team": row.home_team,
            "home_away": "away",
        }

        home_record = {
            "season_id": season_id,
            "season": row.season,
            "game_id": game_id,
            "game_type": row.game_type,
            "game_date": row.game_date,
            "team_id": row.home_team_id,
            "team": row.home_team,
            "opponent_team_id": row.away_team_id,
            "opponent_team": row.away_team,
            "home_away": "home",
        }

        # teamGameStats arrives as category/value pairs. Turn each category
        # into a normal column on the away and home team records.
        team_game_stats = (
            data.get("teamGameStats", [])
            or []
        )

        for stat in team_game_stats:
            category = stat.get("category")

            if not category:
                continue

            away_record[category] = stat.get(
                "awayValue"
            )
            home_record[category] = stat.get(
                "homeValue"
            )

        away_team_top = (
            data.get("awayTeam", {})
            or {}
        )
        home_team_top = (
            data.get("homeTeam", {})
            or {}
        )

        away_record["score"] = away_team_top.get(
            "score"
        )
        away_record["sog"] = away_team_top.get(
            "sog"
        )

        home_record["score"] = home_team_top.get(
            "score"
        )
        home_record["sog"] = home_team_top.get(
            "sog"
        )

        team_rows.append(away_record)
        team_rows.append(home_record)

        files_read += 1

    except (OSError, ValueError, TypeError, IndexError) as error:
        files_failed += 1
        print(f"ERROR reading game_id={game_id}: {error}")

print("\nSection 4: Raw boxscores parsed into player and team records.")


# %%
# =========================================================
# 5. Convert record lists into DataFrames
# =========================================================
#
# WHAT:
# Turn the three lists of dictionaries into pandas DataFrames.
#
# WHY:
# This creates explicit objects at each analytical grain that can be inspected
# in the Interactive Variables pane before anything is written to disk.
#
# PYTHON NOTES:
# - pd.DataFrame(list_of_dicts): one dictionary becomes one row.
# =========================================================

skaters = pd.DataFrame(skater_rows)
goalies = pd.DataFrame(goalie_rows)
teams = pd.DataFrame(team_rows)

print(f"Skater-game rows: {len(skaters):,}")
print(f"Goalie-game rows: {len(goalies):,}")
print(f"Team-game rows: {len(teams):,}")

print("\nSection 5: Boxscore DataFrames created.")


# %%
# =========================================================
# 6. Save the processed boxscore tables
# =========================================================
#
# WHAT:
# Write each non-empty DataFrame to Parquet.
#
# WHY:
# These tables become reusable player-game and team-game analytical layers.
#
# PYTHON NOTES:
# - DataFrame.empty: quickly checks whether a DataFrame has zero rows.
# - to_parquet(): writes typed columnar data.
# =========================================================

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

if not skaters.empty:
    skaters.to_parquet(
        SKATER_FILE,
        index=False,
    )

    s3_key = (
        f"{S3_PREFIX}/"
        f"{SKATER_FILE.relative_to(PROJECT_ROOT).as_posix()}"
    )

    s3.upload_file(
        str(SKATER_FILE),
        S3_BUCKET,
        s3_key,
    )

    print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")

if not goalies.empty:
    goalies.to_parquet(
        GOALIE_FILE,
        index=False,
    )

    s3_key = (
        f"{S3_PREFIX}/"
        f"{GOALIE_FILE.relative_to(PROJECT_ROOT).as_posix()}"
    )

    s3.upload_file(
        str(GOALIE_FILE),
        S3_BUCKET,
        s3_key,
    )

    print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")

if not teams.empty:
    teams.to_parquet(
        TEAM_FILE,
        index=False,
    )

    s3_key = (
        f"{S3_PREFIX}/"
        f"{TEAM_FILE.relative_to(PROJECT_ROOT).as_posix()}"
    )

    s3.upload_file(
        str(TEAM_FILE),
        S3_BUCKET,
        s3_key,
    )

    print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")

print("\nSection 6: Boxscore tables saved to Parquet.")


# %%
# =========================================================
# 7. Print build QA summary
# =========================================================
#
# WHAT:
# Report file coverage and final table row counts.
#
# WHY:
# These totals help identify missing raw data or unexpectedly empty outputs.
# =========================================================

print("\nBoxscore table build complete.")
print(f"Boxscore files read: {files_read}")
print(f"Missing boxscore files: {files_missing}")
print(f"Failed files: {files_failed}")
print(f"Skater-game rows: {len(skaters):,}")
print(f"Goalie-game rows: {len(goalies):,}")
print(f"Team-game rows: {len(teams):,}")
print(f"Skater output: {SKATER_FILE}")
print(f"Goalie output: {GOALIE_FILE}")
print(f"Team output: {TEAM_FILE}")

print("\nSection 7: Boxscore-table QA summary complete.")
