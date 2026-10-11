# %% [markdown]
# 03_build_game_manifest.py
#
# PURPOSE
# -------
# Build the project-wide game manifest by downloading each NHL team's schedule
# for the requested season(s).
#
# The manifest is the inventory of games used by later download/build scripts.
# It deliberately keeps one row per unique game even though each game appears
# on both participating teams' schedules.
#
# UNIT OF OBSERVATION
# -------------------
# One row in `games_df` = one unique NHL game within one season.

# %%
# =========================================================
# 1. Import libraries
# =========================================================
#
# WHAT:
# Load tools for API requests, DataFrames, filesystem paths, and brief pauses.
#
# WHY:
# This script needs to request schedules, assemble them into one table, save
# that table, and avoid hitting the NHL API too aggressively.
#
# PYTHON NOTES:
# - pathlib.Path: object-oriented filesystem paths.
# - time.sleep(): pauses execution for a specified number of seconds.
# =========================================================

from pathlib import Path

import boto3
import time

import pandas as pd
import requests

print("\nSection 1: Libraries loaded.")

# %%
# =========================================================
# 2. Configure seasons, teams, API location, and output paths
# =========================================================
#
# WHAT:
# Define which season(s) and teams to request and where to save the manifest.
#
# WHY:
# Keeping editable settings near the top makes the script easy to rerun for a
# different NHL season without searching through the processing code.
#
# PYTHON NOTES:
# - Lists store the set of season IDs and team abbreviations to loop over.
# - Path(__file__).resolve().parents[2] finds the repository root from this
#   script's location.
#
# NHL DATA NOTE:
# NHL season IDs concatenate the starting and ending years:
# 20252026 means the 2025-26 season.
# =========================================================

SEASONS = [
    "20252026",
]

TEAMS = [
    "ANA", "BOS", "BUF", "CAR", "CBJ", "CGY", "CHI", "COL",
    "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NJD",
    "NSH", "NYI", "NYR", "OTT", "PHI", "PIT", "SEA", "SJS",
    "STL", "TBL", "TOR", "UTA", "VAN", "VGK", "WPG", "WSH",
]

BASE_URL = "https://api-web.nhle.com/v1"

PROJECT_ROOT = Path(__file__).resolve().parents[2]

S3_BUCKET = "nhl-data-bucket1"
S3_REGION = "us-east-1"
S3_PREFIX = "nhl-data-research"
s3 = boto3.client("s3", region_name=S3_REGION)
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_FILE = OUTPUT_DIR / "game_manifest.csv"

print("\nSection 2: Manifest configuration and paths set.")

# %%
# =========================================================
# 3. Download every team schedule and collect game records
# =========================================================
#
# WHAT:
# Loop over requested seasons and teams, download each team schedule, and
# append a simple dictionary for every returned game to `all_games`.
#
# WHY:
# The team schedule endpoint is an easy way to discover the complete game
# universe. A game will appear twice overall (once on each team's schedule);
# we intentionally collect everything first and deduplicate afterward.
#
# PYTHON NOTES:
# - nested for loops: iterate through every season/team combination.
# - try/except: lets one failed request be reported without ending the entire
#   manifest build.
# - dict.get(key, default): safely reads possibly missing JSON fields.
# - list.append(): adds one game dictionary to the growing list.
# - time.sleep(0.1): adds a small pause between API requests.
#
# LEARNING NOTE:
# These steps are intentionally kept in-line rather than hidden in small
# helper functions so the API -> JSON -> row-building flow remains visible.
# =========================================================

all_games = []

for season_id in SEASONS:
    print(f"\nDownloading season: {season_id}")

    # Convert "20252026" to a friendlier "2025-26" label.
    start_year = season_id[:4]
    end_year = season_id[6:]
    season = f"{start_year}-{end_year}"

    for team in TEAMS:
        print(f"  {team}")

        url = f"{BASE_URL}/club-schedule-season/{team}/{season_id}"

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as error:
            print(f"    ERROR: {error}")
            continue

        for game in data.get("games", []):
            away_team = game.get("awayTeam", {})
            home_team = game.get("homeTeam", {})

            game_record = {
                "season_id": season_id,
                "season": season,
                "game_id": game.get("id"),
                "game_type": game.get("gameType"),
                "game_date": game.get("gameDate"),
                "game_state": game.get("gameState"),
                "schedule_state": game.get("gameScheduleState"),
                "away_team_id": away_team.get("id"),
                "away_team": away_team.get("abbrev"),
                "home_team_id": home_team.get("id"),
                "home_team": home_team.get("abbrev"),
            }

            all_games.append(game_record)

        # Be polite to the public API rather than firing requests back-to-back.
        time.sleep(0.1)

print("\nSection 3: Team schedules downloaded and game records collected.")

# %%
# =========================================================
# 4. Convert downloaded game records into a clean DataFrame
# =========================================================
#
# WHAT:
# Turn `all_games` into a DataFrame, remove duplicate games, and sort it.
#
# WHY:
# Each game appears on both teams' schedules. The project manifest needs one
# canonical row per season/game combination.
#
# PYTHON NOTES:
# - pd.DataFrame(): converts a list of dictionaries into a table.
# - drop_duplicates(): keeps one row for each unique key.
# - sort_values(): orders rows using one or more columns.
# - reset_index(drop=True): replaces the old row index after sorting.
# =========================================================

games_df = pd.DataFrame(all_games)

if games_df.empty:
    raise RuntimeError("No games were downloaded.")

games_df = (
    games_df
    .drop_duplicates(subset=["season_id", "game_id"])
    .sort_values(["season_id", "game_date", "game_id"])
    .reset_index(drop=True)
)

print("\nSection 4: Game records converted into a clean DataFrame.")

# %%
# =========================================================
# 5. Merge with an existing manifest when one already exists
# =========================================================
#
# WHAT:
# Read the existing CSV, stack old and newly downloaded rows, and deduplicate.
#
# WHY:
# This makes the manifest update-friendly. We can add or refresh seasons
# without throwing away rows that were already present.
#
# PYTHON NOTES:
# - Path.mkdir(..., exist_ok=True): creates the output directory if needed.
# - Path.exists(): checks whether the current manifest already exists.
# - pd.read_csv(): reads a CSV into a DataFrame.
# - pd.concat(): vertically stacks DataFrames.
# - keep="last": lets the newly downloaded version replace an older duplicate.
# =========================================================

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

if OUTPUT_FILE.exists():
    existing_df = pd.read_csv(
        OUTPUT_FILE,
        dtype={"season_id": str},
    )

    games_df = pd.concat(
        [existing_df, games_df],
        ignore_index=True,
    )

    games_df = (
        games_df
        .drop_duplicates(
            subset=["season_id", "game_id"],
            keep="last",
        )
        .sort_values(["season_id", "game_date", "game_id"])
        .reset_index(drop=True)
    )

print("\nSection 5: Existing manifest merged when available.")

# %%
# =========================================================
# 6. Save the game manifest
# =========================================================
#
# WHAT:
# Write the finished manifest to CSV.
#
# WHY:
# Later scripts use this file as the authoritative list of games to download
# and process.
#
# PYTHON NOTES:
# - DataFrame.to_csv(): writes a DataFrame to a CSV file.
# - index=False: avoids saving pandas' row index as an extra column.
# =========================================================

games_df.to_csv(OUTPUT_FILE, index=False)

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
print("\nSection 6: Game manifest saved.")

# %%
# =========================================================
# 7. Print QA summaries
# =========================================================
#
# WHAT:
# Report where the file was saved, counts by season/game type, total games,
# and a small row preview.
#
# WHY:
# A pipeline step should make it easy to confirm that the output is plausible
# before downstream scripts rely on it.
#
# PYTHON NOTES:
# - groupby().size(): counts rows inside each group.
# - reset_index(name="games"): converts grouped counts back to a DataFrame.
# - len(DataFrame): returns the number of rows.
# =========================================================

games_by_season_type = (
    games_df
    .groupby(["season", "game_type"])
    .size()
    .reset_index(name="games")
)

print("\nSaved game manifest:")
print(OUTPUT_FILE)

print("\nGames by season and game type:")
print(games_by_season_type)

print("\nTotal games:")
print(len(games_df))

print("\nFirst five rows:")
print(games_df.head())

print("\nSection 7: Manifest QA summary complete.")
