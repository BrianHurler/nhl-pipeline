# %% [markdown]
# 09_build_dimensions.py
#
# PURPOSE
# -------
# Build reusable player, roster, and team dimension tables.
#
# Dimensions answer descriptive questions such as:
# - Who is this player?
# - Which team/season roster was the player associated with?
# - What team does this NHL team ID represent?
#
# OUTPUT GRAINS
# -------------
# rosters: one player-team-season
# players: one player
# teams:   one NHL team reference record

# %%
# =========================================================
# 1. Import libraries
# =========================================================
#
# WHAT:
# Load JSON, timing, filesystem, pandas, and web-request tools.
#
# WHY:
# Some reference data may already exist locally; missing records are downloaded
# and cached as raw JSON before processed dimension tables are created.
# =========================================================

import json
from pathlib import Path

import boto3
import time

import pandas as pd
import requests

print("\nSection 1: Libraries loaded.")


# %%
# =========================================================
# 2. Configure paths and API base URLs
# =========================================================
#
# WHAT:
# Locate the manifest/raw/processed directories and two NHL API families.
#
# WHY:
# Rosters and player landing pages use api-web.nhle.com, while the team
# reference table uses the NHL Stats REST API.
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

S3_BUCKET = "nhl-data-bucket1"
S3_REGION = "us-east-1"
S3_PREFIX = "nhl-data-research"
s3 = boto3.client("s3", region_name=S3_REGION)
MANIFEST_FILE = PROJECT_ROOT / "data" / "processed" / "game_manifest.csv"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"

WEB_BASE = "https://api-web.nhle.com/v1"
STATS_BASE = "https://api.nhle.com/stats/rest/en"

ROSTER_FILE = OUTPUT_DIR / "rosters.parquet"
PLAYER_FILE = OUTPUT_DIR / "players.parquet"
TEAM_FILE = OUTPUT_DIR / "teams.parquet"

print("\nSection 2: Dimension-table paths and API URLs configured.")


# %%
# =========================================================
# 3. Build the unique season-team combinations
# =========================================================
#
# WHAT:
# Read the manifest, collect home and away teams, and create one unique row for
# every season/team pair.
#
# WHY:
# We need exactly one roster request for each team-season represented in games.
#
# PYTHON NOTES:
# - rename(): gives away/home columns one common name.
# - pd.concat(): stacks away-team and home-team rows.
# - drop_duplicates(): leaves one season/team combination.
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

away_pairs = (
    manifest[["season_id", "away_team"]]
    .rename(columns={"away_team": "team"})
)

home_pairs = (
    manifest[["season_id", "home_team"]]
    .rename(columns={"home_team": "team"})
)

season_team_pairs = pd.concat(
    [away_pairs, home_pairs],
    ignore_index=True,
)

season_team_pairs = (
    season_team_pairs
    .drop_duplicates()
    .sort_values(["season_id", "team"])
    .reset_index(drop=True)
)

print(f"Unique season-team combinations: {len(season_team_pairs):,}")
print("\nSection 3: Unique season-team roster list created.")


# %%
# =========================================================
# 4. Download/cache rosters and build the roster dimension
# =========================================================
#
# WHAT:
# Load one roster JSON per season/team, downloading it only when not cached,
# then create one simple record per rostered player.
#
# WHY:
# The roster dimension preserves player-team-season association independently
# from the player's current team.
#
# PYTHON NOTES:
# - Path.read_text(): reads a whole text file.
# - json.loads(): parses JSON text into Python objects.
# - isinstance(value, dict): identifies localized NHL text objects.
#
# NHL API NOTE:
# Names/cities can sometimes arrive as dictionaries such as {"default": "..."}.
# We explicitly unwrap those values below rather than hiding that operation in
# a helper function.
# =========================================================

roster_rows = []

for roster_number, row in enumerate(
    season_team_pairs.itertuples(index=False),
    start=1,
):
    season_id = str(row.season_id)
    team = row.team

    raw_file = (
        RAW_DIR
        / season_id
        / "rosters"
        / f"{team}.json"
    )

    print(
        f"[{roster_number}/{len(season_team_pairs)}] "
        f"roster {team} {season_id}"
    )

    if raw_file.exists():
        data = json.loads(
            raw_file.read_text(
                encoding="utf-8"
            )
        )
    else:
        url = (
            f"{WEB_BASE}/roster/"
            f"{team}/{season_id}"
        )

        response = requests.get(
            url,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        raw_file.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with raw_file.open("w", encoding="utf-8") as file:
            json.dump(
                data,
                file,
                ensure_ascii=False,
                indent=2,
            )

        time.sleep(0.1)

    position_groups = [
        ("forwards", "forward"),
        ("defensemen", "defense"),
        ("goalies", "goalie"),
    ]

    for api_group, position_group in position_groups:
        players_in_group = (
            data.get(api_group, [])
            or []
        )

        for player in players_in_group:
            first_name_raw = player.get("firstName")
            last_name_raw = player.get("lastName")
            birth_city_raw = player.get("birthCity")

            first_name = (
                first_name_raw.get("default")
                or first_name_raw.get("en")
                if isinstance(first_name_raw, dict)
                else first_name_raw
            )

            last_name = (
                last_name_raw.get("default")
                or last_name_raw.get("en")
                if isinstance(last_name_raw, dict)
                else last_name_raw
            )

            birth_city = (
                birth_city_raw.get("default")
                or birth_city_raw.get("en")
                if isinstance(birth_city_raw, dict)
                else birth_city_raw
            )

            roster_rows.append(
                {
                    "season_id": season_id,
                    "team": team,
                    "position_group": position_group,
                    "player_id": player.get("id"),
                    "first_name": first_name,
                    "last_name": last_name,
                    "sweater_number": player.get("sweaterNumber"),
                    "position_code": player.get("positionCode"),
                    "shoots_catches": player.get("shootsCatches"),
                    "height_in_inches": player.get("heightInInches"),
                    "weight_in_pounds": player.get("weightInPounds"),
                    "birth_date": player.get("birthDate"),
                    "birth_city": birth_city,
                    "birth_country": player.get("birthCountry"),
                }
            )

rosters = pd.DataFrame(roster_rows)

rosters = (
    rosters
    .drop_duplicates(
        ["season_id", "team", "player_id"]
    )
    .reset_index(drop=True)
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

rosters.to_parquet(
    ROSTER_FILE,
    index=False,
)

s3_key = (
    f"{S3_PREFIX}/"
    f"{ROSTER_FILE.relative_to(PROJECT_ROOT).as_posix()}"
)

s3.upload_file(
    str(ROSTER_FILE),
    S3_BUCKET,
    s3_key,
)

print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")
print("\nSection 4: Roster dimension built and saved.")


# %%
# =========================================================
# 5. Collect the universe of player IDs
# =========================================================
#
# WHAT:
# Start with roster player IDs, then add IDs found in skater/goalie game tables.
#
# WHY:
# Boxscore data can reveal players that a roster snapshot alone might miss.
#
# PYTHON NOTES:
# - set(): stores unique values.
# - set.update(): adds many new values while retaining uniqueness.
# - astype(int): standardizes player IDs before creating landing-page requests.
# =========================================================

player_ids = set(
    rosters["player_id"]
    .dropna()
    .astype(int)
)

for file_name in [
    "skater_game_stats.parquet",
    "goalie_game_stats.parquet",
]:
    stats_path = OUTPUT_DIR / file_name

    if not stats_path.exists():
        continue

    stats = pd.read_parquet(
        stats_path
    )

    if "playerId" in stats.columns:
        stats_player_ids = (
            stats["playerId"]
            .dropna()
            .astype(int)
        )

        player_ids.update(
            stats_player_ids
        )

player_ids = sorted(player_ids)

print(f"Unique player IDs to process: {len(player_ids):,}")
print("\nSection 5: Player-ID universe assembled.")


# %%
# =========================================================
# 6. Download/cache player landing pages and build players
# =========================================================
#
# WHAT:
# Load one landing-page response per player and extract stable player attributes.
#
# WHY:
# This creates a one-row-per-player dimension that can be joined anywhere a
# player ID appears.
#
# PYTHON NOTES:
# - try/except: one player failure should not stop the entire dimension build.
# - list.append(): accumulates records before one DataFrame construction.
# =========================================================

player_rows = []

for player_number, player_id in enumerate(
    player_ids,
    start=1,
):
    raw_file = (
        RAW_DIR
        / "players"
        / f"{player_id}.json"
    )

    print(
        f"[{player_number}/{len(player_ids)}] "
        f"player {player_id}"
    )

    try:
        if raw_file.exists():
            data = json.loads(
                raw_file.read_text(
                    encoding="utf-8"
                )
            )
        else:
            url = (
                f"{WEB_BASE}/player/"
                f"{player_id}/landing"
            )

            response = requests.get(
                url,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            raw_file.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            with raw_file.open("w", encoding="utf-8") as file:
                json.dump(
                    data,
                    file,
                    ensure_ascii=False,
                    indent=2,
                )

            time.sleep(0.1)

        first_name_raw = data.get("firstName")
        last_name_raw = data.get("lastName")
        birth_city_raw = data.get("birthCity")

        first_name = (
            first_name_raw.get("default")
            or first_name_raw.get("en")
            if isinstance(first_name_raw, dict)
            else first_name_raw
        )

        last_name = (
            last_name_raw.get("default")
            or last_name_raw.get("en")
            if isinstance(last_name_raw, dict)
            else last_name_raw
        )

        birth_city = (
            birth_city_raw.get("default")
            or birth_city_raw.get("en")
            if isinstance(birth_city_raw, dict)
            else birth_city_raw
        )

        player_rows.append(
            {
                "player_id": player_id,
                "first_name": first_name,
                "last_name": last_name,
                "birth_date": data.get("birthDate"),
                "birth_city": birth_city,
                "birth_country": data.get("birthCountry"),
                "height_in_inches": data.get("heightInInches"),
                "weight_in_pounds": data.get("weightInPounds"),
                "shoots_catches": data.get("shootsCatches"),
                "position": data.get("position"),
                "current_team_id": data.get("currentTeamId"),
                "current_team_abbrev": data.get("currentTeamAbbrev"),
                "sweater_number": data.get("sweaterNumber"),
            }
        )

    except (requests.RequestException, ValueError, OSError, TypeError) as error:
        print(
            f"  WARNING player {player_id}: "
            f"{error}"
        )

players = pd.DataFrame(player_rows)

players = (
    players
    .drop_duplicates(["player_id"])
    .reset_index(drop=True)
)

players.to_parquet(
    PLAYER_FILE,
    index=False,
)

s3_key = (
    f"{S3_PREFIX}/"
    f"{PLAYER_FILE.relative_to(PROJECT_ROOT).as_posix()}"
)

s3.upload_file(
    str(PLAYER_FILE),
    S3_BUCKET,
    s3_key,
)

print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")
print("\nSection 6: Player dimension built and saved.")


# %%
# =========================================================
# 7. Download/cache and build the NHL team reference dimension
# =========================================================
#
# WHAT:
# Read the NHL Stats team reference response and flatten it to one table.
#
# WHY:
# This gives us a stable lookup for NHL team IDs and related reference fields.
#
# PYTHON NOTES:
# - pd.json_normalize(): converts the API's list of team records into columns.
# =========================================================

teams_raw_file = (
    RAW_DIR
    / "reference"
    / "teams.json"
)

if teams_raw_file.exists():
    teams_data = json.loads(
        teams_raw_file.read_text(
            encoding="utf-8"
        )
    )
else:
    teams_url = f"{STATS_BASE}/team"

    response = requests.get(
        teams_url,
        timeout=30,
    )
    response.raise_for_status()

    teams_data = response.json()

    teams_raw_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with teams_raw_file.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            teams_data,
            file,
            ensure_ascii=False,
            indent=2,
        )

teams = pd.json_normalize(
    teams_data.get("data", []),
    sep="_",
)

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
print("\nSection 7: Team reference dimension built and saved.")


# %%
# =========================================================
# 8. Print dimension-build QA summary
# =========================================================
#
# WHAT:
# Report final row counts and output locations.
#
# WHY:
# Dimensions should be easy to sanity-check before downstream joins depend on them.
# =========================================================

print("\nDimension build complete.")
print(f"Roster rows: {len(rosters):,}")
print(f"Player rows: {len(players):,}")
print(f"Team rows: {len(teams):,}")
print(f"Roster output: {ROSTER_FILE}")
print(f"Player output: {PLAYER_FILE}")
print(f"Team output: {TEAM_FILE}")

print("\nSection 8: Dimension-table QA summary complete.")
