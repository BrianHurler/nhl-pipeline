# %% [markdown]
# 12_download_supplemental_api.py
#
# PURPOSE
# -------
# Download supplemental NHL public API data that is useful beyond the core
# gamecenter and shift feeds.
#
# This includes:
# - global reference datasets
# - historical standings snapshots
# - club season statistics
# - player game logs
# - playoff brackets
# - draft picks
#
# UNIT OF OUTPUT
# --------------
# One raw JSON file = one API response for one reference/date/team/player/season job.

# %%
# =========================================================
# 1. Import libraries
# =========================================================
#
# WHAT:
# Load command-line, JSON, timing, filesystem, Jupyter-detection, pandas, and
# HTTP/retry tools.
#
# WHY:
# This script creates many different API jobs and caches every response locally.
# =========================================================

import argparse
import json
from pathlib import Path
import sys
import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

print("\nSection 1: Libraries loaded.")


# %%
# =========================================================
# 2. Configure paths, API endpoints, and request behavior
# =========================================================
#
# WHAT:
# Locate the manifest/player table/raw archive and define the two NHL API bases.
#
# WHY:
# Supplemental data comes from both api-web.nhle.com and Stats REST.
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

MANIFEST_FILE = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "game_manifest.csv"
)

PLAYERS_FILE = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "players.parquet"
)

RAW_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
)

LOG_DIR = (
    PROJECT_ROOT
    / "data"
    / "logs"
)

FAILURE_LOG = (
    LOG_DIR
    / "supplemental_download_failures.csv"
)

WEB_BASE = "https://api-web.nhle.com/v1"
STATS_BASE = "https://api.nhle.com/stats/rest/en"

REQUEST_TIMEOUT = 30
REQUEST_PAUSE_SECONDS = 0.05

print("\nSection 2: Supplemental API paths and request settings configured.")


# %%
# =========================================================
# 3. Parse optional command-line arguments
# =========================================================
#
# WHAT:
# Allow a season filter and an --overwrite flag.
#
# WHY:
# Supplemental downloads can be large, so targeted/restartable execution matters.
#
# PYTHON NOTES:
# - parse_known_args(): safely ignores Jupyter's kernel arguments.
# - parse_args(): stays strict during normal terminal execution.
# =========================================================

parser = argparse.ArgumentParser(
    description="Download supplemental NHL public API data."
)

parser.add_argument(
    "--season-id",
    default=None,
    help="Optional NHL season ID such as 20252026.",
)

parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Redownload files that already exist.",
)

if "ipykernel" in sys.modules:
    args, jupyter_extra_args = parser.parse_known_args()
else:
    args = parser.parse_args()
    jupyter_extra_args = []

print("\nSection 3: Command-line arguments parsed.")


# %%
# =========================================================
# 4. Load and filter the game manifest
# =========================================================
#
# WHAT:
# Read game_manifest.csv and optionally retain only one NHL season.
#
# WHY:
# The dates, team-season combinations, game types, and seasons in this manifest
# determine which supplemental API jobs are relevant.
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

if args.season_id:
    manifest = manifest.loc[
        manifest["season_id"]
        == str(args.season_id)
    ].copy()

if manifest.empty:
    raise RuntimeError(
        "No manifest rows matched the requested season."
    )

manifest = (
    manifest
    .drop_duplicates(
        subset=["season_id", "game_id"]
    )
    .reset_index(drop=True)
)

print(f"Manifest games in scope: {len(manifest):,}")
print("\nSection 4: Game manifest loaded and filtered.")


# %%
# =========================================================
# 5. Create a retry-enabled HTTP session
# =========================================================
#
# WHAT:
# Configure automatic retries for temporary API failures.
#
# WHY:
# This script can generate thousands of requests; one short-lived 429/5xx error
# should not force a complete restart.
# =========================================================

retry = Retry(
    total=5,
    connect=5,
    read=5,
    status=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)

adapter = HTTPAdapter(
    max_retries=retry
)

session = requests.Session()
session.mount(
    "https://",
    adapter,
)

print("\nSection 5: Retry-enabled HTTP session created.")


# %%
# =========================================================
# 6. Define the reusable download-and-cache operation
# =========================================================
#
# WHAT:
# Create one function that skips cached files, downloads missing/overwritten
# responses, and saves JSON atomically through a temporary file.
#
# WHY:
# The same substantial file-safe download behavior is needed for every endpoint
# family below. Centralizing it avoids repeating error-prone network/file logic.
#
# PYTHON NOTES:
# - return values communicate "downloaded" vs "skipped".
# - *.tmp -> Path.replace(): reduces the chance of keeping partial JSON files.
# =========================================================

def download_json(
    url,
    output_file,
    overwrite=False,
):
    if output_file.exists() and not overwrite:
        return "skipped"

    response = session.get(
        url,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    data = response.json()

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_file = output_file.with_suffix(
        output_file.suffix + ".tmp"
    )

    with temp_file.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=2,
        )

    temp_file.replace(
        output_file
    )

    time.sleep(
        REQUEST_PAUSE_SECONDS
    )

    return "downloaded"

print("\nSection 6: Reusable supplemental download function defined.")


# %%
# =========================================================
# 7. Download global reference endpoints
# =========================================================
#
# WHAT:
# Download API-wide reference files that do not depend on one game/team/player.
#
# WHY:
# These datasets provide reference metadata used throughout the warehouse.
#
# PYTHON NOTES:
# - dictionary: maps a local dataset name to its URL.
# - try/except: records failures while allowing the remaining jobs to continue.
# =========================================================

counts = {
    "downloaded": 0,
    "skipped": 0,
    "failed": 0,
}

failures = []

reference_endpoints = {
    "standings_seasons": f"{WEB_BASE}/standings-season",
    "teams": f"{STATS_BASE}/team",
    "franchises": f"{STATS_BASE}/franchise",
    "seasons": f"{STATS_BASE}/season",
    "component_seasons": f"{STATS_BASE}/componentSeason",
    "countries": f"{STATS_BASE}/country",
    "drafts": f"{STATS_BASE}/draft",
    "glossary": f"{STATS_BASE}/glossary",
    "stats_config": f"{STATS_BASE}/config",
    "game_meta": f"{STATS_BASE}/game/meta",
}

for name, url in reference_endpoints.items():
    output_file = (
        RAW_DIR
        / "reference"
        / f"{name}.json"
    )

    print(f"reference: {name}")

    try:
        result = download_json(
            url,
            output_file,
            overwrite=args.overwrite,
        )

        counts[result] += 1

    except (
        requests.RequestException,
        ValueError,
        OSError,
    ) as error:
        counts["failed"] += 1

        failures.append(
            {
                "label": name,
                "url": url,
                "error": str(error),
            }
        )

        print(
            f"WARNING {name}: "
            f"{error}"
        )

print("\nSection 7: Global reference downloads complete.")


# %%
# =========================================================
# 8. Download historical standings snapshots
# =========================================================
#
# WHAT:
# Request standings for every unique game date represented in the manifest.
#
# WHY:
# Date-specific standings let future models use team state as it existed at that
# point in the season instead of leaking end-of-season information backward.
# =========================================================

standings_dates = sorted(
    manifest["game_date"]
    .dropna()
    .astype(str)
    .unique()
)

for date_number, date in enumerate(
    standings_dates,
    start=1,
):
    label = f"standings {date}"
    url = f"{WEB_BASE}/standings/{date}"

    output_file = (
        RAW_DIR
        / "reference"
        / "standings"
        / f"{date}.json"
    )

    print(
        f"standings "
        f"[{date_number}/{len(standings_dates)}] "
        f"{date}"
    )

    try:
        result = download_json(
            url,
            output_file,
            overwrite=args.overwrite,
        )
        counts[result] += 1

    except (
        requests.RequestException,
        ValueError,
        OSError,
    ) as error:
        counts["failed"] += 1
        failures.append(
            {
                "label": label,
                "url": url,
                "error": str(error),
            }
        )
        print(f"WARNING {label}: {error}")

print("\nSection 8: Historical standings snapshots downloaded.")


# %%
# =========================================================
# 9. Download club statistics by team, season, and game type
# =========================================================
#
# WHAT:
# Build the unique team/season/game-type combinations and request club stats.
#
# WHY:
# These official aggregates are useful for validation and season-level context.
#
# PYTHON NOTES:
# - pd.concat(): combines home-team and away-team combinations.
# - drop_duplicates(): turns repeated game rows into unique API jobs.
# =========================================================

away_team_jobs = (
    manifest[
        [
            "season_id",
            "game_type",
            "away_team",
        ]
    ]
    .rename(
        columns={"away_team": "team"}
    )
)

home_team_jobs = (
    manifest[
        [
            "season_id",
            "game_type",
            "home_team",
        ]
    ]
    .rename(
        columns={"home_team": "team"}
    )
)

team_jobs = pd.concat(
    [
        away_team_jobs,
        home_team_jobs,
    ],
    ignore_index=True,
)

team_jobs = (
    team_jobs
    .drop_duplicates()
    .sort_values(
        [
            "season_id",
            "game_type",
            "team",
        ]
    )
    .reset_index(drop=True)
)

for job_number, row in enumerate(
    team_jobs.itertuples(index=False),
    start=1,
):
    season_id = str(row.season_id)
    game_type = int(row.game_type)
    team = row.team

    label = (
        f"club stats {team} "
        f"{season_id} {game_type}"
    )

    url = (
        f"{WEB_BASE}/club-stats/"
        f"{team}/{season_id}/{game_type}"
    )

    output_file = (
        RAW_DIR
        / season_id
        / "club_stats"
        / str(game_type)
        / f"{team}.json"
    )

    print(
        f"club stats "
        f"[{job_number}/{len(team_jobs)}] "
        f"{team} {season_id} "
        f"type={game_type}"
    )

    try:
        result = download_json(
            url,
            output_file,
            overwrite=args.overwrite,
        )
        counts[result] += 1

    except (
        requests.RequestException,
        ValueError,
        OSError,
    ) as error:
        counts["failed"] += 1
        failures.append(
            {
                "label": label,
                "url": url,
                "error": str(error),
            }
        )
        print(f"WARNING {label}: {error}")

print("\nSection 9: Club-stat downloads complete.")


# %%
# =========================================================
# 10. Download player game logs
# =========================================================
#
# WHAT:
# Cross every discovered player with each season/game-type combination in scope
# and download that player's official game log.
#
# WHY:
# Player logs provide an additional player-game source for analysis and QA.
#
# DATA NOTE:
# Some player/season/game-type combinations legitimately return no useful data.
# Those API outcomes remain visible through the failure log rather than being
# silently interpreted as observed zero games.
# =========================================================

player_log_jobs = []

if PLAYERS_FILE.exists():
    players = pd.read_parquet(
        PLAYERS_FILE
    )

    player_ids = sorted(
        players["player_id"]
        .dropna()
        .astype(int)
        .unique()
    )

    season_game_types = (
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

    for season_row in season_game_types.itertuples(
        index=False
    ):
        for player_id in player_ids:
            player_log_jobs.append(
                (
                    player_id,
                    str(season_row.season_id),
                    int(season_row.game_type),
                )
            )

    for job_number, (
        player_id,
        season_id,
        game_type,
    ) in enumerate(
        player_log_jobs,
        start=1,
    ):
        label = (
            f"player log {player_id} "
            f"{season_id} {game_type}"
        )

        url = (
            f"{WEB_BASE}/player/"
            f"{player_id}/game-log/"
            f"{season_id}/{game_type}"
        )

        output_file = (
            RAW_DIR
            / season_id
            / "player_game_logs"
            / str(game_type)
            / f"{player_id}.json"
        )

        print(
            f"player log "
            f"[{job_number}/{len(player_log_jobs)}] "
            f"player={player_id} "
            f"season={season_id} "
            f"type={game_type}"
        )

        try:
            result = download_json(
                url,
                output_file,
                overwrite=args.overwrite,
            )
            counts[result] += 1

        except (
            requests.RequestException,
            ValueError,
            OSError,
        ) as error:
            counts["failed"] += 1
            failures.append(
                {
                    "label": label,
                    "url": url,
                    "error": str(error),
                }
            )
            print(f"WARNING {label}: {error}")

else:
    players = pd.DataFrame()
    player_ids = []

    print(
        "SKIP player logs: "
        f"{PLAYERS_FILE} does not exist."
    )

print("\nSection 10: Player game-log downloads complete.")


# %%
# =========================================================
# 11. Download playoff brackets and draft picks
# =========================================================
#
# WHAT:
# For every season, download the playoff bracket when playoff games are present
# and download draft-pick data for rounds 1 through 7.
#
# WHY:
# These sources add postseason structure and draft context to the warehouse.
#
# NHL DATA NOTE:
# The season ID 20252026 corresponds to a draft/playoff end year of 2026.
# =========================================================

season_ids = sorted(
    manifest["season_id"]
    .astype(str)
    .unique()
)

for season_id in season_ids:
    end_year = int(
        season_id[-4:]
    )

    season_manifest = manifest.loc[
        manifest["season_id"] == season_id
    ]

    has_playoffs = (
        season_manifest["game_type"]
        == 3
    ).any()

    if has_playoffs:
        label = (
            f"playoff bracket {end_year}"
        )

        url = (
            f"{WEB_BASE}/playoff-bracket/"
            f"{end_year}"
        )

        output_file = (
            RAW_DIR
            / season_id
            / "playoffs"
            / "bracket.json"
        )

        try:
            result = download_json(
                url,
                output_file,
                overwrite=args.overwrite,
            )
            counts[result] += 1

        except (
            requests.RequestException,
            ValueError,
            OSError,
        ) as error:
            counts["failed"] += 1
            failures.append(
                {
                    "label": label,
                    "url": url,
                    "error": str(error),
                }
            )
            print(f"WARNING {label}: {error}")

    for round_number in range(1, 8):
        label = (
            f"draft picks {end_year} "
            f"round {round_number}"
        )

        url = (
            f"{WEB_BASE}/draft/picks/"
            f"{end_year}/{round_number}"
        )

        output_file = (
            RAW_DIR
            / season_id
            / "draft_picks"
            / f"round_{round_number}.json"
        )

        try:
            result = download_json(
                url,
                output_file,
                overwrite=args.overwrite,
            )
            counts[result] += 1

        except (
            requests.RequestException,
            ValueError,
            OSError,
        ) as error:
            counts["failed"] += 1
            failures.append(
                {
                    "label": label,
                    "url": url,
                    "error": str(error),
                }
            )
            print(f"WARNING {label}: {error}")

print("\nSection 11: Playoff-bracket and draft-pick downloads complete.")


# %%
# =========================================================
# 12. Save failures and print the run summary
# =========================================================
#
# WHAT:
# Write any failed jobs to CSV and report downloaded/skipped/failed counts.
#
# WHY:
# A large multi-endpoint ingestion step needs a durable audit trail.
# =========================================================

LOG_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

if failures:
    failures_df = pd.DataFrame(
        failures
    )

    failures_df.to_csv(
        FAILURE_LOG,
        index=False,
    )

    print(f"Failure log: {FAILURE_LOG}")

else:
    failures_df = pd.DataFrame()

    if FAILURE_LOG.exists():
        FAILURE_LOG.unlink()

print("\nSupplemental download complete.")
print(f"New files downloaded: {counts['downloaded']:,}")
print(f"Existing files skipped: {counts['skipped']:,}")
print(f"Failed downloads: {counts['failed']:,}")

print("\nSection 12: Supplemental-download summary complete.")
