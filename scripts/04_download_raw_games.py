# %% [markdown]
# 04_download_raw_games.py
#
# PURPOSE
# -------
# Use game_manifest.csv to download and preserve the raw NHL JSON for each game.
#
# For every manifest game, this script requests three endpoints:
# 1. play-by-play
# 2. boxscore
# 3. landing
#
# Raw responses are stored unchanged so later processing scripts can be rebuilt
# without repeatedly downloading the source data.
#
# UNIT OF OBSERVATION
# -------------------
# This script writes files rather than one analytical table:
# one JSON file = one game + one endpoint.

# %%
# =========================================================
# 1. Import libraries
# =========================================================
#
# WHAT:
# Load command-line, JSON, filesystem, timing, tabular, and web-request tools.
#
# WHY:
# This script needs to read the manifest, communicate with the NHL API,
# retry temporary failures, and safely write many JSON files.
#
# PYTHON NOTES:
# - argparse: defines optional command-line arguments.
# - json: reads/writes JSON data.
# - sys: lets us detect whether the script is running inside Jupyter.
# - requests.Session(): reuses HTTP connections across many requests.
# - Retry + HTTPAdapter: add automatic retry behavior to requests.
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
# 2. Configure endpoints, paths, and request behavior
# =========================================================
#
# WHAT:
# Define the NHL API base URL, project file locations, requested endpoints,
# timeout, and pause between requests.
#
# WHY:
# Centralizing configuration makes the download behavior visible and easy to
# adjust without changing the main loop.
#
# PYTHON NOTES:
# - dictionary: ENDPOINTS maps readable names to endpoint URL templates.
# - pathlib's "/" operator joins path pieces cleanly.
# =========================================================

BASE_URL = "https://api-web.nhle.com/v1"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_FILE = PROJECT_ROOT / "data" / "processed" / "game_manifest.csv"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LOG_DIR = PROJECT_ROOT / "data" / "logs"
FAILURE_LOG = LOG_DIR / "download_failures.csv"

ENDPOINTS = {
    "play_by_play": "gamecenter/{game_id}/play-by-play",
    "boxscore": "gamecenter/{game_id}/boxscore",
    "landing": "gamecenter/{game_id}/landing",
}

REQUEST_TIMEOUT = 30
REQUEST_PAUSE_SECONDS = 0.10

print("\nSection 2: Download configuration and paths set.")

# %%
# =========================================================
# 3. Parse optional command-line arguments
# =========================================================
#
# WHAT:
# Allow an optional season filter and an --overwrite flag.
#
# WHY:
# The full downloader can be expensive to rerun. These arguments let us target
# one season or deliberately replace files when needed.
#
# PYTHON NOTES:
# - argparse.ArgumentParser(): defines a command-line interface.
# - action="store_true": creates a Boolean flag that is False unless supplied.
# - parse_args(): strict parsing for normal terminal execution.
# - parse_known_args(): used only inside Jupyter because ipykernel injects its
#   own arguments (for example a kernel connection-file argument).
#
# INTERACTIVE NOTE:
# Keeping strict parse_args() outside Jupyter means a typo in a real terminal
# command still produces an error instead of being silently ignored.
# =========================================================

parser = argparse.ArgumentParser(
    description="Download raw NHL game JSON using game_manifest.csv."
)

parser.add_argument(
    "--season-id",
    help="Optional NHL season ID such as 20252026. Default: all seasons in manifest.",
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
# 4. Load, validate, and optionally filter the game manifest
# =========================================================
#
# WHAT:
# Read game_manifest.csv, confirm its required keys exist, apply the optional
# season filter, then keep one sorted row per game.
#
# WHY:
# The manifest defines exactly which games the downloader should process.
# Validating it here prevents confusing failures deeper inside the download loop.
#
# PYTHON NOTES:
# - dtype={"season_id": str}: preserves the season ID as text.
# - set subtraction: identifies required columns that are missing.
# - boolean indexing: filters rows using a True/False condition.
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

required_columns = {"season_id", "game_id"}
missing_columns = required_columns - set(manifest.columns)

if missing_columns:
    raise ValueError(
        "Game manifest is missing required columns: "
        + ", ".join(sorted(missing_columns))
    )

if args.season_id:
    manifest = manifest.loc[
        manifest["season_id"] == args.season_id
    ].copy()

if manifest.empty:
    raise RuntimeError("No games matched the requested manifest/season filter.")

manifest = (
    manifest
    .drop_duplicates(subset=["season_id", "game_id"])
    .sort_values(["season_id", "game_id"])
    .reset_index(drop=True)
)

print("\nSection 4: Game manifest loaded and validated.")

# %%
# =========================================================
# 5. Create one reusable HTTP session with automatic retries
# =========================================================
#
# WHAT:
# Configure a requests Session that retries common temporary web failures.
#
# WHY:
# This script may make thousands of requests. A temporary 429 or 5xx response
# should not automatically cause that game's download to fail.
#
# PYTHON NOTES:
# - Retry(): describes retry counts, status codes, and backoff behavior.
# - HTTPAdapter(max_retries=...): connects the retry policy to requests.
# - Session.mount(): applies the adapter to HTTP/HTTPS requests.
#
# LEARNING NOTE:
# The setup stays in-line because it runs once and is useful to see explicitly.
# =========================================================

retry = Retry(
    total=5,
    connect=5,
    read=5,
    status=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)

adapter = HTTPAdapter(max_retries=retry)

session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)

print("\nSection 5: Retry-enabled HTTP session created.")

# %%
# =========================================================
# 6. Download each endpoint for each game
# =========================================================
#
# WHAT:
# Loop through manifest games, then through the three endpoint definitions,
# downloading any file that does not already exist (unless --overwrite is set).
#
# WHY:
# This is the core ingestion step. Preserving the raw response gives us a stable
# local source layer for all later table-building scripts.
#
# PYTHON NOTES:
# - DataFrame.itertuples(): iterates efficiently over DataFrame rows.
# - enumerate(..., start=1): adds a human-friendly progress counter.
# - dict.items(): loops through endpoint-name/template pairs.
# - Path.exists(): supports restartable downloads by skipping existing files.
# - endpoint_template.format(): inserts the game ID into the endpoint path.
# - json.dump(): serializes Python objects back to JSON text.
# - Path.replace(): atomically moves the completed temporary file into place.
#
# FILE-SAFETY NOTE:
# Each response is written to *.tmp first and only renamed to *.json after the
# write completes. This reduces the chance of keeping a partial/corrupt file.
# =========================================================

failures = []
new_files = 0
skipped_files = 0

print(f"Games to process: {len(manifest)}")
print(f"Endpoints per game: {len(ENDPOINTS)}")

for game_number, row in enumerate(
    manifest.itertuples(index=False),
    start=1,
):
    season_id = str(row.season_id)
    game_id = int(row.game_id)

    print(
        f"[{game_number}/{len(manifest)}] "
        f"season={season_id} game_id={game_id}"
    )

    for endpoint_name, endpoint_template in ENDPOINTS.items():
        output_file = (
            RAW_DIR
            / season_id
            / endpoint_name
            / f"{game_id}.json"
        )

        if output_file.exists() and not args.overwrite:
            skipped_files += 1
            continue

        url = f"{BASE_URL}/{endpoint_template.format(game_id=game_id)}"

        try:
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

            with temp_file.open("w", encoding="utf-8") as file:
                json.dump(
                    data,
                    file,
                    ensure_ascii=False,
                    indent=2,
                )

            temp_file.replace(output_file)
            new_files += 1

        except (requests.RequestException, ValueError, OSError) as error:
            print(f"  ERROR {endpoint_name}: {error}")

            failure_record = {
                "season_id": season_id,
                "game_id": game_id,
                "endpoint": endpoint_name,
                "url": url,
                "error": str(error),
            }

            failures.append(failure_record)

        time.sleep(REQUEST_PAUSE_SECONDS)

print("\nSection 6: Raw game endpoints downloaded.")

# %%
# =========================================================
# 7. Save failure information
# =========================================================
#
# WHAT:
# Write failed requests to a CSV, or remove an old failure log when this run
# finishes without failures.
#
# WHY:
# A durable failure list makes a large download auditable and easy to rerun.
#
# PYTHON NOTES:
# - pd.DataFrame(failures): turns the list of failure dictionaries into a table.
# - Path.unlink(): deletes an existing file.
# =========================================================

LOG_DIR.mkdir(parents=True, exist_ok=True)

if failures:
    failures_df = pd.DataFrame(failures)
    failures_df.to_csv(FAILURE_LOG, index=False)
else:
    failures_df = pd.DataFrame()

    if FAILURE_LOG.exists():
        FAILURE_LOG.unlink()

print("\nSection 7: Failure log updated.")

# %%
# =========================================================
# 8. Print download QA / run summary
# =========================================================
#
# WHAT:
# Report counts of new, skipped, and failed files plus the output locations.
#
# WHY:
# These totals give a quick check that the downloader behaved as expected and
# make failures visible before moving on to table-building scripts.
#
# PYTHON NOTES:
# - len(failures): counts failed endpoint requests.
# - f-strings with :, formatting: produce readable progress and count output.
# =========================================================

print("\nDownload complete.")
print(f"New files downloaded: {new_files}")
print(f"Existing files skipped: {skipped_files}")
print(f"Failed downloads: {len(failures)}")
print(f"Raw data directory: {RAW_DIR}")

if failures:
    print(f"Failure log: {FAILURE_LOG}")
    print(
        "Rerun this script after resolving any failures; "
        "existing files will be skipped."
    )

print("\nSection 8: Download QA summary complete.")
