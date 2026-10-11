# %% [markdown]
# 06_download_shift_charts.py
#
# PURPOSE
# -------
# Download NHL shift-chart data for every game in the project manifest.
#
# Shift charts are separate from the gamecenter play-by-play feed. They provide
# player shift start/end records that later let us reconstruct who was on the ice.
#
# UNIT OF OUTPUT
# --------------
# One raw JSON file = one NHL game of shift-chart records.

# %%
# =========================================================
# 1. Import libraries
# =========================================================
#
# WHAT:
# Load command-line, JSON, timing, filesystem, DataFrame, and HTTP tools.
#
# WHY:
# This script reads the manifest, calls the NHL Stats API many times, retries
# temporary failures, and saves each response as raw JSON.
#
# PYTHON NOTES:
# - argparse: optional terminal arguments.
# - json: serializes Python objects to JSON files.
# - sys: lets us distinguish Jupyter from normal terminal execution.
# - requests.Session(): reuses web connections.
# - Retry / HTTPAdapter: add automatic retry behavior.
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
# 2. Configure API, paths, and request behavior
# =========================================================
#
# WHAT:
# Define the shift-chart endpoint, project paths, timeout, and request pause.
#
# WHY:
# Centralizing these values makes the script easy to inspect and adjust.
#
# PYTHON NOTES:
# - Path(__file__).resolve().parents[2]: finds the repository root now that
#   this script lives inside scripts/data_prep/.
# =========================================================

STATS_BASE_URL = "https://api.nhle.com/stats/rest/en"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_FILE = PROJECT_ROOT / "data" / "processed" / "game_manifest.csv"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LOG_DIR = PROJECT_ROOT / "data" / "logs"
FAILURE_LOG = LOG_DIR / "shift_download_failures.csv"

REQUEST_TIMEOUT = 30
REQUEST_PAUSE_SECONDS = 0.10

print("\nSection 2: Shift-download configuration and paths set.")


# %%
# =========================================================
# 3. Parse optional command-line arguments
# =========================================================
#
# WHAT:
# Allow one optional season filter and an --overwrite flag.
#
# WHY:
# We often want to rerun only one season or keep already-downloaded files.
#
# PYTHON NOTES:
# - action="store_true": creates a Boolean flag.
# - parse_known_args(): avoids Jupyter's own kernel arguments causing an
#   argparse SystemExit while we work interactively.
# - parse_args(): remains strict for normal terminal execution.
# =========================================================

parser = argparse.ArgumentParser(
    description="Download NHL shift charts for games in the manifest."
)

parser.add_argument(
    "--season-id",
    help="Optional season ID, e.g. 20252026.",
)

parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Redownload existing files.",
)

if "ipykernel" in sys.modules:
    args, jupyter_extra_args = parser.parse_known_args()
else:
    args = parser.parse_args()
    jupyter_extra_args = []

print("\nSection 3: Command-line arguments parsed.")


# %%
# =========================================================
# 4. Load and prepare the game manifest
# =========================================================
#
# WHAT:
# Read game_manifest.csv, optionally filter one season, and keep one row/game.
#
# WHY:
# The manifest is the authoritative game list for the downloader.
#
# PYTHON NOTES:
# - pd.read_csv(): reads CSV data into a DataFrame.
# - .loc[]: filters DataFrame rows.
# - drop_duplicates(): removes repeated game keys.
# - sort_values(): creates stable processing order.
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

print(f"Games to process: {len(manifest):,}")
print("\nSection 4: Game manifest loaded and prepared.")


# %%
# =========================================================
# 5. Create a retry-enabled HTTP session
# =========================================================
#
# WHAT:
# Configure requests to retry temporary connection/server failures.
#
# WHY:
# A large download should survive occasional 429 or 5xx responses without
# requiring us to restart everything manually.
#
# PYTHON NOTES:
# - Retry(): describes retry counts and exponential backoff.
# - HTTPAdapter(max_retries=...): attaches the retry policy to requests.
# - Session.mount(): applies the adapter to HTTPS requests.
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

print("\nSection 5: Retry-enabled HTTP session created.")


# %%
# =========================================================
# 6. Download and save one shift-chart file per game
# =========================================================
#
# WHAT:
# Loop through every manifest game, request its shift chart, and save the JSON.
#
# WHY:
# Raw preservation lets later processing be rebuilt without re-querying the API.
#
# PYTHON NOTES:
# - enumerate(..., start=1): provides a progress counter.
# - params={...}: lets requests construct the query string safely.
# - json.dump(): writes a Python dictionary/list as JSON.
# - temporary-file -> Path.replace(): reduces risk of keeping a partial file.
#
# API NOTE:
# The shift-chart endpoint uses a cayenneExp query expression such as:
# gameId=2023020204
# =========================================================

failures = []
new_files = 0
skipped_files = 0

for game_number, row in enumerate(
    manifest.itertuples(index=False),
    start=1,
):
    season_id = str(row.season_id)
    game_id = int(row.game_id)

    output_file = (
        RAW_DIR
        / season_id
        / "shifts"
        / f"{game_id}.json"
    )

    print(
        f"[{game_number}/{len(manifest)}] "
        f"season={season_id} game_id={game_id}"
    )

    if output_file.exists() and not args.overwrite:
        skipped_files += 1
        continue

    url = f"{STATS_BASE_URL}/shiftcharts"

    try:
        response = session.get(
            url,
            params={"cayenneExp": f"gameId={game_id}"},
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
        print(f"  ERROR: {error}")

        failure_url = (
            response.url
            if "response" in locals()
            else url
        )

        failures.append(
            {
                "season_id": season_id,
                "game_id": game_id,
                "url": failure_url,
                "error": str(error),
            }
        )

    time.sleep(REQUEST_PAUSE_SECONDS)

print("\nSection 6: Shift-chart download loop complete.")


# %%
# =========================================================
# 7. Save the failure log
# =========================================================
#
# WHAT:
# Save failed downloads to CSV, or remove an old failure file if none failed.
#
# WHY:
# This gives us a durable audit of games that may need another attempt.
#
# PYTHON NOTES:
# - pd.DataFrame(): converts failure dictionaries into a table.
# - Path.unlink(): deletes an existing file.
# =========================================================

LOG_DIR.mkdir(parents=True, exist_ok=True)

if failures:
    failures_df = pd.DataFrame(failures)
    failures_df.to_csv(
        FAILURE_LOG,
        index=False,
    )
else:
    failures_df = pd.DataFrame()

    if FAILURE_LOG.exists():
        FAILURE_LOG.unlink()

print("\nSection 7: Shift-download failure log updated.")


# %%
# =========================================================
# 8. Print download QA summary
# =========================================================
#
# WHAT:
# Report downloaded, skipped, and failed files.
#
# WHY:
# These counts are the quickest sanity check before building the shift table.
#
# PYTHON NOTES:
# - len(): counts list items.
# - f-strings: insert values into readable status messages.
# =========================================================

print("\nShift download complete.")
print(f"New files downloaded: {new_files}")
print(f"Existing files skipped: {skipped_files}")
print(f"Failed downloads: {len(failures)}")
print(f"Raw shift directory: {RAW_DIR}")

if failures:
    print(f"Failure log: {FAILURE_LOG}")

print("\nSection 8: Shift-download QA summary complete.")
