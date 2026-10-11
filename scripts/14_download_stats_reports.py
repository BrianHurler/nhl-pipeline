# %% [markdown]
# 14_download_stats_reports.py
#
# PURPOSE
# -------
# Download the NHL Stats REST report families used by the warehouse.
#
# For every season/game-type represented in the manifest, this script downloads
# configured skater, goalie, and team reports and stores the raw JSON unchanged.

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
# We need to build many report jobs, call the Stats REST API, and cache each
# response safely.
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
# 2. Configure paths, API settings, and report families
# =========================================================
#
# WHAT:
# Define the Stats REST base URL and the exact report names to download.
#
# WHY:
# Keeping this report catalog visible makes it clear what enters the warehouse.
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_FILE = PROJECT_ROOT / "data" / "processed" / "game_manifest.csv"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LOG_DIR = PROJECT_ROOT / "data" / "logs"
FAILURE_LOG = LOG_DIR / "stats_report_download_failures.csv"

STATS_BASE = "https://api.nhle.com/stats/rest/en"
REQUEST_TIMEOUT = 30
REQUEST_PAUSE_SECONDS = 0.05

REPORTS = {
    "skater": [
        "summary",
        "bios",
        "faceoffpercentages",
        "faceoffwins",
        "goalsForAgainst",
        "realtime",
        "penalties",
        "penaltykill",
        "penaltyShots",
        "powerplay",
        "puckPossessions",
        "summaryshooting",
        "percentages",
        "scoringRates",
        "scoringpergame",
        "shootout",
        "shottype",
        "timeonice",
    ],
    "goalie": [
        "summary",
        "advanced",
        "bios",
        "daysrest",
        "penaltyShots",
        "savesByStrength",
        "shootout",
        "startedVsRelieved",
    ],
    "team": [
        "summary",
        "faceoffpercentages",
        "daysbetweengames",
        "faceoffwins",
        "goalsagainstbystrength",
        "goalsbyperiod",
        "goalsforbystrength",
        "leadingtrailing",
        "realtime",
        "outshootoutshotby",
        "penalties",
        "penaltykill",
        "penaltykilltime",
        "powerplay",
        "powerplaytime",
        "summaryshooting",
        "percentages",
        "scoretrailfirst",
        "shootout",
        "shottype",
        "goalgames",
    ],
}

print("\nSection 2: Stats REST report catalog and paths configured.")


# %%
# =========================================================
# 3. Parse optional command-line arguments
# =========================================================
#
# WHAT:
# Allow one season filter and an --overwrite flag.
#
# WHY:
# Report downloads are numerous, so restartable and targeted execution is useful.
# =========================================================

parser = argparse.ArgumentParser(
    description="Download NHL Stats REST report tables."
)

parser.add_argument(
    "--season-id",
    default=None,
    help="Optional NHL season ID such as 20252026.",
)

parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Redownload existing report files.",
)

if "ipykernel" in sys.modules:
    args, jupyter_extra_args = parser.parse_known_args()
else:
    args = parser.parse_args()
    jupyter_extra_args = []

print("\nSection 3: Command-line arguments parsed.")


# %%
# =========================================================
# 4. Load the manifest and determine season/game-type jobs
# =========================================================
#
# WHAT:
# Read the manifest, optionally filter one season, and keep unique
# season_id + game_type combinations.
#
# WHY:
# Stats REST reports are requested at the season/game-type level.
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
        manifest["season_id"] == str(args.season_id)
    ].copy()

if manifest.empty:
    raise RuntimeError(
        "No manifest rows matched the requested season."
    )

season_types = (
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

print(
    "Season/game-type combinations: "
    f"{len(season_types):,}"
)
print("\nSection 4: Stats REST season/game-type scope created.")


# %%
# =========================================================
# 5. Create a retry-enabled HTTP session
# =========================================================
#
# WHAT:
# Configure automatic retries for temporary API failures.
#
# WHY:
# The full report run contains many requests and should tolerate transient errors.
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

session = requests.Session()

session.mount(
    "https://",
    HTTPAdapter(
        max_retries=retry
    ),
)

print("\nSection 5: Retry-enabled Stats REST session created.")


# %%
# =========================================================
# 6. Build the full report-job list
# =========================================================
#
# WHAT:
# Cross every season/game-type pair with every configured entity/report pair.
#
# WHY:
# An explicit jobs list is easy to inspect before any download starts.
#
# PYTHON NOTES:
# - nested loops: construct every valid combination.
# - tuple: stores one immutable job definition.
# =========================================================

jobs = []

for row in season_types.itertuples(
    index=False
):
    season_id = str(row.season_id)
    game_type = int(row.game_type)

    for entity, reports in REPORTS.items():
        for report in reports:
            jobs.append(
                (
                    season_id,
                    game_type,
                    entity,
                    report,
                )
            )

print(f"Stats REST jobs: {len(jobs):,}")
print("\nSection 6: Stats REST report-job list created.")


# %%
# =========================================================
# 7. Download and cache every Stats REST report
# =========================================================
#
# WHAT:
# Loop through jobs, request each report, and atomically save its raw JSON.
#
# WHY:
# Preserving raw responses keeps the processed-table build reproducible and
# independent from future API availability.
#
# PYTHON NOTES:
# - params: sends Stats REST query parameters cleanly.
# - cayenneExp: filters the API to one season and game type.
# - temporary file + replace(): avoids leaving partial JSON files.
# =========================================================

downloaded = 0
skipped = 0
failed = 0
failures = []

for job_number, (
    season_id,
    game_type,
    entity,
    report,
) in enumerate(
    jobs,
    start=1,
):
    output_file = (
        RAW_DIR
        / season_id
        / "stats_reports"
        / entity
        / str(game_type)
        / f"{report}.json"
    )

    print(
        f"[{job_number}/{len(jobs)}] "
        f"{entity}/{report} "
        f"season={season_id} "
        f"type={game_type}"
    )

    if output_file.exists() and not args.overwrite:
        skipped += 1
        continue

    params = {
        "isAggregate": "false",
        "isGame": "false",
        "start": 0,
        "limit": -1,
        "cayenneExp": (
            f"seasonId={season_id} "
            f"and gameTypeId={game_type}"
        ),
    }

    url = (
        f"{STATS_BASE}/"
        f"{entity}/{report}"
    )

    try:
        response = session.get(
            url,
            params=params,
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

        downloaded += 1

        time.sleep(
            REQUEST_PAUSE_SECONDS
        )

    except (
        requests.RequestException,
        ValueError,
        OSError,
    ) as error:
        failed += 1

        failures.append(
            {
                "season_id": season_id,
                "game_type": game_type,
                "entity": entity,
                "report": report,
                "url": url,
                "error": str(error),
            }
        )

        print(
            f"  WARNING: {error}"
        )

print("\nSection 7: Stats REST report download loop complete.")


# %%
# =========================================================
# 8. Save failures and print download QA summary
# =========================================================
#
# WHAT:
# Write failed report jobs to CSV and summarize the run.
#
# WHY:
# The failure log makes it clear which report files may need another attempt.
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

print("\nStats report download complete.")
print(f"New files downloaded: {downloaded:,}")
print(f"Existing files skipped: {skipped:,}")
print(f"Failed downloads: {failed:,}")

print("\nSection 8: Stats REST download QA summary complete.")
