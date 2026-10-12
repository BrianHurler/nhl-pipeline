# Download NHL API responses without transforming them into analytical tables.
# Re-running this script skips saved JSON, except for recently changed records.
import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, local
from datetime import date, timedelta
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Locate the repository on macOS or Linux, independently of the terminal directory.
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "raw-api-data"
WEB = "https://api-web.nhle.com/v1"
STATS = "https://api.nhle.com/stats/rest/en"

# Select a historical season, limit a test run, or request a deliberate refresh.
parser = argparse.ArgumentParser(description="Download missing NHL API JSON records.")
parser.add_argument("--season-id", help="Optional eight-digit NHL season ID")
parser.add_argument("--start-year", type=int, default=2016,
                    help="First season start year for multi-season runs (default: 2016)")
parser.add_argument("--refresh-days", type=int, default=7)
parser.add_argument("--overwrite", action="store_true")
parser.add_argument("--max-downloads", type=int, default=0)
parser.add_argument("--max-games", type=int, default=0,
                    help="Process only the first N games for a short benchmark")
parser.add_argument("--workers", type=int, default=4,
                    help="Concurrent game workers (default: 4)")
parser.add_argument("--games-only", action="store_true",
                    help="Download gamecenter and shift-chart files; skip supplementary datasets")
args = parser.parse_args()

# Retry transient connection/server failures rather than saving incomplete responses.
retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=retry))
today = date.today()
recent_cutoff = today - timedelta(days=args.refresh_days)
current_start_year = today.year if today.month >= 8 else today.year - 1

# Require an S3 destination so downloaded raw records are durably archived.
# Boto3 uses your local AWS profile or EC2 instance role for authentication.
bucket = os.environ["NHL_S3_BUCKET"]
prefix = os.getenv("NHL_S3_PREFIX", "").strip("/")

import boto3
from botocore.exceptions import ClientError
s3 = boto3.client("s3")

# List existing S3 objects once instead of checking each file individually.
archive_prefix = "/".join(filter(None, [prefix, "raw-api-data"])) + "/"
objects = set()
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=archive_prefix):
    objects.update(item["Key"] for item in page.get("Contents", []))
print(f"S3 inventory: {len(objects):,} archived objects.", flush=True)

# Each worker has its own HTTP session; shared counters use a lock.
lock = Lock()
worker_local = local()

# A single download function keeps retry, caching, and atomic writes consistent.
# Missing historical endpoints (404) are recorded so they are not retried every night.
counts = {"new": 0, "updated": 0, "cached": 0, "unavailable": 0, "failed": 0}
failures = []
download_limit_reached = False
test_downloads = 0


def get_json(url, path, params=None, refresh=False, count_limit=True, read_data=True):
    global download_limit_reached, test_downloads

    # Use the same S3 object key on both the MacBook and EC2.
    key = "/".join(filter(None, [prefix, "raw-api-data", path.relative_to(RAW).as_posix()]))
    missing_marker = path.with_suffix(".unavailable")

    # Reuse the S3 inventory and avoid restoring game files we do not need to read.
    if not (args.overwrite or refresh):
        if key in objects:
            if read_data and not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                temporary = path.with_suffix(".json.tmp")
                s3.download_file(bucket, key, str(temporary))
                temporary.replace(path)
            with lock:
                counts["cached"] += 1
            return json.loads(path.read_text(encoding="utf-8")) if read_data else None

        # Preserve a locally downloaded file by uploading it if S3 lacks the key.
        if path.exists():
            s3.upload_file(str(path), bucket, key)
            with lock:
                objects.add(key)
                counts["cached"] += 1
            return json.loads(path.read_text(encoding="utf-8")) if read_data else None

        if missing_marker.exists():
            with lock:
                counts["unavailable"] += 1
            return None

    # Bound successful API downloads when testing, without limiting cache restoration.
    if count_limit and args.max_downloads and test_downloads >= args.max_downloads:
        download_limit_reached = True
        return None

    # Fetch the NHL response; mark historical 404s only when not refreshing.
    try:
        # Separate sessions prevent cross-thread sharing of Requests state.
        if not hasattr(worker_local, "session"):
            worker_local.session = requests.Session()
            worker_local.session.mount("https://", HTTPAdapter(max_retries=retry))
        response = worker_local.session.get(url, params=params, timeout=30)
        if response.status_code == 404:
            if not (args.overwrite or refresh):
                missing_marker.parent.mkdir(parents=True, exist_ok=True)
                missing_marker.write_text(url + "\n", encoding="utf-8")
            with lock:
                counts["unavailable"] += 1
            return None
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as error:
        with lock:
            counts["failed"] += 1
            failures.append(f"{path}: {error}")
        return None

    # Write the source response safely, then publish it to the shared S3 archive.
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(data), encoding="utf-8")
    temporary.replace(path)

    # Never mark an upload failure as successful; the next run can retry it.
    try:
        s3.upload_file(str(path), bucket, key)
    except Exception as error:
        with lock:
            counts["failed"] += 1
            failures.append(f"S3 {key}: {error}")
        return data

    if missing_marker.exists():
        missing_marker.unlink()
    with lock:
        objects.add(key)
        counts["updated" if refresh or args.overwrite else "new"] += 1
        if count_limit:
            test_downloads += 1
    time.sleep(0.05)
    return data

# Retrieve official season, team and reference catalogs before scheduling jobs.
# Cached references are refreshed daily, because they discover new seasons and teams.
references = {
    "standings_seasons": f"{WEB}/standings-season",
    "teams": f"{STATS}/team",
    "franchises": f"{STATS}/franchise",
    "seasons": f"{STATS}/season",
    "component_seasons": f"{STATS}/componentSeason",
    "countries": f"{STATS}/country",
    "drafts": f"{STATS}/draft",
    "glossary": f"{STATS}/glossary",
    "stats_config": f"{STATS}/config",
    "game_meta": f"{STATS}/game/meta",
}
reference_data = {}
for name, url in references.items():
    reference_data[name] = get_json(url, RAW / "reference" / f"{name}.json", refresh=True, count_limit=False)

# Build an all-history season list from the NHL season catalog, not a hardcoded year.
# The season filter is useful when backfilling or testing one season.
seasons_data = reference_data.get("seasons") or {}
seasons = sorted({
    str(row["id"]) for row in seasons_data.get("data", [])
    if str(row.get("id", "")).isdigit() and len(str(row["id"])) == 8
})
if args.season_id:
    seasons = [args.season_id]
else:
    seasons = [season for season in seasons if int(season[:4]) >= args.start_year]
if not seasons:
    raise RuntimeError("No NHL seasons discovered. Check the season reference endpoint.")

# Use the league-wide Stats REST game catalog, independent of team expansion.
# Keep the original paginated responses in S3 alongside other raw NHL JSON.
# Team IDs are mapped to abbreviations only for downstream roster/stat requests.
teams_data = reference_data.get("teams") or {}
team_by_id = {
    int(row["id"]): str(row.get("triCode") or row.get("abbrev") or "").upper()
    for row in teams_data.get("data", [])
    if row.get("id") is not None
}

games = {}
page_size = 500
for season in seasons:
    start = 0
    season_games = 0
    while True:
        params = {"cayenneExp": f"season={season}", "start": start, "limit": page_size}
        path = RAW / "game_catalog" / season / f"page_{start}.json"
        catalog = get_json(f"{STATS}/game", path, params=params,
                           refresh=season.startswith(str(current_start_year)),
                           count_limit=False)
        if not isinstance(catalog, dict) or not isinstance(catalog.get("data"), list):
            raise RuntimeError(f"Missing or unexpected NHL game catalog for {season}, page {start}")
        rows = catalog["data"]
        if not rows and start == 0:
            raise RuntimeError(f"NHL game catalog returned no games for {season}")
        for row in rows:
            required = ("id", "season", "gameDate", "gameType", "gameStateId",
                        "homeTeamId", "visitingTeamId")
            missing = [field for field in required if row.get(field) is None]
            if missing:
                raise RuntimeError(f"Game catalog schema changed: missing {missing} in {row}")
            if str(row["season"]) != season:
                raise RuntimeError(f"Unexpected season in catalog: {row['season']} (requested {season})")
            game_id = int(row["id"])
            games[game_id] = {
                "id": game_id,
                "season": season,
                "gameDate": str(row["gameDate"])[:10],
                "gameType": int(row["gameType"]),
                "gameStateId": int(row["gameStateId"]),
                "homeTeam": {"abbrev": team_by_id.get(int(row["homeTeamId"]))},
                "awayTeam": {"abbrev": team_by_id.get(int(row["visitingTeamId"]))},
            }
        season_games += len(rows)
        start += len(rows)
        total = catalog.get("total")
        if total is not None:
            if start >= int(total):
                break
        elif len(rows) < page_size:
            break
        if not rows:
            raise RuntimeError(f"Catalog pagination ended early for {season}: {start}/{total}")
    print(f"{season}: {season_games:,} league-wide game records discovered.")

print(f"Discovered {len(games):,} unique games in {len(seasons)} requested seasons.")
print("Game download test limit:", args.max_downloads or "unlimited")

# Game state IDs 6 and 7 represent completed games in the Stats REST feed.
# Retain all states in the archived catalog and download gamecenter files only when finished.
game_states = {}
for game in games.values():
    state = game["gameStateId"]
    game_states[state] = game_states.get(state, 0) + 1
print("Discovered game state IDs:", dict(sorted(game_states.items())))
# Fetch the three gamecenter feeds and shift charts for completed games only.
# Recent completed games are refreshed to capture corrections.
game_endpoints = {
    "play_by_play": "play-by-play",
    "boxscore": "boxscore",
    "landing": "landing",
}
completed_games = {
    game_id: game for game_id, game in games.items()
    if game["gameStateId"] in (6, 7)
    and date.fromisoformat(game["gameDate"]) <= today
}
print(f"Completed games eligible for download: {len(completed_games):,}")
# Workers process separate games; a small download-limit test stays sequential.
def download_game(item):
    game_id, game = item
    game_date = date.fromisoformat(game["gameDate"])
    refresh = game_date >= recent_cutoff
    season = game["season"]

    for name, endpoint in game_endpoints.items():
        if download_limit_reached:
            break
        url = f"{WEB}/gamecenter/{game_id}/{endpoint}"
        get_json(url, RAW / season / name / f"{game_id}.json",
                 refresh=refresh, read_data=False)

    if not download_limit_reached:
        params = {"cayenneExp": f"gameId={game_id}", "limit": -1}
        get_json(f"{STATS}/shiftcharts", RAW / season / "shifts" / f"{game_id}.json",
                 params=params, refresh=refresh, read_data=False)


# Process games in parallel and show progress every 25 completed game workers.
game_items = sorted(completed_games.items())
if args.max_games:
    game_items = game_items[:args.max_games]
workers = 1 if args.max_downloads else max(1, args.workers)
print(f"Processing {len(game_items):,} games with {workers} worker(s).", flush=True)
with ThreadPoolExecutor(max_workers=workers) as executor:
    futures = [executor.submit(download_game, item) for item in game_items]
    for game_number, future in enumerate(as_completed(futures), start=1):
        future.result()
        if game_number % 25 == 0 or game_number == len(game_items):
            print(f"Game progress: {game_number:,}/{len(game_items):,} "
                  f"| new={counts['new']} cached={counts['cached']} "
                  f"unavailable={counts['unavailable']} failed={counts['failed']}",
                  flush=True)

# Run the game archive by itself before collecting supplementary datasets.
# This makes a full-season pilot easier to validate and safely resume.
if args.games_only:
    print(f"Download summary: {counts}")
    print(f"Request limit reached: {download_limit_reached}")
    if failures:
        print("\n".join(failures[:25]))
        raise RuntimeError(f"{len(failures)} downloads/uploads failed; rerun to retry.")
    raise SystemExit(0)

# Discover season-team combinations and download rosters and club statistics.
# Collect player IDs for player landing pages and season game logs.
players = set()
team_seasons = set()
season_types = set()
game_dates = set()
for game_id, game in games.items():
    season = game["season"]
    if game_id in completed_games:
        season_types.add((season, int(game.get("gameType", 2))))
        game_dates.add(game["gameDate"])
    for side in ("homeTeam", "awayTeam"):
        abbrev = (game.get(side) or {}).get("abbrev")
        if abbrev:
            team_seasons.add((season, abbrev))

# Download rosters for teams that actually participated in each season.
# The roster responses provide player identifiers for other API requests.
roster_jobs = sorted(team_seasons)
print(f"Stage: rosters ({len(roster_jobs):,} team-seasons)", flush=True)
for index, (season, team) in enumerate(roster_jobs, start=1):
    if download_limit_reached:
        break
    url = f"{WEB}/roster/{team}/{season}"
    roster = get_json(url, RAW / season / "rosters" / f"{team}.json") or {}
    for group in ("forwards", "defensemen", "goalies"):
        players.update(int(row["id"]) for row in roster.get(group, []) if row.get("id"))
    if index % 25 == 0 or index == len(roster_jobs):
        print(f"Rosters: {index:,}/{len(roster_jobs):,} | {counts}", flush=True)

# Save player profiles once per player, independent of their seasons and teams.
# Profiles are reference records; recent updates can be requested with --overwrite.
player_jobs = sorted(players)
print(f"Stage: player profiles ({len(player_jobs):,} players)", flush=True)
for index, player_id in enumerate(player_jobs, start=1):
    if download_limit_reached:
        break
    get_json(f"{WEB}/player/{player_id}/landing", RAW / "players" / f"{player_id}.json")
    if index % 100 == 0 or index == len(player_jobs):
        print(f"Player profiles: {index:,}/{len(player_jobs):,} | {counts}", flush=True)

# Retrieve date-specific standings and per-team season aggregate statistics.
# Each source remains raw JSON and is not combined with other endpoints here.
standings_jobs = sorted(game_dates)
print(f"Stage: standings ({len(standings_jobs):,} dates)", flush=True)
for index, game_date in enumerate(standings_jobs, start=1):
    if download_limit_reached:
        break
    get_json(f"{WEB}/standings/{game_date}", RAW / "standings" / f"{game_date}.json",
             refresh=date.fromisoformat(game_date) >= recent_cutoff)
    if index % 25 == 0 or index == len(standings_jobs):
        print(f"Standings: {index:,}/{len(standings_jobs):,} | {counts}", flush=True)

club_jobs = sorted(team_seasons)
print(f"Stage: club statistics ({len(club_jobs):,} team-seasons)", flush=True)
for index, (season, team) in enumerate(club_jobs, start=1):
    if download_limit_reached:
        break
    types = {kind for sid, kind in season_types if sid == season}
    for kind in sorted(types):
        url = f"{WEB}/club-stats/{team}/{season}/{kind}"
        get_json(url, RAW / season / "club_stats" / str(kind) / f"{team}.json",
                 refresh=season.startswith(str(current_start_year)))
    if index % 25 == 0 or index == len(club_jobs):
        print(f"Club statistics: {index:,}/{len(club_jobs):,} | {counts}", flush=True)

# Preserve season-specific player game logs for players on each season's rosters.
# This avoids requesting the Cartesian product of every player and NHL season.
log_jobs = sorted(team_seasons)
print(f"Stage: player game logs ({len(log_jobs):,} team-seasons)", flush=True)
for index, (season, team) in enumerate(log_jobs, start=1):
    if download_limit_reached:
        break
    roster_file = RAW / season / "rosters" / f"{team}.json"
    if not roster_file.exists():
        continue
    roster = json.loads(roster_file.read_text(encoding="utf-8"))
    season_players = {
        int(row["id"]) for group in ("forwards", "defensemen", "goalies")
        for row in roster.get(group, []) if row.get("id")
    }
    for kind in sorted({k for sid, k in season_types if sid == season}):
        for player_id in sorted(season_players):
            url = f"{WEB}/player/{player_id}/game-log/{season}/{kind}"
            path = RAW / season / "player_game_logs" / str(kind) / f"{player_id}.json"
            get_json(url, path, refresh=season.startswith(str(current_start_year)))
            if download_limit_reached:
                break
    if index % 5 == 0 or index == len(log_jobs):
        print(f"Player game logs: {index:,}/{len(log_jobs):,} teams | {counts}", flush=True)

# Download playoff brackets and drafts for discovered seasons.
# Missing brackets for seasons without playoffs are expected.
print(f"Stage: playoff brackets and drafts ({len(seasons):,} seasons)", flush=True)
for index, season in enumerate(seasons, start=1):
    if download_limit_reached:
        break
    year = season[-4:]
    get_json(f"{WEB}/playoff-bracket/{year}", RAW / season / "playoffs" / "bracket.json")
    for round_number in range(1, 8):
        url = f"{WEB}/draft/picks/{year}/{round_number}"
        get_json(url, RAW / season / "draft_picks" / f"round_{round_number}.json")
    print(f"Playoffs and drafts: {index:,}/{len(seasons):,} | {counts}", flush=True)

# Download the configured Stats REST reports by season and game type.
# The report families match those used by the existing NHL research warehouse.
reports = {
    "skater": ["summary", "bios", "faceoffpercentages", "faceoffwins", "goalsForAgainst",
               "realtime", "penalties", "penaltykill", "penaltyShots", "powerplay",
               "puckPossessions", "summaryshooting", "percentages", "scoringRates",
               "scoringpergame", "shootout", "shottype", "timeonice"],
    "goalie": ["summary", "advanced", "bios", "daysrest", "penaltyShots",
               "savesByStrength", "shootout", "startedVsRelieved"],
    "team": ["summary", "faceoffpercentages", "daysbetweengames", "faceoffwins",
             "goalsagainstbystrength", "goalsbyperiod", "goalsforbystrength",
             "leadingtrailing", "realtime", "outshootoutshotby", "penalties",
             "penaltykill", "penaltykilltime", "powerplay", "powerplaytime",
             "summaryshooting", "percentages", "scoretrailfirst", "shootout",
             "shottype", "goalgames"],
}
report_jobs = sorted(season_types)
print(f"Stage: statistical reports ({len(report_jobs):,} season/game-type groups)", flush=True)
for index, (season, kind) in enumerate(report_jobs, start=1):
    if download_limit_reached:
        break
    params = {"isAggregate": "false", "isGame": "false", "start": 0,
              "limit": -1, "cayenneExp": f"seasonId={season} and gameTypeId={kind}"}
    for entity, names in reports.items():
        for name in names:
            path = RAW / season / "stats_reports" / str(kind) / entity / f"{name}.json"
            get_json(f"{STATS}/{entity}/{name}", path, params=params,
                     refresh=season.startswith(str(current_start_year)))
            if download_limit_reached:
                break
    print(f"Statistical reports: {index:,}/{len(report_jobs):,} | {counts}", flush=True)

# Report a concise summary and fail visibly if network or upload errors occurred.
print(f"Download summary: {counts}")
print(f"Request limit reached: {download_limit_reached}")
if failures:
    print("\n".join(failures[:25]))
    raise RuntimeError(f"{len(failures)} downloads/uploads failed; rerun to retry.")
