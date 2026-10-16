"""Build games, plays, and shifts Parquets for one NHL season from the S3 raw archive.

Examples:
  python scripts/02_build_season_parquets.py --season-id 20262027
  python scripts/02_build_season_parquets.py --active-season
  python scripts/02_build_season_parquets.py --active-season --publish

Requires NHL_S3_BUCKET; optional NHL_S3_PREFIX. Default is local-only.
"""
import argparse
import json
import os
from pathlib import Path
from datetime import date

import boto3
import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "aws-parquet-data"

parser = argparse.ArgumentParser(description="Build NHL season games, plays and shifts Parquets")
choice = parser.add_mutually_exclusive_group(required=True)
choice.add_argument("--season-id", help="Eight-digit season ID, e.g. 20262027")
choice.add_argument("--active-season", action="store_true", help="Choose NHL season automatically; rollover October 1 UTC")
parser.add_argument("--publish", action="store_true", help="Upload validated Parquets to S3")
args = parser.parse_args()

today = date.today()
start_year = today.year if today.month >= 10 else today.year - 1
season = args.season_id or f"{start_year}{start_year + 1}"
print(f"Building NHL season {season}; UTC date {today}", flush=True)
if len(season) != 8 or not season.isdigit() or int(season[4:]) != int(season[:4]) + 1:
    parser.error("--season-id must represent consecutive years as eight digits")

bucket = os.environ["NHL_S3_BUCKET"]
prefix = os.getenv("NHL_S3_PREFIX", "").strip("/")
s3 = boto3.client("s3")


def object_key(*parts):
    return "/".join(str(p).strip("/") for p in (prefix, *parts) if p)


def list_keys(folder):
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=folder.rstrip("/") + "/"
    ):
        keys.extend(
            item["Key"] for item in page.get("Contents", [])
            if item["Key"].endswith(".json")
        )
    return sorted(keys)


def read_json(key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


# Read and validate all pages of the league-wide season catalog.
catalog_root = object_key("raw-api-data", "game_catalog", season)
catalog_keys = list_keys(catalog_root)
pages = []
for key in catalog_keys:
    name = key.rsplit("/", 1)[-1]
    if name.startswith("page_") and name.endswith(".json"):
        number = name[len("page_"):-len(".json")]
        if number.isdigit():
            pages.append((int(number), key))
pages.sort()

if not pages:
    raise RuntimeError(f"No game catalogs under s3://{bucket}/{catalog_root}/")

records = []
expected_total = None
expected_offset = 0
for offset, key in pages:
    page = read_json(key)
    rows = page["data"]
    total = int(page["total"])
    if expected_total is None:
        expected_total = total
    if offset != expected_offset or total != expected_total or not isinstance(rows, list):
        raise RuntimeError(f"Incomplete/inconsistent catalog page: {key}")
    records.extend(rows)
    expected_offset += len(rows)

ids = [int(row["id"]) for row in records]
if len(records) != expected_total or len(ids) != len(set(ids)):
    raise RuntimeError("Game catalog missing rows or contains duplicate game IDs")
if not all(str(row["season"]) == season for row in records):
    raise RuntimeError("Catalog contains games from another season")

# Preserve preseason in the raw archive, but exclude it from every analytical table.
# NHL game_type 1 is preseason; retain other types (including regular season/playoffs).
preseason_games = sum(int(row["gameType"]) == 1 for row in records)
records = [row for row in records if int(row["gameType"]) != 1]
if not records:
    raise RuntimeError("No non-preseason games found in the season catalog")
print(f"Excluded {preseason_games:,} preseason games from Parquet outputs")

# One game per row, including future games listed by the NHL catalog.
games = pd.DataFrame([{
    "game_id": int(game["id"]),
    "game_date": str(game["gameDate"])[:10],
    "game_type": int(game["gameType"]),
    "game_state_id": int(game["gameStateId"]),
    "home_team_id": int(game["homeTeamId"]),
    "away_team_id": int(game["visitingTeamId"]),
} for game in records])

# One play per row; only completed games are expected to have a play-by-play feed.
completed = {
    int(game["id"]): game for game in records
    if int(game["gameStateId"]) in (6, 7)
}
play_root = object_key("raw-api-data", season, "play_by_play")
play_keys = list_keys(play_root)
keys_by_game = {
    int(Path(key).stem): key for key in play_keys if Path(key).stem.isdigit()
}
missing = sorted(set(completed) - set(keys_by_game))
if missing:
    raise RuntimeError(
        f"Missing play-by-play JSON for {len(missing)} completed games; "
        f"first IDs: {missing[:10]}"
    )

frames = []
for game_id in sorted(completed):
    doc = read_json(keys_by_game[game_id])
    events = doc.get("plays")
    if not isinstance(events, list) or not events:
        raise RuntimeError(f"Missing/invalid play events for completed game {game_id}")
    frame = pd.json_normalize(events, sep="_")
    frame.insert(0, "game_id", game_id)
    frame.insert(1, "game_date", str(completed[game_id]["gameDate"])[:10])
    frame.insert(2, "game_type", int(completed[game_id]["gameType"]))
    frames.append(frame)

if not frames:
    raise RuntimeError("No completed game plays to write")
plays = pd.concat(frames, ignore_index=True, sort=False)
if "eventId" not in plays:
    raise RuntimeError("Missing eventId from play-by-play API")
if plays["eventId"].isna().any() or plays.duplicated(["game_id", "eventId"]).any():
    raise RuntimeError("Null or duplicate (game_id, eventId) keys")
if plays["game_id"].nunique() != len(completed):
    raise RuntimeError("Play-by-play game coverage differs from completed game catalog")
if games["game_type"].eq(1).any() or plays["game_type"].eq(1).any():
    raise RuntimeError("Preseason games detected in analytical output")

# Shift charts use the NHL Stats REST "data" list. Require coverage for each
# completed non-preseason game rather than silently exporting missing games.
shift_root = object_key("raw-api-data", season, "shifts")
shift_keys = list_keys(shift_root)
shift_by_game = {
    int(Path(key).stem): key for key in shift_keys if Path(key).stem.isdigit()
}
missing_shifts = sorted(set(completed) - set(shift_by_game))
if missing_shifts:
    raise RuntimeError(
        f"Missing shift JSON for {len(missing_shifts)} completed games; "
        f"first IDs: {missing_shifts[:10]}"
    )
shift_frames = []
for game_id in sorted(completed):
    doc = read_json(shift_by_game[game_id])
    rows = doc.get("data")
    if not isinstance(rows, list) or not rows:
        raise RuntimeError(f"Missing/invalid shift records for completed game {game_id}")
    frame = pd.json_normalize(rows, sep="_")
    if "gameId" in frame:
        if not frame["gameId"].astype(str).eq(str(game_id)).all():
            raise RuntimeError(f"Shift game ID mismatch for {game_id}")
        frame = frame.drop(columns=["gameId"])
    frame.insert(0, "game_id", game_id)
    frame.insert(1, "game_date", str(completed[game_id]["gameDate"])[:10])
    frame.insert(2, "game_type", int(completed[game_id]["gameType"]))
    shift_frames.append(frame)

shifts = pd.concat(shift_frames, ignore_index=True, sort=False)
if "id" not in shifts or shifts["id"].isna().any():
    raise RuntimeError("Shift IDs are missing or null")
if shifts.duplicated(["game_id", "id"]).any():
    raise RuntimeError("Duplicate shift IDs within a game")
if shifts["game_id"].nunique() != len(completed) or shifts["game_type"].eq(1).any():
    raise RuntimeError("Shift coverage mismatch or preseason shifts detected")

# Season lives in the Hive directory; avoid conflicting duplicate season columns.
label = f"{season[:4]}-{season[4:]}"
games_file = OUTPUT / "games" / f"season={season}" / f"games_{label}.parquet"
plays_file = OUTPUT / "plays" / f"season={season}" / f"plays_{label}.parquet"
shifts_file = OUTPUT / "shifts" / f"season={season}" / f"shifts_{label}.parquet"
for path in (games_file, plays_file, shifts_file):
    path.parent.mkdir(parents=True, exist_ok=True)

games["game_date"] = pd.to_datetime(games["game_date"]).dt.date
plays["game_date"] = pd.to_datetime(plays["game_date"]).dt.date
shifts["game_date"] = pd.to_datetime(shifts["game_date"]).dt.date
games.to_parquet(games_file, index=False, compression="zstd")
plays.to_parquet(plays_file, index=False, compression="zstd")
shifts.to_parquet(shifts_file, index=False, compression="zstd")

# Validate all Parquet files before optional publication.
con = duckdb.connect()
for name, path, expected in (
    ("games", games_file, len(games)),
    ("plays", plays_file, len(plays)),
    ("shifts", shifts_file, len(shifts)),
):
    actual = con.execute(
        "SELECT count(*) FROM read_parquet(?)", [str(path)]
    ).fetchone()[0]
    if actual != expected:
        raise RuntimeError(f"{name} Parquet validation failed: {actual} != {expected}")
    print(f"{name}: {actual:,} rows | {path.stat().st_size / 1024**2:.2f} MiB | {path}")

if args.publish:
    for path in (games_file, plays_file, shifts_file):
        key = object_key("nhl-parquet", *path.relative_to(OUTPUT).parts)
        s3.upload_file(str(path), bucket, key)
        print(f"Published: s3://{bucket}/{key}")
else:
    print("Local-only run. Add --publish to upload validated output.")
