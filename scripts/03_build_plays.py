# Convert downloaded NHL gamecenter play-by-play JSON into a Parquet table.
# Keep nested event details as JSON so we can extract additional fields later.
import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd

# Locate raw game files and the season-partitioned Parquet output.
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "raw-api-data"
OUTPUT = ROOT / "aws-parquet-data" / "plays"

# Process one season at a time, including partially downloaded seasons.
parser = argparse.ArgumentParser(description="Build a play-by-play Parquet table.")
parser.add_argument("--season-id", required=True, help="Eight-digit NHL season ID")
args = parser.parse_args()

# Identify the gamecenter play-by-play files available on this computer.
# The downloader might still be fetching other games or archiving them on S3.
folder = RAW / args.season_id / "play_by_play"
paths = sorted(folder.glob("*.json"))
if not paths:
    raise FileNotFoundError(f"No local play-by-play JSON found in {folder}")
print(f"Reading {len(paths):,} local game files.")

# Flatten the basic fields while keeping source-specific event details intact.
# game_id and event_id together identify an event in a game.
records = []
empty_game_ids = []
for path in paths:
    game_id = int(path.stem)
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data.get("plays"), list):
        raise ValueError(f"Missing plays list in {path}")
    if not data["plays"]:
        empty_game_ids.append(game_id)

    for play in data["plays"]:
        period = play.get("periodDescriptor") or {}
        details = play.get("details") or {}
        records.append({
            "game_id": game_id,
            "event_id": play.get("eventId"),
            "sort_order": play.get("sortOrder"),
            "period": period.get("number"),
            "period_type": period.get("periodType"),
            "time_in_period": play.get("timeInPeriod"),
            "time_remaining": play.get("timeRemaining"),
            "event_type": play.get("typeDescKey"),
            "event_owner_team_id": details.get("eventOwnerTeamId"),
            "x_coord": details.get("xCoord"),
            "y_coord": details.get("yCoord"),
            "details_json": json.dumps(details),
        })

# Validate event identifiers before publishing an analytical dataset.
plays = pd.DataFrame(records)
if plays.empty or plays["event_id"].isna().any():
    raise ValueError("No events found, or some events have no event_id")
if plays.duplicated(["game_id", "event_id"]).any():
    raise ValueError("Duplicate game_id + event_id combinations found")

# Use DuckDB SQL to assign consistent types and sort chronologically.
con = duckdb.connect()
con.register("source_plays", plays)
result = con.execute("""
    SELECT
        game_id::BIGINT AS game_id,
        event_id::BIGINT AS event_id,
        sort_order::INTEGER AS sort_order,
        period::INTEGER AS period,
        period_type::VARCHAR AS period_type,
        time_in_period::VARCHAR AS time_in_period,
        time_remaining::VARCHAR AS time_remaining,
        event_type::VARCHAR AS event_type,
        event_owner_team_id::INTEGER AS event_owner_team_id,
        x_coord::DOUBLE AS x_coord,
        y_coord::DOUBLE AS y_coord,
        details_json::VARCHAR AS details_json
    FROM source_plays
    ORDER BY game_id, sort_order
""").df()

# Write a single season partition, replacing an earlier partial build if necessary.
target = OUTPUT / f"season={args.season_id}" / "plays.parquet"
target.parent.mkdir(parents=True, exist_ok=True)
result.to_parquet(target, index=False)

# Summarize the source coverage and resulting events.
print(f"Wrote {len(result):,} plays from {len(paths) - len(empty_game_ids):,} games to {target}")
print(f"Game files checked: {len(paths):,}; empty play feeds: {len(empty_game_ids):,}")
if empty_game_ids:
    print(f"Game IDs with no plays: {sorted(empty_game_ids)}")
print(result["event_type"].value_counts().head(10).to_string())
