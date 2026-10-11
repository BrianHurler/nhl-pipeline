# Build one season of NHL games from the archived league-wide game catalog.
# The result is a season-partitioned Parquet dataset for downstream analytics.
import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd

# Use repository-relative paths so the script works on macOS and EC2.
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "raw-api-data" / "game_catalog"
OUTPUT = ROOT / "aws-parquet-data" / "games"

# Build one season at a time, including while other seasons download.
parser = argparse.ArgumentParser(description="Build the NHL games Parquet table.")
parser.add_argument("--season-id", required=True, help="Eight-digit season ID, e.g. 20172018")
args = parser.parse_args()

# Load all archived catalog pages for the requested season.
# The NHL returns a 'data' list and a 'total' count on each page.
season_dir = RAW / args.season_id
pages = sorted(season_dir.glob("page_*.json"))
if not pages:
    raise FileNotFoundError(f"No local game catalog pages in {season_dir}")

records = []
expected_total = None
for path in pages:
    catalog = json.loads(path.read_text(encoding="utf-8"))
    if expected_total is None:
        expected_total = int(catalog["total"])
    records.extend(catalog["data"])

# Stop if pagination is incomplete rather than publishing a partial season.
game_ids = [int(game["id"]) for game in records]
if len(records) != expected_total or len(game_ids) != len(set(game_ids)):
    raise ValueError(
        f"Incomplete or duplicate catalog: {len(records)} rows, "
        f"{len(set(game_ids))} unique games, expected {expected_total}"
    )

# Flatten the source fields into one row per game.
# Team IDs are more durable identifiers than team names or abbreviations.
games = pd.DataFrame([
    {
        "game_id": int(game["id"]),
        "season": str(game["season"]),
        "game_date": str(game["gameDate"])[:10],
        "game_type": int(game["gameType"]),
        "game_state_id": int(game["gameStateId"]),
        "home_team_id": int(game["homeTeamId"]),
        "away_team_id": int(game["visitingTeamId"]),
    }
    for game in records
])

# Use DuckDB SQL to enforce types and a consistent output column order.
con = duckdb.connect()
con.register("source_games", games)
result = con.execute("""
    SELECT
        game_id::BIGINT AS game_id,
        season::VARCHAR AS season,
        game_date::DATE AS game_date,
        game_type::INTEGER AS game_type,
        game_state_id::INTEGER AS game_state_id,
        home_team_id::INTEGER AS home_team_id,
        away_team_id::INTEGER AS away_team_id
    FROM source_games
    ORDER BY game_date, game_id
""").df()

# Validate the season before writing its Parquet partition.
if not result["season"].eq(args.season_id).all():
    raise ValueError("Source catalog contains games from a different season")

target = OUTPUT / f"season={args.season_id}" / "games.parquet"
target.parent.mkdir(parents=True, exist_ok=True)
result.to_parquet(target, index=False)

# Show the output path and record count for an easy first-run check.
print(f"Wrote {len(result):,} games to {target}")
print(result.head(5).to_string(index=False))
