# %% [markdown]
# NHL PUBLIC API WAREHOUSE: INTERACTIVE TOUR
#
# PURPOSE
# -------
# This is not a production pipeline. It is a learning/exploration script.
#
# Run one cell at a time in VS Code's Jupyter Interactive Window.
#
# The goal is to understand:
#   * what data the NHL public APIs expose,
#   * the grain of each table,
#   * how the tables connect,
#   * what can be reconstructed from event + shift data,
#   * which tables are official NHL aggregates vs tables we built ourselves.
#
# Rough architecture:
#
#   schedules / gamecenter / stats APIs
#                 |
#                 v
#              raw JSON
#                 |
#                 v
#           processed tables
#                 |
#                 v
#       curated Parquet warehouse
#
# We intentionally keep multiple tables rather than forcing everything into
# one giant flat table. Different hockey questions require different grains.


# %%
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CURATED = PROJECT_ROOT / "data" / "curated"


def load_table(table_name):
    files = sorted((CURATED / table_name).rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found for {table_name}")

    return pd.concat(
        [pd.read_parquet(path) for path in files],
        ignore_index=True,
        sort=False,
    )


def table_summary(table_name):
    df = load_table(table_name)

    print(f"TABLE: {table_name}")
    print(f"ROWS: {len(df):,}")
    print(f"COLUMNS: {len(df.columns):}")
    print()
    print(df.dtypes)

    return df


def show_columns(df, contains=None):
    cols = list(df.columns)

    if contains is not None:
        contains_lower = contains.lower()
        cols = [c for c in cols if contains_lower in c.lower()]

    return cols

print("\nSection 1: Warehouse exploration cell complete.")

# %% [markdown]
# 1. WAREHOUSE INVENTORY
#
# Start by seeing the warehouse as a database rather than as individual files.


# %%
table_names = sorted(
    path.name
    for path in CURATED.iterdir()
    if path.is_dir()
)

len(table_names), table_names

print("\nSection 2: Warehouse exploration cell complete.")

# %%
inventory = []

for table_name in table_names:
    df = load_table(table_name)

    inventory.append({
        "table": table_name,
        "rows": len(df),
        "columns": len(df.columns),
    })

inventory = (
    pd.DataFrame(inventory)
    .sort_values(["rows", "table"], ascending=[False, True])
    .reset_index(drop=True)
)

inventory

print("\nSection 3: Warehouse exploration cell complete.")

# %% [markdown]
# 2. GAMES
#
# GRAIN:
#   one row = one NHL game
#
# This table is our game-level spine. Most fact tables can join to it on
# season_id + game_id.
#
# game_type:
#   1 = preseason
#   2 = regular season
#   3 = playoffs


# %%
games = table_summary("games")

print("\nSection 4: Warehouse exploration cell complete.")

# %%
games.head()

print("\nSection 5: Warehouse exploration cell complete.")

# %%
games["game_type"].value_counts().sort_index()

print("\nSection 6: Warehouse exploration cell complete.")

# %%
games.groupby(["season", "game_type"]).size()

print("\nSection 7: Warehouse exploration cell complete.")

# %% [markdown]
# 3. PLAY-BY-PLAY
#
# GRAIN:
#   one row = one NHL recorded event
#
# Examples:
#   faceoff
#   hit
#   giveaway
#   takeaway
#   blocked-shot
#   missed-shot
#   shot-on-goal
#   goal
#   penalty
#
# This is the main event-sequence table for tactical and predictive work.


# %%
plays = table_summary("plays")

print("\nSection 8: Warehouse exploration cell complete.")

# %%
plays["typeDescKey"].value_counts()

print("\nSection 9: Warehouse exploration cell complete.")

# %%
plays[
    [
        "game_id",
        "periodDescriptor_number",
        "timeInPeriod",
        "typeDescKey",
        "details_eventOwnerTeamId",
        "details_xCoord",
        "details_yCoord",
    ]
].head(30)

print("\nSection 10: Warehouse exploration cell complete.")

# %% [markdown]
# Coordinates are available for many event types. For shot attempts, they are
# the basis for distance, angle, rebound movement, lateral movement, etc.


# %%
shot_attempts = plays[
    plays["typeDescKey"].isin(
        ["shot-on-goal", "goal", "missed-shot", "blocked-shot"]
    )
].copy()

shot_attempts["typeDescKey"].value_counts()

print("\nSection 11: Warehouse exploration cell complete.")

# %%
shot_attempts[
    [
        "typeDescKey",
        "details_xCoord",
        "details_yCoord",
        "details_shotType",
        "details_shootingPlayerId",
        "details_goalieInNetId",
    ]
].head(30)

print("\nSection 12: Warehouse exploration cell complete.")

# %% [markdown]
# situationCode is one of the most useful raw fields in play-by-play.
#
# It encodes the manpower / goalie state at the time of the event. We should
# decode it carefully before production use, but this is where 5v5, PP, PK,
# empty-net context, etc. comes from.


# %%
plays["situationCode"].value_counts().head(40)

print("\nSection 13: Warehouse exploration cell complete.")

# %% [markdown]
# 4. SHIFTS
#
# GRAIN:
#   one row = one NHL shift-chart record
#
# Shift charts tell us when a player entered and left the ice.
#
# This unlocks:
#   * who was on the ice for an event,
#   * line combinations,
#   * defense pairs,
#   * matchup analysis,
#   * teammate/opponent context,
#   * on-ice rates,
#   * RAPM-style models and other player-impact work.


# %%
shifts = table_summary("shifts")

print("\nSection 14: Warehouse exploration cell complete.")

# %%
shifts[
    [
        "id",
        "game_id",
        "playerId",
        "teamAbbrev",
        "period",
        "shiftNumber",
        "startTime",
        "endTime",
        "duration",
    ]
].head(30)

print("\nSection 15: Warehouse exploration cell complete.")

# %%
# NHL's native shift-record id should be unique.
shifts["id"].duplicated().sum()

print("\nSection 16: Warehouse exploration cell complete.")

# %% [markdown]
# 5. BOXSCORE-DERIVED PLAYER-GAME TABLES
#
# These are not individual events.
#
# GRAIN:
#   one player in one game
#
# They are useful for ordinary game summaries and as validation targets for
# metrics we reconstruct from play-by-play + shifts.


# %%
skater_game_stats = table_summary("skater_game_stats")

print("\nSection 17: Warehouse exploration cell complete.")

# %%
skater_game_stats.head()

print("\nSection 18: Warehouse exploration cell complete.")

# %%
show_columns(skater_game_stats)

print("\nSection 19: Warehouse exploration cell complete.")

# %%
goalie_game_stats = table_summary("goalie_game_stats")

print("\nSection 20: Warehouse exploration cell complete.")

# %%
goalie_game_stats.head()

print("\nSection 21: Warehouse exploration cell complete.")

# %% [markdown]
# 6. TEAM-GAME STATS
#
# GRAIN:
#   one team in one game
#
# Therefore a normal game should have exactly two rows.


# %%
team_game_stats = table_summary("team_game_stats")

print("\nSection 22: Warehouse exploration cell complete.")

# %%
team_game_stats.groupby("game_id").size().value_counts()

print("\nSection 23: Warehouse exploration cell complete.")

# %%
team_game_stats.head()

print("\nSection 24: Warehouse exploration cell complete.")

# %% [markdown]
# 7. PLAYERS
#
# GRAIN:
#   one player
#
# This is a stable dimension table. It translates player_id into attributes
# such as name, birth information, position, handedness, height, weight and
# current-team metadata.


# %%
players = table_summary("players")

print("\nSection 25: Warehouse exploration cell complete.")

# %%
players.head(20)

print("\nSection 26: Warehouse exploration cell complete.")

# %% [markdown]
# 8. ROSTERS
#
# PLAYERS answers:
#   "Who is this player?"
#
# ROSTERS answers:
#   "Which team was this player associated with for this season?"
#
# That distinction matters when players change teams or when we expand to
# multiple seasons.


# %%
rosters = table_summary("rosters")

print("\nSection 27: Warehouse exploration cell complete.")

# %%
rosters.head(20)

print("\nSection 28: Warehouse exploration cell complete.")

# %%
rosters.groupby(["season_id", "team"]).size().sort_values(ascending=False).head(20)

print("\nSection 29: Warehouse exploration cell complete.")

# %% [markdown]
# 9. TEAMS AND FRANCHISES
#
# A team record and a franchise record are not exactly the same concept.
# Franchise tables help preserve historical continuity when team names,
# locations or abbreviations change.


# %%
teams = table_summary("teams")

print("\nSection 30: Warehouse exploration cell complete.")

# %%
teams.head(20)

print("\nSection 31: Warehouse exploration cell complete.")

# %%
franchises = table_summary("franchises")

print("\nSection 32: Warehouse exploration cell complete.")

# %%
franchises.head(20)

print("\nSection 33: Warehouse exploration cell complete.")

# %% [markdown]
# 10. DAILY STANDINGS
#
# This table lets us ask historical-state questions such as:
#
#   "What did this team's record look like on the date of this game?"
#
# That is much better for modeling than accidentally using end-of-season
# information for games that occurred months earlier.


# %%
standings_daily = table_summary("standings_daily")

print("\nSection 34: Warehouse exploration cell complete.")

# %%
standings_daily.head()

print("\nSection 35: Warehouse exploration cell complete.")

# %%
show_columns(standings_daily)

print("\nSection 36: Warehouse exploration cell complete.")

# %% [markdown]
# 11. PLAYER GAME LOGS
#
# This is another NHL-provided player-game source.
#
# It overlaps with our boxscore-derived player-game tables but is valuable for
# validation and may expose fields that are not convenient to reconstruct.


# %%
player_game_logs = table_summary("player_game_logs")

print("\nSection 37: Warehouse exploration cell complete.")

# %%
player_game_logs.head()

print("\nSection 38: Warehouse exploration cell complete.")

# %% [markdown]
# 12. CLUB SEASON STATS
#
# NHL club-stat endpoints provide official player season aggregates by club.
#
# These are useful for:
#   * season leaderboards,
#   * feature engineering,
#   * validating our own aggregation from game-level data.


# %%
club_skaters = table_summary("club_skater_season_stats")

print("\nSection 39: Warehouse exploration cell complete.")

# %%
club_skaters.head()

print("\nSection 40: Warehouse exploration cell complete.")

# %%
club_goalies = table_summary("club_goalie_season_stats")

print("\nSection 41: Warehouse exploration cell complete.")

# %%
club_goalies.head()

print("\nSection 42: Warehouse exploration cell complete.")

# %% [markdown]
# 13. STATS REST REPORT FAMILIES
#
# These are pre-aggregated NHL statistical views.
#
# They are grouped into:
#   * skater reports
#   * goalie reports
#   * team reports
#
# Examples:
#   stats_skater_timeonice
#   stats_skater_powerplay
#   stats_goalie_savesByStrength
#   stats_team_goalsbyperiod
#
# They are not raw event data. Think of them as official NHL report tables.


# %%
stats_tables = [name for name in table_names if name.startswith("stats_")]

len(stats_tables), stats_tables

print("\nSection 43: Warehouse exploration cell complete.")

# %%
stats_inventory = inventory[
    inventory["table"].str.startswith("stats_")
].reset_index(drop=True)

stats_inventory

print("\nSection 44: Warehouse exploration cell complete.")

# %% [markdown]
# 14. SKATER REPORTS
#
# Many skater report tables have the same player-season/game-type grain but
# different groups of statistics.


# %%
stats_skater_summary = table_summary("stats_skater_summary")

print("\nSection 45: Warehouse exploration cell complete.")

# %%
stats_skater_summary.head()

print("\nSection 46: Warehouse exploration cell complete.")

# %%
stats_skater_toi = table_summary("stats_skater_timeonice")

print("\nSection 47: Warehouse exploration cell complete.")

# %%
stats_skater_toi.head()

print("\nSection 48: Warehouse exploration cell complete.")

# %%
stats_skater_realtime = table_summary("stats_skater_realtime")

print("\nSection 49: Warehouse exploration cell complete.")

# %%
stats_skater_realtime.head()

print("\nSection 50: Warehouse exploration cell complete.")

# %%
stats_skater_pp = table_summary("stats_skater_powerplay")

print("\nSection 51: Warehouse exploration cell complete.")

# %%
stats_skater_pp.head()

print("\nSection 52: Warehouse exploration cell complete.")

# %% [markdown]
# 15. GOALIE REPORTS
#
# Goalie reports include several specialized contexts that would otherwise
# require substantial reconstruction.


# %%
stats_goalie_summary = table_summary("stats_goalie_summary")

print("\nSection 53: Warehouse exploration cell complete.")

# %%
stats_goalie_summary.head()

print("\nSection 54: Warehouse exploration cell complete.")

# %%
stats_goalie_strength = table_summary("stats_goalie_savesByStrength")

print("\nSection 55: Warehouse exploration cell complete.")

# %%
stats_goalie_strength.head()

print("\nSection 56: Warehouse exploration cell complete.")

# %%
stats_goalie_rest = table_summary("stats_goalie_daysrest")

print("\nSection 57: Warehouse exploration cell complete.")

# %%
stats_goalie_rest.head()

print("\nSection 58: Warehouse exploration cell complete.")

# %% [markdown]
# 16. TEAM REPORTS
#
# Team report tables provide official NHL aggregate splits by categories such
# as period, strength state, special teams, score state and shooting.


# %%
stats_team_summary = table_summary("stats_team_summary")

print("\nSection 59: Warehouse exploration cell complete.")

# %%
stats_team_summary.head()

print("\nSection 60: Warehouse exploration cell complete.")

# %%
stats_team_periods = table_summary("stats_team_goalsbyperiod")

print("\nSection 61: Warehouse exploration cell complete.")

# %%
stats_team_periods.head()

print("\nSection 62: Warehouse exploration cell complete.")

# %%
stats_team_strength = table_summary("stats_team_goalsforbystrength")

print("\nSection 63: Warehouse exploration cell complete.")

# %%
stats_team_strength.head()

print("\nSection 64: Warehouse exploration cell complete.")

# %% [markdown]
# 17. DRAFT DATA
#
# These tables are separate from game analytics but make the warehouse more
# useful for roster-construction and prospect research.


# %%
drafts = table_summary("drafts")

print("\nSection 65: Warehouse exploration cell complete.")

# %%
drafts.head()

print("\nSection 66: Warehouse exploration cell complete.")

# %%
draft_picks = table_summary("draft_picks")

print("\nSection 67: Warehouse exploration cell complete.")

# %%
draft_picks.head(20)

print("\nSection 68: Warehouse exploration cell complete.")

# %% [markdown]
# 18. PLAYOFF SERIES
#
# GRAIN:
#   one playoff series
#
# This gives tournament / bracket context that is awkward to infer from
# individual game rows alone.


# %%
playoff_series = table_summary("playoff_series")

print("\nSection 69: Warehouse exploration cell complete.")

# %%
playoff_series

print("\nSection 70: Warehouse exploration cell complete.")

# %% [markdown]
# 19. REFERENCE TABLES
#
# The warehouse also contains API metadata/reference tables:
#
#   seasons
#   component_seasons
#   countries
#   stat_glossary
#   standings_seasons
#
# These are small, but useful for interpreting the rest of the API.


# %%
seasons = table_summary("seasons")

print("\nSection 71: Warehouse exploration cell complete.")

# %%
seasons.tail(20)

print("\nSection 72: Warehouse exploration cell complete.")

# %%
countries = table_summary("countries")

print("\nSection 73: Warehouse exploration cell complete.")

# %%
countries.head(20)

print("\nSection 74: Warehouse exploration cell complete.")

# %%
stat_glossary = table_summary("stat_glossary")

print("\nSection 75: Warehouse exploration cell complete.")

# %%
stat_glossary.head(30)

print("\nSection 76: Warehouse exploration cell complete.")

# %% [markdown]
# 20. HOW THE MAIN TABLES RELATE
#
# A useful mental model:
#
# games
#   |
#   +-- plays               event grain
#   +-- shifts              shift grain
#   +-- skater_game_stats   player-game grain
#   +-- goalie_game_stats   goalie-game grain
#   +-- team_game_stats     team-game grain
#
# players
#   |
#   +-- rosters
#   +-- player_game_logs
#   +-- skater/goalie game stats
#   +-- Stats REST player reports
#
# teams / franchises
#   |
#   +-- games
#   +-- team_game_stats
#   +-- standings_daily
#   +-- club season stats
#   +-- Stats REST team reports
#
# The exciting analytics usually come from JOINING grains carefully, not from
# treating one table as "the dataset."


# %%
# Example: event counts by game, then attach game context.

event_counts = (
    plays
    .groupby(["season_id", "game_id"])
    .size()
    .reset_index(name="events")
)

games_with_event_counts = games.merge(
    event_counts,
    on=["season_id", "game_id"],
    how="left",
)

games_with_event_counts[
    [
        "game_date",
        "game_type",
        "away_team",
        "home_team",
        "events",
    ]
].head(20)

print("\nSection 77: Warehouse exploration cell complete.")

# %% [markdown]
# 21. EXAMPLE: WHAT HAPPENED IN ONE GAME?
#
# Pick a game, then inspect its event stream and its shifts side-by-side.


# %%
example_game_id = int(games.iloc[0]["game_id"])

example_game_id

print("\nSection 78: Warehouse exploration cell complete.")

# %%
example_plays = (
    plays[plays["game_id"] == example_game_id]
    .sort_values("sortOrder")
)

example_plays[
    [
        "periodDescriptor_number",
        "timeInPeriod",
        "typeDescKey",
        "details_eventOwnerTeamId",
        "details_xCoord",
        "details_yCoord",
    ]
].head(100)

print("\nSection 79: Warehouse exploration cell complete.")

# %%
example_shifts = shifts[
    shifts["game_id"] == example_game_id
].copy()

example_shifts.head(50)

print("\nSection 80: Warehouse exploration cell complete.")

# %% [markdown]
# 22. QUESTIONS THIS WAREHOUSE CAN EVENTUALLY SUPPORT
#
# EVENT / TACTICAL
#   * xG and shot quality
#   * shot sequences
#   * rebounds
#   * pre-shot movement
#   * possession/event-state models
#   * special-teams patterns
#
# PLAYER IMPACT
#   * on-ice rates
#   * teammate/opponent-adjusted impact
#   * RAPM-style models
#   * deployment and matchup analysis
#   * line / pair chemistry
#
# TEAM
#   * style profiles
#   * special teams
#   * score-state behavior
#   * rest effects
#   * playoff-vs-regular-season differences
#
# ROSTER / PLAYER DEVELOPMENT
#   * aging curves
#   * player comparables
#   * draft analysis
#   * role/deployment changes
#
# IMPORTANT:
# The public NHL API is rich, but it is not raw optical tracking. We have event
# coordinates and shift timing, not continuous XY trajectories of every player
# and the puck.


# %% [markdown]
# 23. NEXT EXPLORATION IDEAS
#
# Try these interactively:
#
#   plays["typeDescKey"].value_counts()
#
#   plays.groupby("game_type")["typeDescKey"].value_counts()
#
#   shifts.groupby("playerId")["duration"].count().sort_values(ascending=False)
#
#   skater_game_stats.groupby("playerId").size().sort_values(ascending=False)
#
#   standings_daily.columns.tolist()
#
#   stats_skater_summary.columns.tolist()
#
#   stats_team_summary.columns.tolist()
#
# A major next step will be adding DuckDB so we can query all of these Parquet
# tables without loading entire tables into pandas first.
