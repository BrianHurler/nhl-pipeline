# %% [markdown]
# 16_qa_curated_warehouse.py
#
# PURPOSE
# -------
# Run integrity and coverage checks against the curated NHL Parquet warehouse.
#
# The QA is intentionally broad: table presence/readability, processed-to-curated
# row reconciliation, key uniqueness, game coverage, foreign-key coverage, nulls,
# season/game-type coverage, standings coverage, Stats REST presence, and partitions.

# %%
# ============================================================================
# 1. IMPORTS, PATHS, AND REUSABLE QA HELPERS
# ============================================================================
#
# WHAT:
# Load pandas/path tools, configure QA locations, and define the small reusable
# operations used repeatedly by the checks below.
#
# WHY:
# Reading curated partition sets, recording standardized PASS/WARN/FAIL rows,
# and checking key uniqueness are repeated operations where helpers materially
# reduce duplication and inconsistency.
#
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
CURATED_DIR = PROJECT_ROOT / "data" / "curated"
QA_DIR = PROJECT_ROOT / "data" / "qa"
QA_DIR.mkdir(parents=True, exist_ok=True)

results = []


def record(check, status, detail):
    results.append({
        "check": check,
        "status": status,
        "detail": str(detail),
    })


def parquet_files(table_name):
    table_dir = CURATED_DIR / table_name
    if not table_dir.exists():
        return []
    return sorted(table_dir.rglob("*.parquet"))


def read_curated(table_name):
    files = parquet_files(table_name)
    if not files:
        raise FileNotFoundError(f"No parquet files found for curated table: {table_name}")
    frames = [pd.read_parquet(path) for path in files]
    return pd.concat(frames, ignore_index=True, sort=False)


def check_unique(df, cols, label, status_if_duplicate="FAIL"):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        record(label, "WARN", f"missing columns: {missing}")
        return
    duplicates = int(df.duplicated(cols).sum())
    record(
        label,
        "PASS" if duplicates == 0 else status_if_duplicate,
        f"duplicates={duplicates}",
    )


CORE_TABLES = [
    "games",
    "plays",
    "shifts",
    "skater_game_stats",
    "goalie_game_stats",
    "team_game_stats",
    "rosters",
    "players",
    "teams",
]

SUPPLEMENTAL_TABLES = [
    "standings_daily",
    "player_game_logs",
    "club_skater_season_stats",
    "club_goalie_season_stats",
    "franchises",
    "seasons",
    "component_seasons",
    "countries",
    "drafts",
    "stat_glossary",
    "standings_seasons",
    "draft_picks",
    "playoff_series",
]

STATS_REPORT_TABLES = [
    "stats_goalie_advanced",
    "stats_goalie_bios",
    "stats_goalie_daysrest",
    "stats_goalie_penaltyShots",
    "stats_goalie_savesByStrength",
    "stats_goalie_shootout",
    "stats_goalie_startedVsRelieved",
    "stats_goalie_summary",
    "stats_skater_bios",
    "stats_skater_faceoffpercentages",
    "stats_skater_faceoffwins",
    "stats_skater_goalsForAgainst",
    "stats_skater_penalties",
    "stats_skater_penaltyShots",
    "stats_skater_penaltykill",
    "stats_skater_percentages",
    "stats_skater_powerplay",
    "stats_skater_puckPossessions",
    "stats_skater_realtime",
    "stats_skater_scoringRates",
    "stats_skater_scoringpergame",
    "stats_skater_shootout",
    "stats_skater_shottype",
    "stats_skater_summary",
    "stats_skater_summaryshooting",
    "stats_skater_timeonice",
    "stats_team_daysbetweengames",
    "stats_team_faceoffpercentages",
    "stats_team_faceoffwins",
    "stats_team_goalgames",
    "stats_team_goalsagainstbystrength",
    "stats_team_goalsbyperiod",
    "stats_team_goalsforbystrength",
    "stats_team_leadingtrailing",
    "stats_team_outshootoutshotby",
    "stats_team_penalties",
    "stats_team_penaltykill",
    "stats_team_penaltykilltime",
    "stats_team_percentages",
    "stats_team_powerplay",
    "stats_team_powerplaytime",
    "stats_team_realtime",
    "stats_team_scoretrailfirst",
    "stats_team_shootout",
    "stats_team_shottype",
    "stats_team_summary",
    "stats_team_summaryshooting",
]

EXPECTED_TABLES = CORE_TABLES + SUPPLEMENTAL_TABLES + STATS_REPORT_TABLES


# %%
# ============================================================================
# 1. INVENTORY, READABILITY, ROW COUNTS
# ============================================================================

table_cache = {}

for table in EXPECTED_TABLES:
    files = parquet_files(table)

    if not files:
        record(f"{table}: curated parquet exists", "FAIL", "no parquet files found")
        continue

    try:
        df = read_curated(table)
        table_cache[table] = df
        record(
            f"{table}: readable",
            "PASS",
            f"rows={len(df):,}; columns={len(df.columns)}; files={len(files)}",
        )
    except Exception as error:
        record(f"{table}: readable", "FAIL", repr(error))

print("\nSection 1: 1. inventory, readability, row counts complete.")

# %%
# ============================================================================
# 2. PROCESSED -> CURATED RECONCILIATION
# ============================================================================

for table in EXPECTED_TABLES:
    if table not in table_cache:
        continue

    processed_parquet = PROCESSED_DIR / f"{table}.parquet"
    processed_csv = PROCESSED_DIR / (
        "game_manifest.csv" if table == "games"
        else "plays.csv" if table == "plays"
        else ""
    )

    try:
        if processed_parquet.exists():
            processed_rows = len(pd.read_parquet(processed_parquet))
        elif table in {"games", "plays"} and processed_csv.exists():
            processed_rows = len(pd.read_csv(processed_csv, low_memory=False))
        else:
            continue

        curated_rows = len(table_cache[table])
        status = "PASS" if processed_rows == curated_rows else "FAIL"
        record(
            f"{table}: processed vs curated rows",
            status,
            f"processed={processed_rows:,}; curated={curated_rows:,}",
        )
    except Exception as error:
        record(f"{table}: processed vs curated rows", "FAIL", repr(error))

print("\nSection 2: 2. processed -> curated reconciliation complete.")

# %%
# ============================================================================
# 3. CORE PRIMARY-KEY / GRAIN CHECKS
# ============================================================================

games = table_cache.get("games")
plays = table_cache.get("plays")
shifts = table_cache.get("shifts")
skaters = table_cache.get("skater_game_stats")
goalies = table_cache.get("goalie_game_stats")
team_games = table_cache.get("team_game_stats")
players = table_cache.get("players")
teams = table_cache.get("teams")
rosters = table_cache.get("rosters")
player_logs = table_cache.get("player_game_logs")

if games is not None:
    check_unique(games, ["season_id", "game_id"], "games: unique season_id + game_id")

if plays is not None:
    check_unique(
        plays,
        ["season_id", "game_id", "eventId"],
        "plays: unique season/game/event",
    )

if shifts is not None:
    check_unique(shifts, ["id"], "shifts: NHL id unique")

if players is not None:
    check_unique(players, ["player_id"], "players: player_id unique")

if rosters is not None:
    check_unique(
        rosters,
        ["season_id", "team", "player_id"],
        "rosters: season/team/player unique",
        status_if_duplicate="WARN",
    )

if team_games is not None:
    check_unique(
        team_games,
        ["season_id", "game_id", "team_id"],
        "team_game_stats: season/game/team unique",
    )

    if "game_id" in team_games.columns:
        counts = team_games.groupby("game_id", dropna=False).size()
        bad = counts[counts != 2]
        record(
            "team_game_stats: exactly 2 rows per game",
            "PASS" if bad.empty else "FAIL",
            f"bad_games={len(bad)}",
        )

print("\nSection 3: 3. core primary-key / grain checks complete.")

# %%
# ============================================================================
# 4. GAME COVERAGE
# ============================================================================

if games is not None:
    manifest_game_ids = set(pd.to_numeric(games["game_id"], errors="coerce").dropna().astype(int))

    for table_name in [
        "plays",
        "shifts",
        "skater_game_stats",
        "goalie_game_stats",
        "team_game_stats",
    ]:
        df = table_cache.get(table_name)
        if df is None or "game_id" not in df.columns:
            continue

        ids = set(pd.to_numeric(df["game_id"], errors="coerce").dropna().astype(int))
        missing = manifest_game_ids - ids

        # Goalies/skaters can theoretically have unusual edge cases, so only
        # the event/team fact tables are hard failures for missing games.
        status = "PASS" if not missing else (
            "FAIL" if table_name in {"plays", "shifts", "team_game_stats"} else "WARN"
        )

        record(
            f"{table_name}: manifest game coverage",
            status,
            f"represented={len(ids):,}; missing={len(missing):,}",
        )

print("\nSection 4: 4. game coverage complete.")

# %%
# ============================================================================
# 5. FOREIGN-KEY COVERAGE
# ============================================================================

if players is not None and "player_id" in players.columns:
    known_players = set(
        pd.to_numeric(players["player_id"], errors="coerce").dropna().astype(int)
    )

    for table_name in ["skater_game_stats", "goalie_game_stats", "player_game_logs"]:
        df = table_cache.get(table_name)
        if df is None:
            continue

        id_col = next((c for c in ["playerId", "player_id"] if c in df.columns), None)
        if id_col is None:
            record(f"{table_name}: player FK coverage", "WARN", "no recognized player ID column")
            continue

        referenced = set(
            pd.to_numeric(df[id_col], errors="coerce").dropna().astype(int)
        )
        missing = referenced - known_players

        record(
            f"{table_name}: player FK coverage",
            "PASS" if not missing else "WARN",
            f"referenced={len(referenced):,}; unmatched={len(missing):,}",
        )

if teams is not None:
    team_id_col = next((c for c in ["id", "teamId", "team_id"] if c in teams.columns), None)

    if team_id_col:
        known_teams = set(
            pd.to_numeric(teams[team_id_col], errors="coerce").dropna().astype(int)
        )

        for table_name in ["team_game_stats", "skater_game_stats", "goalie_game_stats"]:
            df = table_cache.get(table_name)
            if df is None or "team_id" not in df.columns:
                continue

            referenced = set(
                pd.to_numeric(df["team_id"], errors="coerce").dropna().astype(int)
            )
            missing = referenced - known_teams

            record(
                f"{table_name}: team FK coverage",
                "PASS" if not missing else "WARN",
                f"referenced={len(referenced):,}; unmatched={len(missing):,}",
            )

print("\nSection 5: 5. foreign-key coverage complete.")

# %%
# ============================================================================
# 6. CRITICAL NULL CHECKS
# ============================================================================

CRITICAL_FIELDS = {
    "games": ["season_id", "game_id"],
    "plays": ["season_id", "game_id", "eventId", "typeDescKey"],
    "shifts": ["season_id", "game_id", "id"],
    "players": ["player_id"],
    "team_game_stats": ["season_id", "game_id", "team_id"],
}

for table_name, columns in CRITICAL_FIELDS.items():
    df = table_cache.get(table_name)
    if df is None:
        continue

    for col in columns:
        if col not in df.columns:
            record(f"{table_name}: {col} present", "FAIL", "column missing")
            continue

        null_count = int(df[col].isna().sum())
        record(
            f"{table_name}: {col} non-null",
            "PASS" if null_count == 0 else "FAIL",
            f"nulls={null_count:,}",
        )

print("\nSection 6: 6. critical null checks complete.")

# %%
# ============================================================================
# 7. SEASON AND GAME-TYPE COVERAGE
# ============================================================================

if games is not None:
    if "season_id" in games.columns:
        season_counts = games["season_id"].astype(str).value_counts().to_dict()
        record("games: season coverage", "PASS", season_counts)

    if "game_type" in games.columns:
        game_type_counts = games["game_type"].value_counts(dropna=False).sort_index().to_dict()
        expected_types = {1, 2, 3}
        actual_types = set(pd.to_numeric(games["game_type"], errors="coerce").dropna().astype(int))
        record(
            "games: preseason/regular/playoff coverage",
            "PASS" if expected_types.issubset(actual_types) else "WARN",
            game_type_counts,
        )

print("\nSection 7: 7. season and game-type coverage complete.")

# %%
# ============================================================================
# 8. STANDINGS COVERAGE
# ============================================================================

standings = table_cache.get("standings_daily")

if standings is not None:
    date_col = next(
        (
            c for c in [
                "standings_date",
                "standingsDate",
                "date",
                "date_",
            ]
            if c in standings.columns
        ),
        None,
    )

    if date_col:
        dates = pd.to_datetime(standings[date_col], errors="coerce")
        record(
            "standings_daily: date range",
            "PASS" if dates.notna().any() else "WARN",
            f"{dates.min()} through {dates.max()}; unique_dates={dates.nunique()}",
        )
    else:
        record(
            "standings_daily: date range",
            "WARN",
            "could not identify date column",
        )

print("\nSection 8: 8. standings coverage complete.")

# %%
# ============================================================================
# 9. STATS REST REPORT PRESENCE AND BASIC SHAPE
# ============================================================================

present_stats = [name for name in STATS_REPORT_TABLES if name in table_cache]

record(
    "Stats REST reports: all 47 present",
    "PASS" if len(present_stats) == 47 else "FAIL",
    f"present={len(present_stats)}/47",
)

for table_name in present_stats:
    df = table_cache[table_name]

    if len(df) == 0:
        record(f"{table_name}: non-empty", "WARN", "0 rows")

    if "season_id" in df.columns:
        null_season = int(df["season_id"].isna().sum())
        record(
            f"{table_name}: season_id non-null",
            "PASS" if null_season == 0 else "WARN",
            f"nulls={null_season}",
        )

print("\nSection 9: 9. stats rest report presence and basic shape complete.")

# %%
# ============================================================================
# 10. PARTITION SANITY
# ============================================================================

for table in EXPECTED_TABLES:
    files = parquet_files(table)
    if not files:
        continue

    season_dirs = sorted(
        {
            parent.name
            for file in files
            for parent in file.parents
            if parent.parent == CURATED_DIR / table and parent.name.startswith("season_id=")
        }
    )

    if season_dirs:
        record(
            f"{table}: season partitions",
            "PASS",
            ", ".join(season_dirs),
        )

print("\nSection 10: 10. partition sanity complete.")

# %%
# ============================================================================
# WRITE QA ARTIFACTS
# ============================================================================

qa = pd.DataFrame(results)

qa_file = QA_DIR / "curated_warehouse_qa.csv"
qa.to_csv(qa_file, index=False)

summary = (
    qa["status"]
    .value_counts()
    .reindex(["PASS", "WARN", "FAIL"], fill_value=0)
)

summary_file = QA_DIR / "curated_warehouse_qa_summary.csv"
summary.rename_axis("status").reset_index(name="count").to_csv(summary_file, index=False)

print("\n" + "=" * 72)
print("NHL CURATED WAREHOUSE QA")
print("=" * 72)

for status, count in summary.items():
    print(f"{status:5s}: {count}")

print(f"\nDetailed report: {qa_file}")
print(f"Summary report:  {summary_file}")

failures = qa[qa["status"] == "FAIL"]
warnings = qa[qa["status"] == "WARN"]

if not failures.empty:
    print("\nFAILURES")
    print(failures[["check", "detail"]].to_string(index=False))

if not warnings.empty:
    print("\nWARNINGS")
    print(warnings[["check", "detail"]].to_string(index=False))

if failures.empty:
    print("\nWarehouse QA completed with no FAIL results.")
else:
    print("\nWarehouse QA FAILED. Resolve failures before S3 upload.")

print("\nSection 11: write qa artifacts complete.")

