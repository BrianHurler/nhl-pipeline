# %% [markdown]
# 18_build_shots_table.py
#
# PURPOSE
# -------
# Build the canonical reusable shot-attempt table from curated NHL play-by-play.
# The table standardizes attacking direction, derives shot geometry, runs QA, and
# writes a season-partitioned curated shots table.
#
# This script is organized into # %% cells for VS Code's Jupyter Interactive Window.
# Each section ends with a short completion message so cell execution is visible.

from pathlib import Path

import boto3
import shutil

import numpy as np
import pandas as pd


# %%
# ============================================================================
# CONFIGURATION
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

S3_BUCKET = "nhl-data-bucket1"
S3_REGION = "us-east-1"
S3_PREFIX = "nhl-data-research"
s3 = boto3.client("s3", region_name=S3_REGION)

CURATED_DIR = PROJECT_ROOT / "data" / "curated"
PLAYS_DIR = CURATED_DIR / "plays"
SHOTS_DIR = CURATED_DIR / "shots"

QA_DIR = PROJECT_ROOT / "data" / "qa"

SHOT_EVENT_TYPES = {
    "goal",
    "shot-on-goal",
    "missed-shot",
}

# NHL rink coordinate convention:
# goal centers are approximately at x = +/- 89 feet.
ATTACKING_GOAL_X = 89.0

# Regulation NHL rink dimensions are 200 x 85 feet.
RINK_X_LIMIT = 100.0
RINK_Y_LIMIT = 42.5

print("\nSection 1: configuration complete.")

# %%
# ============================================================================
# HELPERS
# ============================================================================

def load_parquet_table(table_dir):
    """
    Load every parquet partition for one curated warehouse table.
    """

    files = sorted(table_dir.rglob("*.parquet"))

    if not files:
        raise FileNotFoundError(
            f"No parquet files found under: {table_dir}"
        )

    print(f"Loading {len(files)} parquet files from {table_dir}")

    return pd.concat(
        [pd.read_parquet(path) for path in files],
        ignore_index=True,
        sort=False,
    )


def require_columns(df, columns):
    """
    Fail early if a field needed for the canonical shot table
    is missing from the play-by-play warehouse.
    """

    missing = sorted(set(columns) - set(df.columns))

    if missing:
        raise ValueError(
            "plays table is missing required columns:\n  "
            + "\n  ".join(missing)
        )


def write_partitioned(df, table_dir, partition_col="season_id"):
    """
    Write the shots table using the same partition convention as
    the rest of the curated warehouse.
    """

    if table_dir.exists():
        shutil.rmtree(table_dir)

    table_dir.mkdir(parents=True, exist_ok=True)

    for value, chunk in df.groupby(partition_col, dropna=False):
        value_text = "unknown" if pd.isna(value) else str(value)

        out_dir = table_dir / f"{partition_col}={value_text}"
        out_dir.mkdir(parents=True, exist_ok=True)

        output_file = out_dir / "part-000.parquet"

        chunk.to_parquet(
            output_file,
            index=False,
        )

        s3_key = (
            f"{S3_PREFIX}/"
            f"{output_file.relative_to(PROJECT_ROOT).as_posix()}"
        )

        s3.upload_file(
            str(output_file),
            S3_BUCKET,
            s3_key,
        )

        print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")

print("\nSection 2: helpers complete.")

# %%
# ============================================================================
# LOAD PLAY-BY-PLAY
# ============================================================================

if not PLAYS_DIR.exists():
    raise FileNotFoundError(
        f"Curated plays table not found: {PLAYS_DIR}\n"
        "Run the warehouse build first."
    )

plays = load_parquet_table(PLAYS_DIR)

print()
print(f"All play rows: {len(plays):,}")

print("\nSection 3: load play-by-play complete.")

# %%
# ============================================================================
# REQUIRED FIELDS
# ============================================================================

required_columns = [
    "season_id",
    "season",
    "game_id",
    "game_type",
    "game_date",
    "away_team_id",
    "away_team",
    "home_team_id",
    "home_team",
    "eventId",
    "sortOrder",
    "typeCode",
    "typeDescKey",
    "periodDescriptor_number",
    "periodDescriptor_periodType",
    "timeInPeriod",
    "timeRemaining",
    "situationCode",
    "homeTeamDefendingSide",
    "details_eventOwnerTeamId",
    "details_xCoord",
    "details_yCoord",
    "details_shotType",
    "details_goalieInNetId",
]

require_columns(
    plays,
    required_columns,
)


# These fields are useful but can legitimately be sparse.
optional_columns = [
    "utc",
    "details_zoneCode",
    "details_shootingPlayerId",
    "details_scoringPlayerId",
    "details_homeScore",
    "details_awayScore",
]

for column in optional_columns:
    if column not in plays.columns:
        plays[column] = pd.NA

print("\nSection 4: required fields complete.")

# %%
# ============================================================================
# FILTER TO THE INITIAL XG POPULATION
# ============================================================================

shots = plays[
    plays["typeDescKey"].isin(SHOT_EVENT_TYPES)
].copy()

expected_shot_rows = int(
    plays["typeDescKey"]
    .isin(SHOT_EVENT_TYPES)
    .sum()
)

if len(shots) != expected_shot_rows:
    raise AssertionError(
        "Shot-row reconciliation failed."
    )

print(f"Unblocked shot-attempt rows: {len(shots):,}")

print("\nSection 5: filter to the initial xg population complete.")

# %%
# ============================================================================
# CLEAN IDENTIFIERS
# ============================================================================

id_columns = [
    "game_id",
    "eventId",
    "home_team_id",
    "away_team_id",
    "details_eventOwnerTeamId",
    "details_shootingPlayerId",
    "details_scoringPlayerId",
    "details_goalieInNetId",
]

for column in id_columns:
    shots[column] = (
        pd.to_numeric(
            shots[column],
            errors="coerce",
        )
        .astype("Int64")
    )

shots["season_id"] = shots["season_id"].astype(str)

print("\nSection 6: clean identifiers complete.")

# %%
# ============================================================================
# CANONICAL EVENT FIELDS
# ============================================================================

shots["event_type"] = shots["typeDescKey"]
shots["event_id"] = shots["eventId"]

shots["period"] = pd.to_numeric(
    shots["periodDescriptor_number"],
    errors="coerce",
).astype("Int64")

shots["period_type"] = (
    shots["periodDescriptor_periodType"]
    .astype("string")
)

shots["shooting_team_id"] = (
    shots["details_eventOwnerTeamId"]
)

shots["shot_type"] = (
    shots["details_shotType"]
    .astype("string")
)

shots["zone_code"] = (
    shots["details_zoneCode"]
    .astype("string")
)

shots["home_defending_side"] = (
    shots["homeTeamDefendingSide"]
    .astype("string")
    .str.lower()
)

print("\nSection 7: canonical event fields complete.")

# %%
# ============================================================================
# OUTCOME
# ============================================================================

shots["is_goal"] = (
    shots["event_type"]
    .eq("goal")
    .astype("int8")
)

print("\nSection 8: outcome complete.")

# %%
# ============================================================================
# SHOOTER
# ============================================================================

shooting_player = pd.to_numeric(
    shots["details_shootingPlayerId"],
    errors="coerce",
)

scoring_player = pd.to_numeric(
    shots["details_scoringPlayerId"],
    errors="coerce",
)

shots["shooter_id"] = (
    shooting_player
    .fillna(scoring_player)
    .astype("Int64")
)

shots["goalie_id"] = (
    pd.to_numeric(
        shots["details_goalieInNetId"],
        errors="coerce",
    )
    .astype("Int64")
)

print("\nSection 9: shooter complete.")

# %%
# ============================================================================
# SHOOTING / DEFENDING TEAM CONTEXT
# ============================================================================

home_team_id = pd.to_numeric(
    shots["home_team_id"],
    errors="coerce",
)

away_team_id = pd.to_numeric(
    shots["away_team_id"],
    errors="coerce",
)

shooting_team_id = pd.to_numeric(
    shots["shooting_team_id"],
    errors="coerce",
)

# Convert nullable comparisons to ordinary bool masks. Pandas' nullable
# BooleanArray can carry pd.NA, which makes compound masks with "&" ambiguous.
is_home_shot = (
    shooting_team_id.eq(home_team_id)
    .fillna(False)
    .astype(bool)
)
is_away_shot = (
    shooting_team_id.eq(away_team_id)
    .fillna(False)
    .astype(bool)
)

shots["is_home_shot"] = is_home_shot

shots["shooting_team"] = pd.Series(
    pd.NA,
    index=shots.index,
    dtype="string",
)

shots.loc[
    is_home_shot,
    "shooting_team",
] = shots.loc[
    is_home_shot,
    "home_team",
].astype("string")

shots.loc[
    is_away_shot,
    "shooting_team",
] = shots.loc[
    is_away_shot,
    "away_team",
].astype("string")

shots["defending_team_id"] = pd.Series(
    pd.NA,
    index=shots.index,
    dtype="Int64",
)

shots.loc[
    is_home_shot,
    "defending_team_id",
] = shots.loc[
    is_home_shot,
    "away_team_id",
]

shots.loc[
    is_away_shot,
    "defending_team_id",
] = shots.loc[
    is_away_shot,
    "home_team_id",
]

shots["defending_team"] = pd.Series(
    pd.NA,
    index=shots.index,
    dtype="string",
)

shots.loc[
    is_home_shot,
    "defending_team",
] = shots.loc[
    is_home_shot,
    "away_team",
].astype("string")

shots.loc[
    is_away_shot,
    "defending_team",
] = shots.loc[
    is_away_shot,
    "home_team",
].astype("string")

print("\nSection 10: shooting / defending team context complete.")

# %%
# ============================================================================
# RAW COORDINATES
# ============================================================================

shots["x_coord"] = pd.to_numeric(
    shots["details_xCoord"],
    errors="coerce",
)

shots["y_coord"] = pd.to_numeric(
    shots["details_yCoord"],
    errors="coerce",
)

print("\nSection 11: raw coordinates complete.")

# %%
# ============================================================================
# DETERMINE WHICH GOAL THE SHOOTING TEAM IS ATTACKING
# ============================================================================

shots["attacking_goal_side"] = pd.Series(
    pd.NA,
    index=shots.index,
    dtype="string",
)

home_defends_left = (
    shots["home_defending_side"]
    .eq("left")
    .fillna(False)
    .astype(bool)
)

home_defends_right = (
    shots["home_defending_side"]
    .eq("right")
    .fillna(False)
    .astype(bool)
)

shots.loc[
    is_home_shot & home_defends_left,
    "attacking_goal_side",
] = "right"

shots.loc[
    is_home_shot & home_defends_right,
    "attacking_goal_side",
] = "left"

shots.loc[
    is_away_shot & home_defends_left,
    "attacking_goal_side",
] = "left"

shots.loc[
    is_away_shot & home_defends_right,
    "attacking_goal_side",
] = "right"

print("\nSection 12: determine which goal the shooting team is attacking complete.")

# %%
# ============================================================================
# STANDARDIZE COORDINATES
# ============================================================================

shots["x_attacking"] = np.nan
shots["y_attacking"] = np.nan

attacks_right = (
    shots["attacking_goal_side"]
    .eq("right")
    .fillna(False)
    .astype(bool)
)

attacks_left = (
    shots["attacking_goal_side"]
    .eq("left")
    .fillna(False)
    .astype(bool)
)

shots.loc[
    attacks_right,
    "x_attacking",
] = shots.loc[
    attacks_right,
    "x_coord",
]

shots.loc[
    attacks_right,
    "y_attacking",
] = shots.loc[
    attacks_right,
    "y_coord",
]

shots.loc[
    attacks_left,
    "x_attacking",
] = -shots.loc[
    attacks_left,
    "x_coord",
]

shots.loc[
    attacks_left,
    "y_attacking",
] = -shots.loc[
    attacks_left,
    "y_coord",
]

print("\nSection 13: standardize coordinates complete.")

# %%
# ============================================================================
# BASIC SHOT GEOMETRY
# ============================================================================

x_distance_to_goal = (
    ATTACKING_GOAL_X
    - shots["x_attacking"]
)

y_distance_to_goal = (
    shots["y_attacking"]
)

shots["shot_distance_ft"] = np.hypot(
    x_distance_to_goal,
    y_distance_to_goal,
)

shots["shot_angle_deg"] = np.degrees(
    np.arctan2(
        np.abs(y_distance_to_goal),
        x_distance_to_goal,
    )
)

print("\nSection 14: basic shot geometry complete.")

# %%
# ============================================================================
# SIMPLE EVENT KEY
# ============================================================================

shots["shot_id"] = (
    shots["game_id"]
    .astype("string")
    + "-"
    + shots["event_id"]
    .astype("string")
)

print("\nSection 15: simple event key complete.")

# %%
# ============================================================================
# SELECT / ORDER CANONICAL OUTPUT
# ============================================================================

output_columns = [
    "shot_id",
    "season_id",
    "season",
    "game_id",
    "event_id",
    "sortOrder",
    "game_type",
    "game_date",
    "home_team_id",
    "home_team",
    "away_team_id",
    "away_team",
    "event_type",
    "typeCode",
    "period",
    "period_type",
    "timeInPeriod",
    "timeRemaining",
    "utc",
    "situationCode",
    "zone_code",
    "shooting_team_id",
    "shooting_team",
    "defending_team_id",
    "defending_team",
    "is_home_shot",
    "shooter_id",
    "goalie_id",
    "shot_type",
    "is_goal",
    "x_coord",
    "y_coord",
    "home_defending_side",
    "attacking_goal_side",
    "x_attacking",
    "y_attacking",
    "shot_distance_ft",
    "shot_angle_deg",
    "details_homeScore",
    "details_awayScore",
]

shots = shots[output_columns].copy()

print("\nSection 16: select / order canonical output complete.")

# %%
# ============================================================================
# QA
# ============================================================================

print()
print("=" * 72)
print("SHOT TABLE QA")
print("=" * 72)

print()
print("EVENT TYPES")
print(
    shots["event_type"]
    .value_counts(dropna=False)
)

goal_event_count = int(
    shots["event_type"]
    .eq("goal")
    .sum()
)

goal_flag_count = int(
    shots["is_goal"]
    .sum()
)

if goal_event_count != goal_flag_count:
    raise AssertionError(
        "is_goal does not reconcile to goal events."
    )

print()
print(f"Goals: {goal_flag_count:,}")

duplicate_events = int(
    shots.duplicated(
        subset=[
            "season_id",
            "game_id",
            "event_id",
        ]
    )
    .sum()
)

print(
    "Duplicate "
    "(season_id, game_id, event_id) rows: "
    f"{duplicate_events:,}"
)

if duplicate_events > 0:
    raise AssertionError(
        "Duplicate shot events detected."
    )

unknown_shooting_team = int(
    (~is_home_shot & ~is_away_shot)
    .sum()
)

print(
    "Shot rows where eventOwnerTeamId does not match "
    "home or away team: "
    f"{unknown_shooting_team:,}"
)

qa_missing_columns = [
    "shooter_id",
    "goalie_id",
    "x_coord",
    "y_coord",
    "home_defending_side",
    "attacking_goal_side",
    "x_attacking",
    "y_attacking",
    "shot_distance_ft",
    "shot_angle_deg",
]

missing_summary = []

for column in qa_missing_columns:
    missing_n = int(
        shots[column]
        .isna()
        .sum()
    )

    missing_pct = (
        missing_n
        / len(shots)
        * 100
        if len(shots)
        else 0
    )

    missing_summary.append({
        "column": column,
        "missing_n": missing_n,
        "missing_pct": round(
            missing_pct,
            3,
        ),
    })

missing_summary = pd.DataFrame(
    missing_summary
)

print()
print("MISSINGNESS")
print(
    missing_summary
    .to_string(index=False)
)

participant_missing = (
    shots
    .groupby(
        "event_type",
        dropna=False,
    )
    .agg(
        rows=("shot_id", "size"),
        missing_shooter=(
            "shooter_id",
            lambda x: x.isna().sum(),
        ),
        missing_goalie=(
            "goalie_id",
            lambda x: x.isna().sum(),
        ),
    )
    .reset_index()
)

print()
print("PARTICIPANT MISSINGNESS BY EVENT TYPE")
print(
    participant_missing
    .to_string(index=False)
)

x_out_of_bounds = int(
    shots["x_coord"]
    .abs()
    .gt(RINK_X_LIMIT)
    .fillna(False)
    .sum()
)

y_out_of_bounds = int(
    shots["y_coord"]
    .abs()
    .gt(RINK_Y_LIMIT)
    .fillna(False)
    .sum()
)

print()
print("RAW COORDINATE CHECKS")
print(
    f"|x| > {RINK_X_LIMIT}: "
    f"{x_out_of_bounds:,}"
)
print(
    f"|y| > {RINK_Y_LIMIT}: "
    f"{y_out_of_bounds:,}"
)

negative_distance = int(
    shots["shot_distance_ft"]
    .lt(0)
    .fillna(False)
    .sum()
)

invalid_angle = int(
    (
        shots["shot_angle_deg"].lt(0)
        | shots["shot_angle_deg"].gt(180)
    )
    .fillna(False)
    .sum()
)

behind_goal_line = int(
    shots["x_attacking"]
    .gt(ATTACKING_GOAL_X)
    .fillna(False)
    .sum()
)

print()
print("GEOMETRY CHECKS")
print(
    f"Negative distance: "
    f"{negative_distance:,}"
)
print(
    f"Angle outside 0-180 degrees: "
    f"{invalid_angle:,}"
)
print(
    f"Attempts originating behind goal line: "
    f"{behind_goal_line:,}"
)

if negative_distance > 0:
    raise AssertionError(
        "Negative shot distance detected."
    )

if invalid_angle > 0:
    raise AssertionError(
        "Invalid shot angle detected."
    )

season_summary = (
    shots
    .groupby(
        ["season_id", "season"],
        dropna=False,
    )
    .agg(
        shots=("shot_id", "size"),
        goals=("is_goal", "sum"),
        games=("game_id", "nunique"),
    )
    .reset_index()
)

season_summary["goal_rate"] = (
    season_summary["goals"]
    / season_summary["shots"]
)

print()
print("SEASON SUMMARY")
print(
    season_summary
    .to_string(index=False)
)

print("\nSection 17: qa complete.")

# %%
# ============================================================================
# WRITE QA OUTPUTS
# ============================================================================

QA_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

missing_summary.to_csv(
    QA_DIR / "shots_missingness.csv",
    index=False,
)

participant_missing.to_csv(
    QA_DIR / "shots_participant_missingness.csv",
    index=False,
)

season_summary.to_csv(
    QA_DIR / "shots_season_summary.csv",
    index=False,
)

print("\nSection 18: write qa outputs complete.")

# %%
# ============================================================================
# WRITE CURATED SHOTS TABLE
# ============================================================================

write_partitioned(
    shots,
    SHOTS_DIR,
)

print()
print("=" * 72)
print("BUILD COMPLETE")
print("=" * 72)
print(
    f"Shots: {len(shots):,}"
)
print(
    f"Goals: {goal_flag_count:,}"
)
print(
    f"Games: {shots['game_id'].nunique():,}"
)
print(
    f"Output: {SHOTS_DIR}"
)
print()
print(
    "QA files:"
)
print(
    f"  {QA_DIR / 'shots_missingness.csv'}"
)
print(
    f"  {QA_DIR / 'shots_participant_missingness.csv'}"
)
print(
    f"  {QA_DIR / 'shots_season_summary.csv'}"
)

print("\nSection 19: write curated shots table complete.")

