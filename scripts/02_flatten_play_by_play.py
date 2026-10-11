# %% [markdown]
# 02_flatten_play_by_play.py
#
# PURPOSE
# -------
# Download one NHL game's play-by-play data and flatten nested JSON fields into
# regular DataFrame columns.
#
# This builds directly on script 01. The important new idea is
# `pandas.json_normalize()`, which is especially useful when API records contain
# nested dictionaries.
#
# UNIT OF OBSERVATION
# -------------------
# The final `plays_flat` DataFrame has one row per play-by-play event for one game.

# %%
# =========================================================
# 1. Import libraries
# =========================================================
#
# WHAT:
# Load requests for the API call and pandas for tabular data work.
#
# WHY:
# We need both the raw web response and tools for flattening it into columns.
#
# PYTHON NOTES:
# - requests: HTTP requests.
# - pandas: DataFrame creation and JSON normalization.
# =========================================================

import requests
import pandas as pd

print("\nSection 1: Libraries loaded.")

# %%
# =========================================================
# 2. Choose one NHL game and build the endpoint
# =========================================================
#
# WHAT:
# Specify one game and create its play-by-play URL.
#
# WHY:
# Keeping the example to one game lets us focus on understanding nested JSON
# before later scripts scale the process across the full game manifest.
#
# PYTHON NOTES:
# - f-string: inserts `game_id` directly into the URL string.
# =========================================================

game_id = 2023020204

url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"

print("\nSection 2: NHL game API URL built.")

# %%
# =========================================================
# 3. Download and parse the API response
# =========================================================
#
# WHAT:
# Request the endpoint, verify that it succeeded, and parse the JSON.
#
# WHY:
# We need the Python representation of the response before pandas can flatten
# the nested play records.
#
# PYTHON NOTES:
# - requests.get(): sends the request.
# - raise_for_status(): stops on HTTP errors.
# - response.json(): converts JSON text into Python dictionaries/lists.
# =========================================================

response = requests.get(url, timeout=30)
response.raise_for_status()

data = response.json()

print("\nSection 3: API response downloaded and parsed.")

# %%
# =========================================================
# 4. Flatten the play-by-play events
# =========================================================
#
# WHAT:
# Expand nested dictionaries inside each play into regular DataFrame columns.
#
# WHY:
# NHL play records contain useful information inside nested objects such as
# "periodDescriptor" and "details". Flat columns are much easier to filter,
# summarize, join, and model later.
#
# PYTHON NOTES:
# - pd.json_normalize(): converts nested JSON-like records into a flat table.
# - sep="_": joins nested key names with underscores.
#
# EXAMPLE:
#   details = {"xCoord": 72, "yCoord": -8}
#
# becomes columns:
#   details_xCoord
#   details_yCoord
# =========================================================

plays_flat = pd.json_normalize(data["plays"], sep="_")

print("\nSection 4: Play-by-play events flattened into a DataFrame.")

# %%
# =========================================================
# 5. Inspect the flattened DataFrame
# =========================================================
#
# WHAT:
# Look at example rows, all discovered columns, and the table dimensions.
#
# WHY:
# API exploration should happen before we assume which fields exist. This is
# especially important with semi-structured JSON where fields can vary by event.
#
# PYTHON NOTES:
# - DataFrame.head(): previews rows.
# - DataFrame.columns: contains the column labels.
# - DataFrame.shape: gives (rows, columns).
# =========================================================

print("First five flattened plays:")
print(plays_flat.head())

print("\nColumns:")
print(plays_flat.columns.tolist())

print("\nShape:")
print(plays_flat.shape)

print("\nSection 5: Flattened DataFrame inspected.")

# %%
# =========================================================
# 6. Inspect a smaller set of hockey-relevant columns
# =========================================================
#
# WHAT:
# Build a readable subset containing event, period, situation, location,
# shooter, and goalie fields.
#
# WHY:
# The full flattened table can be wide. A focused subset helps us understand
# which columns are immediately useful for hockey analysis.
#
# PYTHON NOTES:
# - list comprehension: builds a new list from another list.
# - "if col in plays_flat.columns": safely keeps only fields that actually
#   exist in this game's response.
# - DataFrame[columns]: selects a group of columns by name.
#
# NHL DATA NOTE:
# Not every possible `details_*` field appears in every game because many
# fields are event-specific.
# =========================================================

useful_columns = [
    "eventId",
    "periodDescriptor_number",
    "timeInPeriod",
    "timeRemaining",
    "situationCode",
    "typeDescKey",
    "details_eventOwnerTeamId",
    "details_xCoord",
    "details_yCoord",
    "details_zoneCode",
    "details_shotType",
    "details_shootingPlayerId",
    "details_goalieInNetId",
]

useful_columns = [
    col
    for col in useful_columns
    if col in plays_flat.columns
]

useful_plays = plays_flat[useful_columns].copy()

print("\nSelected hockey columns:")
print(useful_plays.head(20))

print("\nSection 6: Hockey-relevant columns selected and inspected.")
