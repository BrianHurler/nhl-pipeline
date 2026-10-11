# %% [markdown]
# 01_test_nhl_api.py
#
# PURPOSE
# -------
# Make one simple request to the NHL play-by-play API and inspect the result.
#
# This is intentionally a small learning script. It introduces the basic path
# from an HTTP request -> JSON -> pandas DataFrame without hiding those steps
# inside helper functions.
#
# UNIT OF OBSERVATION
# -------------------
# The final `plays` DataFrame has one row per play-by-play event for one game.

# %%
# =========================================================
# 1. Import the libraries we need
# =========================================================
#
# WHAT:
# Load the two external libraries used in this script.
#
# WHY:
# requests handles the web request; pandas turns the returned play list into
# a table that is easy to inspect and analyze.
#
# PYTHON NOTES:
# - import: makes code from another package available in this script.
# - "as pd": gives pandas its conventional short alias.
# =========================================================

import requests
import pandas as pd

print("\nSection 1: Libraries loaded.")

# %%
# =========================================================
# 2. Choose one NHL game and build its API URL
# =========================================================
#
# WHAT:
# Select one NHL game ID and insert it into the play-by-play endpoint.
#
# WHY:
# Starting with a single known game makes it easy to understand the API
# structure before we scale the same idea to many games in later scripts.
#
# PYTHON NOTES:
# - f"...{game_id}...": an f-string inserts a Python value into text.
# =========================================================

game_id = 2023020204

url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"

print("\nSection 2: NHL game API URL built.")

# %%
# =========================================================
# 3. Request the play-by-play data
# =========================================================
#
# WHAT:
# Send an HTTP GET request to the NHL API.
#
# WHY:
# The API response is the raw source for the play-by-play data we will inspect.
#
# PYTHON NOTES:
# - requests.get(): sends an HTTP GET request.
# - timeout=30: prevents the request from hanging forever.
# - response.raise_for_status(): raises an error for unsuccessful HTTP status
#   codes such as 404 or 500 instead of silently continuing.
# =========================================================

response = requests.get(url, timeout=30)
response.raise_for_status()

print("\nSection 3: Play-by-play data requested successfully.")

# %%
# =========================================================
# 4. Convert the JSON response into Python objects
# =========================================================
#
# WHAT:
# Decode the API's JSON response.
#
# WHY:
# JSON is the format returned over the web. response.json() converts it into
# ordinary Python dictionaries and lists that we can inspect and manipulate.
#
# PYTHON NOTES:
# - response.json(): parses JSON into Python objects.
# - dict.keys(): shows the top-level keys available in a dictionary.
# =========================================================

data = response.json()

print("Top-level JSON keys:")
print(data.keys())

print("\nSection 4: JSON response converted to Python objects.")

# %%
# =========================================================
# 5. Turn the play list into a pandas DataFrame
# =========================================================
#
# WHAT:
# Convert data["plays"] from a list of event dictionaries into a table.
#
# WHY:
# A DataFrame gives us rows and columns, which is the structure we will use
# for most analysis throughout this repository.
#
# PYTHON NOTES:
# - pd.DataFrame(): constructs a DataFrame from records such as dictionaries.
# - .head(): previews the first rows.
# - .columns.tolist(): returns the column names as a normal Python list.
# - .shape: returns (number_of_rows, number_of_columns).
#
# INTERACTIVE TIP:
# After running this section, inspect `plays` in the Jupyter Variables pane
# and open it in the Data Viewer.
# =========================================================

plays = pd.DataFrame(data["plays"])

print("\nFirst five plays:")
print(plays.head())

print("\nColumns:")
print(plays.columns.tolist())

print("\nShape:")
print(plays.shape)

print("\nSection 5: Plays DataFrame created and inspected.")
