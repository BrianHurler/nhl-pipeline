# AGENTS.md — NHL Pipeline Coding Guidelines

These instructions apply to all scripts and code added to this repository.

## Primary objective

Write straightforward, readable Python that someone unfamiliar with the project can understand while reading from top to bottom. Prefer clarity and minimalism over abstraction or cleverness.

## 1. Prefer inline code

- Write code inline by default, following the actual order of operations.
- Avoid creating a function just to wrap a short block or make the code look more elegant.
- Introduce functions only when there is meaningful reuse, significant complexity, or a clear readability benefit.
- Avoid unnecessary classes, helpers, abstractions, or configuration layers.

## 2. Avoid extras that do not add meaningful value

- Implement only what the current task needs; skip speculative features and overengineering.
- Do not add dependency-installation checks, missing-library checks, or broad defensive wrappers where Python's normal error message is already clear.
- Do not add redundant preflight QA that merely duplicates an obvious exception or error.
- Keep validation when it detects a meaningful data-quality problem that would otherwise produce a misleading result, silent corruption, or difficult-to-diagnose downstream failure.
- Favor direct, informative exceptions over quietly catching or suppressing errors.

## 3. Keep blocks short

- Organize scripts into small, easy-to-follow blocks, typically **2–6 lines of code**.
- Avoid blocks longer than six lines when there is a natural place to separate them.
- Longer blocks are fine when splitting them would damage readability, such as a single cohesive DataFrame transformation, dictionary, or necessary control-flow unit.
- Favor clear variable names and straightforward statements over dense one-liners.

## 4. Comment every block

- Put **1–2 short comment lines immediately above each logical code block**.
- Use ordinary Python comments beginning with `#`; explain what the block does and the outcome it is intended to produce.
- Write comments in plain language for an outside reader. Do not restate each individual line verbatim.
- Do not add lengthy narrative comments or decorative section banners unless they genuinely help navigation.

Example:

```python
# Read the saved play events so we can isolate attempts on goal.
plays = pd.read_parquet(plays_file)

# Keep shot attempts and label the events that resulted in goals.
shots = plays.loc[plays["event_type"].isin(["goal", "shot-on-goal", "missed-shot"])].copy()
shots["is_goal"] = shots["event_type"].eq("goal").astype(int)
```

## 5. Work incrementally

- Make the smallest practical change that solves the requested problem.
- Preserve existing behavior unless a change has been requested or is necessary for correctness.
- Do not refactor unrelated scripts or add tooling merely because it is customary.
- Explain any genuinely necessary deviation from these preferences briefly.

## 6. Data and credentials

- Keep credentials out of source code and Git; use environment variables or AWS profiles.
- Make dataset sources and output paths clear.
- Do not omit checks needed to protect data correctness or prevent silent data loss.
