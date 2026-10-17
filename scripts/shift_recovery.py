"""Recover missing NHL shift charts without blocking usable analytical tables.

S3 persists consecutive failed publication dates across EC2 restarts.
A same-day manual rerun cannot consume another night. Read-only runs do not
change S3 or the retry ledger. A confirmed good response clears its issue.
"""
import json
from datetime import datetime, timezone

import requests

MAX_FAILED_NIGHTS = 4
SHIFT_URL = "https://api.nhle.com/stats/rest/en/shiftcharts"


def fetch_shift_chart(game_id):
    """Always contact NHL directly, bypassing the archived empty response."""
    response = requests.get(
        SHIFT_URL,
        params={"cayenneExp": f"gameId={game_id}", "limit": -1},
        timeout=35,
        headers={"Cache-Control": "no-cache"},
    )
    response.raise_for_status()
    result = response.json()
    if not isinstance(result, dict) or not isinstance(result.get("data"), list):
        raise ValueError("Malformed NHL shift API response")
    if not result["data"]:
        raise ValueError("NHL shift API still returns zero records")
    if any(not isinstance(row, dict) for row in result["data"]):
        raise ValueError("NHL shift API returned malformed shift rows")
    return result


def load_retry_state(s3, bucket, key):
    try:
        data = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {}
    except Exception as exc:
        # A broken state file must not reset counters silently.
        if getattr(exc, "response", {}).get("Error", {}).get("Code") == "NoSuchKey":
            return {}
        raise
    if not isinstance(data, dict) or not isinstance(data.get("issues"), dict):
        raise ValueError(f"Malformed shift retry ledger: {key}")
    return data["issues"]


def recover_shifts(s3, bucket, prefix, season, completed, read_json, list_keys,
                   publish=False):
    """Return per-game valid shift documents and a coverage summary.

    Missing/empty/malformed source: one live NHL recheck per nightly publication.
    After 4 failed distinct publication dates, keep the gap documented and stop
    special retries. Existing valid games/plays always remain publishable.
    """
    today = datetime.now(timezone.utc).date().isoformat()
    key_prefix = "/".join(filter(None, [prefix, "raw-api-data", season, "shifts"]))
    state_key = "/".join(filter(None, [prefix, "pipeline-quality", "shift-retries.json"]))
    issues = load_retry_state(s3, bucket, state_key)
    archived_keys = {
        int(key.rsplit("/", 1)[-1][:-5]): key
        for key in list_keys(key_prefix)
        if key.rsplit("/", 1)[-1].endswith(".json")
        and key.rsplit("/", 1)[-1][:-5].isdigit()
    }
    valid = {}
    empty = []
    recovered = []
    skipped = []
    failures = []

    for game_id in sorted(completed):
        game_key = archived_keys.get(game_id)
        source = None
        reason = None
        if game_key:
            try:
                source = read_json(game_key)
                records = source.get("data") if isinstance(source, dict) else None
                if not isinstance(records, list):
                    reason = "archived shift JSON malformed"
                elif not records:
                    reason = "archived shift JSON empty"
                elif any(not isinstance(row, dict) for row in records):
                    reason = "archived shift rows malformed"
            except (ValueError, TypeError, KeyError) as exc:
                reason = f"archived shift JSON unreadable: {exc}"
        else:
            reason = "archived shift JSON missing"

        if reason is None:
            valid[game_id] = source
            if str(game_id) in issues:
                print(f"Shift issue recovered from archive: game {game_id}", flush=True)
                if publish:
                    del issues[str(game_id)]
            continue

        issue = issues.get(str(game_id), {})
        failed_nights = int(issue.get("failed_nights", 0))
        last_attempt = issue.get("last_attempt_date")
        if failed_nights >= MAX_FAILED_NIGHTS:
            skipped.append(game_id)
            empty.append(game_id)
            continue
        if publish and last_attempt == today:
            # Publishing more than once in a day does not double-count failures.
            empty.append(game_id)
            continue

        try:
            source = fetch_shift_chart(game_id)
            valid[game_id] = source
            recovered.append(game_id)
            if publish:
                raw_key = game_key or f"{key_prefix}/{game_id}.json"
                s3.put_object(
                    Bucket=bucket, Key=raw_key,
                    Body=json.dumps(source).encode("utf-8"),
                    ContentType="application/json",
                )
                issues.pop(str(game_id), None)
        except (requests.RequestException, ValueError, TypeError) as exc:
            empty.append(game_id)
            failures.append((game_id, str(exc)))
            if publish:
                issue = dict(issue)
                issue["game_id"] = game_id
                issue["season"] = season
                issue["first_failure_date"] = issue.get("first_failure_date", today)
                issue["last_attempt_date"] = today
                issue["last_reason"] = str(exc)
                issue["failed_nights"] = failed_nights + 1
                issue["status"] = (
                    "escalated" if issue["failed_nights"] >= MAX_FAILED_NIGHTS
                    else "retry_pending"
                )
                issues[str(game_id)] = issue
                print(
                    f"SHIFT ISSUE game={game_id}: night {issue['failed_nights']}/4; "
                    f"{issue['status']}; {exc}", flush=True
                )

    if publish:
        s3.put_object(
            Bucket=bucket, Key=state_key,
            Body=json.dumps({"issues": issues}, indent=2, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )

    print(
        f"Shift coverage: {len(valid)}/{len(completed)} completed games; "
        f"missing={len(empty)}, recovered_from_API={len(recovered)}, "
        f"retry_limit_reached={len(skipped)}",
        flush=True,
    )
    if empty:
        print(f"Shift gaps (not zero ice time): {empty[:100]}", flush=True)
    if failures and not publish:
        print(f"Dry run: retry ledger unchanged; failures: {failures[:20]}", flush=True)
    if publish and skipped:
        print(f"ESCALATED shift gaps after 4 failed nights: {skipped}", flush=True)
    return valid
