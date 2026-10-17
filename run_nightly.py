"""Nightly NHL pipeline: download raw JSON, then publish active-season Parquets.

Run from EC2 with NHL_S3_BUCKET and optional NHL_S3_PREFIX configured.
The downloader must succeed before any Parquet publication is attempted.
"""
import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parent
python = sys.executable

steps = [
    [python, "-u", str(root / "scripts" / "01_download_nhl_data.py"), "--nightly", "--workers", "8"],
    [python, "-u", str(root / "scripts" / "02_build_season_parquets.py"), "--active-season", "--publish"],
]

for command in steps:
    print(f"Running: {' '.join(command)}", flush=True)
    subprocess.run(command, cwd=root, check=True)

print("Nightly NHL downloader + Parquet publication completed successfully.", flush=True)
