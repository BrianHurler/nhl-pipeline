# %% [markdown]
# 11_sync_to_s3.py
#
# PURPOSE
# -------
# Upload the curated NHL Parquet warehouse to Amazon S3.
#
# By default, only data/curated is uploaded. The raw JSON archive can also be
# included with --include-raw.
#
# UNIT OF WORK
# ------------
# One upload operation = one local file copied to one S3 object key.

# %%
# =========================================================
# 1. Import libraries
# =========================================================
#
# WHAT:
# Load command-line, filesystem, Jupyter-detection, and AWS tools.
#
# WHY:
# We need to locate local files, parse optional terminal arguments, and upload
# them through the AWS SDK for Python.
#
# PYTHON NOTES:
# - boto3: AWS SDK for Python.
# - pathlib.Path: filesystem paths.
# - sys.modules: lets us detect Jupyter Interactive execution.
# =========================================================

import argparse
from pathlib import Path
import sys

import boto3

print("\nSection 1: Libraries loaded.")


# %%
# =========================================================
# 2. Configure local project paths
# =========================================================
#
# WHAT:
# Define the curated warehouse and optional raw-data directories.
#
# WHY:
# These are the only local trees this script is allowed to upload.
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CURATED_DIR = PROJECT_ROOT / "data" / "curated"
RAW_DIR = PROJECT_ROOT / "data" / "raw"

print("\nSection 2: Local S3-sync paths configured.")


# %%
# =========================================================
# 3. Parse S3 upload arguments
# =========================================================
#
# WHAT:
# Read bucket, prefix, AWS profile/region, raw-data inclusion, and dry-run flags.
#
# WHY:
# Upload destination and credentials differ by machine/environment, so they
# should remain runtime options rather than hard-coded values.
#
# PYTHON NOTES:
# - required=True: forces an argument during normal terminal execution.
# - action="store_true": creates Boolean command flags.
# - parse_known_args(): avoids Jupyter kernel arguments causing SystemExit.
#
# INTERACTIVE NOTE:
# A required --bucket makes sense in the terminal, but in Jupyter there is no
# command line to supply it. For interactive use, edit INTERACTIVE_BUCKET below.
# =========================================================

INTERACTIVE_BUCKET = None
INTERACTIVE_PREFIX = "nhl-data-research"
INTERACTIVE_PROFILE = None
INTERACTIVE_REGION = None
INTERACTIVE_INCLUDE_RAW = False
INTERACTIVE_DRY_RUN = True

parser = argparse.ArgumentParser(
    description="Upload curated NHL parquet warehouse to S3."
)

parser.add_argument(
    "--bucket",
    required="ipykernel" not in sys.modules,
    help="Destination S3 bucket name.",
)

parser.add_argument(
    "--prefix",
    default="nhl-data-research",
    help="S3 key prefix. Default: nhl-data-research",
)

parser.add_argument(
    "--profile",
    help="Optional local AWS profile name.",
)

parser.add_argument(
    "--region",
    help="Optional AWS region override.",
)

parser.add_argument(
    "--include-raw",
    action="store_true",
    help="Also upload raw JSON archive under <prefix>/raw/.",
)

parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Print uploads without sending them.",
)

if "ipykernel" in sys.modules:
    args, jupyter_extra_args = parser.parse_known_args()

    args.bucket = INTERACTIVE_BUCKET
    args.prefix = INTERACTIVE_PREFIX
    args.profile = INTERACTIVE_PROFILE
    args.region = INTERACTIVE_REGION
    args.include_raw = INTERACTIVE_INCLUDE_RAW
    args.dry_run = INTERACTIVE_DRY_RUN
else:
    args = parser.parse_args()
    jupyter_extra_args = []

print("\nSection 3: S3 upload arguments prepared.")


# %%
# =========================================================
# 4. Validate the upload source and create the AWS client
# =========================================================
#
# WHAT:
# Confirm the curated warehouse exists and initialize boto3.
#
# WHY:
# Failing early is safer than creating an AWS session before we know there is
# anything valid to upload.
#
# PYTHON NOTES:
# - **session_kwargs: expands a dictionary into keyword arguments.
# - boto3.Session(): creates an AWS session using the selected profile/region.
# - session.client("s3"): creates an S3 service client.
# =========================================================

if not CURATED_DIR.exists():
    raise FileNotFoundError(
        f"Curated warehouse not found: {CURATED_DIR}. "
        "Run scripts/data_prep/10_build_curated_parquet.py first."
    )

if "ipykernel" in sys.modules and not args.bucket:
    print(
        "Interactive mode: set INTERACTIVE_BUCKET in Section 3 "
        "before performing an S3 sync."
    )
    s3 = None
else:
    session_kwargs = {}

    if args.profile:
        session_kwargs["profile_name"] = args.profile

    if args.region:
        session_kwargs["region_name"] = args.region

    aws_session = boto3.Session(
        **session_kwargs
    )

    s3 = aws_session.client(
        "s3"
    )

print("\nSection 4: S3 source validated and AWS client prepared.")


# %%
# =========================================================
# 5. Define the reusable directory-upload function
# =========================================================
#
# WHAT:
# Define one function that walks a local directory and mirrors its relative
# file paths beneath an S3 key prefix.
#
# WHY:
# The exact same substantial operation is used for curated data and, optionally,
# the raw archive. This is a case where a function reduces meaningful duplication.
#
# PYTHON NOTES:
# - Path.rglob("*"): recursively searches all descendants.
# - Path.relative_to(): calculates a file path relative to the upload root.
# - Path.as_posix(): produces "/" separators for S3 keys.
# - s3.upload_file(): streams a local file to S3.
# =========================================================

def upload_tree(
    s3_client,
    bucket,
    local_root,
    key_root,
    dry_run=False,
):
    files = [
        path
        for path in local_root.rglob("*")
        if path.is_file()
    ]

    uploaded = 0

    print(
        f"{local_root}: "
        f"{len(files):,} files"
    )

    for file_number, path in enumerate(
        files,
        start=1,
    ):
        relative_path = (
            path
            .relative_to(local_root)
            .as_posix()
        )

        s3_key = (
            f"{key_root.rstrip('/')}/"
            f"{relative_path}"
        )

        print(
            f"[{file_number}/{len(files)}] "
            f"s3://{bucket}/{s3_key}"
        )

        if not dry_run:
            s3_client.upload_file(
                str(path),
                bucket,
                s3_key,
            )

        uploaded += 1

    return uploaded

print("\nSection 5: Reusable S3 directory-upload function defined.")


# %%
# =========================================================
# 6. Upload the curated warehouse
# =========================================================
#
# WHAT:
# Mirror data/curated beneath <prefix>/curated in S3.
#
# WHY:
# This is the query-ready warehouse that should be available to cloud tools.
# =========================================================

if s3 is None:
    curated_file_count = 0
    print("SKIP: no S3 client because INTERACTIVE_BUCKET is not set.")
else:
    curated_file_count = upload_tree(
        s3,
        args.bucket,
        CURATED_DIR,
        f"{args.prefix}/curated",
        dry_run=args.dry_run,
    )

print("\nSection 6: Curated warehouse upload step complete.")


# %%
# =========================================================
# 7. Optionally upload the raw JSON archive
# =========================================================
#
# WHAT:
# Upload data/raw only when --include-raw (or its interactive setting) is enabled.
#
# WHY:
# Raw files are much larger and are not required for ordinary warehouse querying,
# so uploading them should remain an explicit choice.
# =========================================================

raw_file_count = 0

if args.include_raw:
    if s3 is None:
        print("SKIP raw upload: no S3 client is available.")
    elif not RAW_DIR.exists():
        print(f"SKIP raw upload: {RAW_DIR} does not exist.")
    else:
        raw_file_count = upload_tree(
            s3,
            args.bucket,
            RAW_DIR,
            f"{args.prefix}/raw",
            dry_run=args.dry_run,
        )

print("\nSection 7: Optional raw-data upload step complete.")


# %%
# =========================================================
# 8. Print S3 sync summary
# =========================================================
#
# WHAT:
# Report how many files were processed in each local tree.
#
# WHY:
# This gives us a simple audit of what the sync attempted.
# =========================================================

total_file_count = (
    curated_file_count
    + raw_file_count
)

if args.dry_run:
    print("\nDry run complete.")
else:
    print("\nS3 sync complete.")

print(f"Curated files processed: {curated_file_count:,}")
print(f"Raw files processed: {raw_file_count:,}")
print(f"Total files processed: {total_file_count:,}")

print("\nSection 8: S3-sync summary complete.")
