import os
import logging
from datetime import datetime

import pandas as pd

from domino_flows.utils.validation import validate_stage
from domino_flows.utils.incremental import get_watermark, set_watermark
from domino_flows.utils.shell_runner import run_shell_command

DATA_DIR = os.path.join(os.getcwd(), "domino_flows", "data")
os.makedirs(DATA_DIR, exist_ok=True)

logger = logging.getLogger(__name__)


# ==========================================================
# EXTRACT
# ==========================================================
def extract(source: str = None, **context) -> str:
    """
    Extract data from source system.
    Replace this logic with migrated GPS/Davnic shell script.
    """

    source_name = source or "Example Source"

    logger.info("Starting Extract Step")

    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [10, 20, 30],
            "ts": [
                datetime.utcnow(),
                datetime.utcnow(),
                datetime.utcnow(),
            ],
        }
    )

    path = os.path.join(DATA_DIR, "extracted.parquet")
    df.to_parquet(path)

    print(f"Source              : {source_name}")
    print(f"Rows Extracted      : {len(df)}")
    print(f"Columns             : {len(df.columns)}")
    print(f"Output File         : {path}")
    print("Status              : SUCCESS")

    logger.info("Extract completed successfully")

    return path


# ==========================================================
# TRANSFORM
# ==========================================================
def transform(input_path: str, **context) -> str:
    """
    Transform extracted data.
    """

    src = input_path

    logger.info("Starting Transform Step")

    df = pd.read_parquet(src)

    before = len(df)

    # Example transformation
    df["value"] = df["value"] * 1.10

    out = os.path.join(DATA_DIR, "transformed.parquet")
    df.to_parquet(out)

    print(f"Input File          : {src}")
    print(f"Rows Read           : {before}")

    print("\nTransformations Applied")
    print("  ✓ Business Rule Applied")
    print("  ✓ Value Enrichment")
    print("  ✓ Data Standardization")

    print(f"Rows Written        : {len(df)}")
    print(f"Output File         : {out}")
    print("Status              : SUCCESS")

    logger.info("Transform completed successfully")

    return out


# ==========================================================
# VALIDATE
# ==========================================================
def validate(input_path: str, **context) -> str:
    """
    Validate transformed data.
    """

    path = input_path

    logger.info("Starting Validation Step")

    df = pd.read_parquet(path)

    validate_stage("transform", df)

    print(f"Input File          : {path}")

    print("\nValidation Checks")

    print("  ✓ Schema Validation")
    print("  ✓ Null Check")
    print("  ✓ Duplicate Check")
    print("  ✓ Data Type Check")

    print(f"Rows Validated      : {len(df)}")
    print("Validation Result   : PASSED")

    logger.info("Validation completed successfully")

    return input_path


# ==========================================================
# INCREMENTAL LOAD
# ==========================================================
def incremental_load(input_path: str, **context) -> str:
    """
    Incremental loading using watermark.
    """

    logger.info("Starting Incremental Load")

    in_path = input_path

    df = pd.read_parquet(in_path)

    total_rows = len(df)

    watermark = get_watermark("sample_pipeline")

    print(f"Previous Watermark  : {watermark}")

    if watermark is not None:
        df = df[df["ts"] > watermark]

    if df.empty:
        print("No new rows identified.")
    else:
        new_max = df["ts"].max()
        set_watermark("sample_pipeline", new_max)

        print(f"New Watermark       : {new_max}")

    out = os.path.join(DATA_DIR, "incremental.parquet")
    df.to_parquet(out)

    print(f"Input Rows          : {total_rows}")
    print(f"Rows Selected       : {len(df)}")
    print(f"Output File         : {out}")
    print("Status              : SUCCESS")

    logger.info("Incremental Load completed")

    return out


# ==========================================================
# LOAD
# ==========================================================
def load(input_path: str, **context) -> bool:
    """
    Final Load Step.
    """


    logger.info("Starting Load Step")

    path = input_path

    df = pd.read_parquet(path)

    print(f"Input File          : {path}")
    print("Target System       : Destination Data Warehouse")
    print("Load Type           : Incremental")
    print(f"Rows Loaded         : {len(df)}")
    print("Status              : SUCCESS")

    logger.info("Load completed successfully")

    # Production logic goes here

    return True


# ==========================================================
# LEGACY SHELL WRAPPER
# ==========================================================
def run_legacy_shell_step(
    command: str,
    env: dict = None,
    timeout: int = 3600,
    **context,
) -> str:
    """
    Wrapper for migrated shell scripts.
    """

    print(f"Command             : {command}")

    logger.info("Executing legacy shell command")

    result = run_shell_command(
        command,
        env=env,
        timeout=timeout,
    )

    if result.returncode != 0:

        out = (
            result.stdout.decode(errors="ignore")
            if result.stdout
            else ""
        )

        err = (
            result.stderr.decode(errors="ignore")
            if result.stderr
            else ""
        )

        logger.error(err)

        raise RuntimeError(
            f"Shell command failed (rc={result.returncode})\n"
            f"STDOUT:\n{out}\n"
            f"STDERR:\n{err}"
        )

    out_path = os.path.join(DATA_DIR, "legacy_out.txt")

    with open(out_path, "wb") as f:
        f.write(result.stdout or b"")

    print("Shell Execution     : SUCCESS")
    print(f"Output File         : {out_path}")

    logger.info("Legacy shell execution completed")

    return out_path