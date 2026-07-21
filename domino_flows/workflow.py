"""
Domino Flyte Workflow

POC:
Migration of GPS/Davnic ETL Workflow
to Domino Workflows using FlyteKit.
pyflyte run --remote domino_flows/workflow.py gps_to_domino_workflow
"""

import logging

from flytekit import task, workflow

from domino_flows.operators import (
    extract,
    transform,
    validate,
    incremental_load,
    load,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ==========================================================
# Extract Task
# ==========================================================
@task
def extract_task() -> str:
    """
    Extract source data.

    Returns
    -------
    str
        Path to extracted parquet file.
    """

    logger.info("Starting Flyte Task : Extract")

    output = extract()

    logger.info("Extract Completed")

    return output


# ==========================================================
# Transform Task
# ==========================================================
@task
def transform_task(input_path: str) -> str:
    """
    Transform extracted data.

    Parameters
    ----------
    input_path
        Extracted parquet file.
    """

    logger.info("Starting Flyte Task : Transform")

    output = transform(input_path)

    logger.info("Transform Completed")

    return output


# ==========================================================
# Validate Task
# ==========================================================
@task
def validate_task(input_path: str) -> str:
    """
    Validate transformed data.
    """

    logger.info("Starting Flyte Task : Validate")

    output = validate(input_path)

    logger.info("Validation Completed")

    return output


# ==========================================================
# Incremental Load Task
# ==========================================================
@task
def incremental_load_task(input_path: str) -> str:
    """
    Apply watermark based incremental loading.
    """

    logger.info("Starting Flyte Task : Incremental Load")

    output = incremental_load(input_path)

    logger.info("Incremental Load Completed")

    return output


# ==========================================================
# Load Task
# ==========================================================
@task
def load_task(input_path: str) -> bool:
    """
    Load data into target.
    """

    logger.info("Starting Flyte Task : Load")

    status = load(input_path)

    logger.info("Load Completed")

    return status


# ==========================================================
# Workflow
# ==========================================================
@workflow
def gps_to_domino_workflow() -> bool:
    """
    GPS/Davnic → Domino Workflow

    Extract
        ↓
    Transform
        ↓
    Validate
        ↓
    Incremental Load
        ↓
    Load
    """

    extracted_file = extract_task()

    transformed_file = transform_task(
        input_path=extracted_file
    )

    validated_file = validate_task(
        input_path=transformed_file
    )

    incremental_file = incremental_load_task(
        input_path=validated_file
    )

    status = load_task(
        input_path=incremental_file
    )

    return status


# ==========================================================
# Local Execution
# ==========================================================
if __name__ == "__main__":

    print("=" * 70)
    print("Domino Flyte Workflow")
    print("=" * 70)

    result = gps_to_domino_workflow()

    print()

    print("=" * 70)
    print("Workflow Finished")
    print("=" * 70)

    print(f"Status : {result}")