import os
from datetime import datetime
import pandas as pd
import logging

from domino_flows.utils.validation import validate_df, validate_stage
from domino_flows.utils.incremental import get_watermark, set_watermark
from domino_flows.utils.shell_runner import run_shell_command

DATA_DIR = os.path.join(os.getcwd(), 'domino_flows', 'data')
os.makedirs(DATA_DIR, exist_ok=True)


def extract(source: str = None, **context) -> str:
    """Extract step placeholder. Replace with converted shell logic."""
    print(f"Extracting from {source or 'example source'}")
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'value': [10, 20, 30],
        'ts': [datetime.utcnow(), datetime.utcnow(), datetime.utcnow()],
    })
    path = os.path.join(DATA_DIR, 'extracted.parquet')
    df.to_parquet(path)
    return path


def transform(**context) -> str:
    """Transform step placeholder."""
    src = os.path.join(DATA_DIR, 'extracted.parquet')
    print(f"Transforming {src}")
    df = pd.read_parquet(src)
    # example transform
    df['value'] = df['value'] * 1.1
    out = os.path.join(DATA_DIR, 'transformed.parquet')
    df.to_parquet(out)
    return out


def validate(**context) -> bool:
    """Validate transformed data using pandera schema."""
    path = os.path.join(DATA_DIR, 'transformed.parquet')
    print(f"Validating {path}")
    df = pd.read_parquet(path)
    validate_stage('transform', df)
    return True


def incremental_load(**context) -> str:
    """Perform incremental filtering based on watermark."""
    in_path = os.path.join(DATA_DIR, 'transformed.parquet')
    df = pd.read_parquet(in_path)
    watermark = get_watermark('sample_pipeline')
    print(f"Current watermark: {watermark}")
    if watermark is not None:
        df = df[df['ts'] > watermark]
    if df.empty:
        print('No new rows to load')
    else:
        new_max = df['ts'].max()
        set_watermark('sample_pipeline', new_max)
    out = os.path.join(DATA_DIR, 'incremental.parquet')
    df.to_parquet(out)
    return out


def load(**context) -> bool:
    """Load step placeholder. Replace with production sink logic."""
    path = os.path.join(DATA_DIR, 'incremental.parquet')
    print(f"Loading {path} to destination (placeholder)")
    # In production, write to database / data warehouse
    return True


def run_legacy_shell_step(command: str, env: dict = None, timeout: int = 3600, **context) -> str:
    """Run a legacy bash/sh command (converted from a script).

    This helper wraps the command, captures stdout/stderr, and raises on non-zero exit
    so Airflow's retry/failure semantics apply. Replace usage with direct Python logic
    where possible for better portability.
    """
    logging.info(f"Running legacy shell command: {command}")
    result = run_shell_command(command, env=env, timeout=timeout)
    if result.returncode != 0:
        out = result.stdout.decode(errors='ignore') if result.stdout else ''
        err = result.stderr.decode(errors='ignore') if result.stderr else ''
        logging.error('Shell step failed: %s', err)
        raise RuntimeError(f"Shell command failed (rc={result.returncode})\nSTDOUT:\n{out}\nSTDERR:\n{err}")

    out_path = os.path.join(DATA_DIR, 'legacy_out.txt')
    with open(out_path, 'wb') as f:
        f.write(result.stdout or b'')
    return out_path
