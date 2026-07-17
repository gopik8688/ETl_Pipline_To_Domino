Domino Flows — Airflow DAG Templates

This folder contains a scaffold to convert multi-step ETL pipelines (from GPS/Davnic) into Domino Flows using Airflow DAGs.

Contents:
- `dags/sample_pipeline_dag.py`: example DAG mapping tasks to operators
- `operators.py`: Python callables replacing bash scripts
- `utils/validation.py`: data validation helpers (uses `pandera`)
- `utils/incremental.py`: simple watermark storage for incremental loads
- `requirements.txt`: dependencies for local testing

Quick start (local testing):

1. Create a virtualenv and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate   # or .venv\\Scripts\\activate on Windows
pip install -r domino_flows/requirements.txt
```

2. Initialize Airflow and run scheduler + webserver (for local testing):

```bash
export AIRFLOW_HOME=$(pwd)/.airflow  # Windows: set AIRFLOW_HOME=%cd%\\.airflow
airflow db init
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
airflow scheduler &
airflow webserver
```

3. Copy this folder into your Airflow DAGs folder or set `AIRFLOW__CORE__DAGS_FOLDER`.

Notes:
- Replace placeholder logic in `operators.py` with your converted bash steps.
- Validation uses `pandera`; you can swap in Great Expectations if preferred.
 - Validation uses `pandera`; you can swap in Great Expectations if preferred.
 - Incremental watermark is stored in a local SQLite DB by default (`domino_flows/watermarks.db`).
	 Set the `WATERMARK_DB_URL` env var to use a different DB path or a production DSN.

Local quick-run (without Airflow):

```bash
python -m domino_flows.run_local
```

This runs the pipeline steps sequentially using the same operator functions so you can validate behavior before deploying to Domino.
