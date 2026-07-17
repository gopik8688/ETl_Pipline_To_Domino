import os
import json
import logging

ALERT_DIR = os.path.join(os.getcwd(), 'domino_flows', 'alerts')
os.makedirs(ALERT_DIR, exist_ok=True)


def task_failure_alert(context: dict):
    """Simple on_failure_callback for Airflow tasks.

    Writes a small JSON file with failure context and logs the error. Replace or
    extend this with email/Slack/Teams integrations in production.
    """
    try:
        dag_id = context.get('dag').dag_id if context.get('dag') else context.get('dag_id')
        task_id = context.get('task_instance').task_id if context.get('task_instance') else context.get('task_id')
        run_id = context.get('run_id')
        ts = context.get('ts')
        exception = repr(context.get('exception'))
        payload = {
            'dag_id': dag_id,
            'task_id': task_id,
            'run_id': run_id,
            'ts': ts,
            'exception': exception,
        }
        fname = f"{dag_id}__{task_id}__{ts}.json"
        path = os.path.join(ALERT_DIR, fname)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        logging.error('Task failure captured: %s %s %s', dag_id, task_id, exception)
    except Exception as e:
        logging.exception('Failed to write alert file: %s', e)


def task_retry_alert(context: dict):
    """Simple on_retry_callback for Airflow tasks.

    Writes a small log entry indicating a retry. Extend to send notifications.
    """
    try:
        dag_id = context.get('dag').dag_id if context.get('dag') else context.get('dag_id')
        task_id = context.get('task_instance').task_id if context.get('task_instance') else context.get('task_id')
        run_id = context.get('run_id')
        ts = context.get('ts')
        logging.warning('Task retry: %s %s %s', dag_id, task_id, ts)
    except Exception:
        logging.exception('Failed in retry alert')
