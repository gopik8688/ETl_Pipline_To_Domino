"""Local runner to execute the pipeline tasks sequentially without Airflow.

Usage:
    python -m domino_flows.run_local

This executes `extract`, `transform`, `validate`, `incremental_load`, `load`
using the same operator functions so you can test logic locally before pushing
to Domino.
"""
import logging
import sys

from domino_flows.operators import extract, transform, validate, incremental_load, load


def main():
    logging.basicConfig(level=logging.INFO)
    try:
        print('Running extract...')
        extract()
        print('Running transform...')
        transform()
        print('Running validate...')
        validate()
        print('Running incremental_load...')
        incremental_load()
        print('Running load...')
        load()
        print('Local run completed successfully')
    except Exception as e:
        logging.exception('Local run failed: %s', e)
        sys.exit(2)


if __name__ == '__main__':
    main()
