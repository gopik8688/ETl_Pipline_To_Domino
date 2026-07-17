import os
import sqlite3
from datetime import datetime
from typing import Optional


# Watermark storage using SQLite by default for local runs. To use a different
# RDBMS (Postgres), set the `WATERMARK_DB_URL` env var to a DSN and replace the
# sqlite connection logic with SQLAlchemy/psycopg2 as appropriate.
DB_PATH = os.environ.get('WATERMARK_DB_URL')
if not DB_PATH:
    DB_PATH = os.path.join(os.getcwd(), 'domino_flows', 'watermarks.db')


def _conn():
    # For SQLite, DB_PATH is a file path; for production DB URLs, caller should
    # provide an adapter. Keep this simple for local testing.
    return sqlite3.connect(DB_PATH)


def _ensure_table():
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS watermarks (name TEXT PRIMARY KEY, ts TEXT)'
        )
        conn.commit()
    finally:
        conn.close()


def get_watermark(name: str) -> Optional[datetime]:
    _ensure_table()
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute('SELECT ts FROM watermarks WHERE name = ?', (name,))
        row = cur.fetchone()
        if not row:
            return None
        ts = row[0]
        try:
            return datetime.fromisoformat(ts)
        except Exception:
            return None
    finally:
        conn.close()


def set_watermark(name: str, ts: datetime):
    _ensure_table()
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO watermarks(name, ts) VALUES(?, ?) ON CONFLICT(name) DO UPDATE SET ts=excluded.ts',
            (name, ts.isoformat()),
        )
        conn.commit()
    finally:
        conn.close()
    return True
