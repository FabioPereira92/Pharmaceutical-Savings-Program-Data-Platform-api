"""
Small helper module to store and retrieve API keys from a SQLite database file `api_keys.db`.

Schema:
CREATE TABLE IF NOT EXISTS api_keys (
    api_key TEXT PRIMARY KEY,
    client_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    rate_limit INTEGER DEFAULT 60
);

Functions:
- ensure_db_initialized(seed=True): create the DB file and table, optionally insert a sample key if table empty.
- get_key_info(api_key) -> dict | None: return row as dict if found.
"""
from pathlib import Path
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Dict

DB_PATH = Path(__file__).parent / "api_keys.db"


def _connect(read_only: bool = False):
    if read_only:
        uri = f"file:{DB_PATH}?mode=ro"
        return sqlite3.connect(uri, uri=True)
    return sqlite3.connect(str(DB_PATH))


def ensure_db_initialized(seed: bool = True) -> None:
    """Create the DB and table if missing. Optionally seed with a sample key.

    This is idempotent.
    """
    conn = _connect(read_only=False)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS api_keys (
                api_key TEXT PRIMARY KEY,
                client_name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                rate_limit INTEGER DEFAULT 60
            );
            """
        )
        conn.commit()

        # Seed a sample key if empty and seed requested
        if seed:
            cur.execute("SELECT COUNT(*) FROM api_keys;")
            row = cur.fetchone()
            count = row[0] if row is not None else 0
            if count == 0:
                sample_key = "testkey123"
                now = datetime.now(timezone.utc).isoformat()
                cur.execute(
                    "INSERT OR IGNORE INTO api_keys(api_key, client_name, created_at, rate_limit) VALUES (?, ?, ?, ?);",
                    (sample_key, "local-dev", now, 60),
                )
                conn.commit()
    finally:
        conn.close()


def get_key_info(api_key: str) -> Optional[Dict]:
    """Return the key record as a dict, or None if not found."""
    if not DB_PATH.exists():
        return None
    conn = _connect(read_only=True)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute("SELECT api_key, client_name, created_at, rate_limit FROM api_keys WHERE api_key = ? LIMIT 1;", (api_key,))
        row = cur.fetchone()
        if not row:
            return None
        return {k: row[k] for k in row.keys()}
    finally:
        conn.close()
