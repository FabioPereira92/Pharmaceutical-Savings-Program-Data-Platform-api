from __future__ import annotations
from pathlib import Path
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Dict, List
import secrets
import os

from config import get_settings

settings = get_settings()
DB_PATH = settings.api_keys_db_path
DB_URL = settings.api_keys_db_url

# Lazy import psycopg2 if needed
_psycopg2 = None


def _utc_now() -> str:
    # Use timezone-aware UTC ISO format to avoid deprecation warnings
    return datetime.now(timezone.utc).isoformat()


# ---------- SQLite helpers (existing behavior) ----------

def _sqlite_connect(read_only: bool = False):
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if read_only:
        uri = f"file:{DB_PATH}?mode=ro"
        return sqlite3.connect(uri, uri=True)
    return sqlite3.connect(str(DB_PATH))


def _sqlite_column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    cols = [r[1] for r in cur.fetchall()]
    return col in cols


def _sqlite_ensure_initialized(seed: bool = False) -> None:
    conn = _sqlite_connect(read_only=False)
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

        # Safe migrations: add lifecycle columns if missing
        if not _sqlite_column_exists(conn, "api_keys", "is_active"):
            cur.execute("ALTER TABLE api_keys ADD COLUMN is_active INTEGER DEFAULT 1;")
        if not _sqlite_column_exists(conn, "api_keys", "revoked_at"):
            cur.execute("ALTER TABLE api_keys ADD COLUMN revoked_at TEXT;")
        if not _sqlite_column_exists(conn, "api_keys", "last_used_at"):
            cur.execute("ALTER TABLE api_keys ADD COLUMN last_used_at TEXT;")
        conn.commit()

        # Optional seed: only if requested and table empty
        if seed:
            cur.execute("SELECT COUNT(*) FROM api_keys;")
            count = int(cur.fetchone()[0])
            if count == 0:
                sample_key = "testkey123"
                now = _utc_now()
                cur.execute(
                    """
                    INSERT OR IGNORE INTO api_keys(api_key, client_name, created_at, rate_limit, is_active)
                    VALUES (?, ?, ?, ?, 1);
                    """,
                    (sample_key, "local-dev", now, 60),
                )
                conn.commit()
    finally:
        conn.close()


# ---------- Postgres helpers ----------

def _ensure_psycopg2():
    global _psycopg2
    if _psycopg2 is None:
        try:
            import psycopg2
            import psycopg2.extras as extras
        except Exception as e:
            raise RuntimeError("psycopg2 is required for Postgres support: install psycopg[binary]") from e
        _psycopg2 = psycopg2
        _psycopg2_extras = extras
    return _psycopg2


def _pg_connect():
    ensure = _ensure_psycopg2()
    # psycopg2.connect will parse DATABASE_URL
    conn = _psycopg2.connect(DB_URL)
    return conn


def _pg_ensure_initialized(seed: bool = False) -> None:
    conn = _pg_connect()
    try:
        cur = conn.cursor()
        # Create table if not exists with necessary columns
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS api_keys (
                api_key TEXT PRIMARY KEY,
                client_name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                rate_limit INTEGER DEFAULT 60,
                is_active INTEGER DEFAULT 1,
                revoked_at TEXT,
                last_used_at TEXT
            );
            """
        )
        conn.commit()
        if seed:
            cur.execute("SELECT COUNT(*) FROM api_keys;")
            count = int(cur.fetchone()[0])
            if count == 0:
                sample_key = "testkey123"
                now = _utc_now()
                cur.execute(
                    "INSERT INTO api_keys(api_key, client_name, created_at, rate_limit, is_active) VALUES (%s, %s, %s, %s, 1) ON CONFLICT DO NOTHING;",
                    (sample_key, "local-dev", now, 60),
                )
                conn.commit()
    finally:
        conn.close()


# ---------- Public API (choose backend based on DB_URL) ----------

def ensure_db_initialized(seed: bool = False) -> None:
    if DB_URL:
        _pg_ensure_initialized(seed=seed)
    else:
        _sqlite_ensure_initialized(seed=seed)


def get_key_info(api_key: str) -> Optional[Dict]:
    """Return active key record dict, or None if missing/inactive/revoked."""
    if DB_URL:
        # Postgres
        conn = _pg_connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT api_key, client_name, created_at, rate_limit, is_active, revoked_at, last_used_at
                FROM api_keys
                WHERE api_key = %s
                LIMIT 1;
                """,
                (api_key,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            d = dict(zip(cols, row))
            if int(d.get("is_active") or 0) != 1:
                return None
            if d.get("revoked_at"):
                return None
            return d
        finally:
            conn.close()
    else:
        if not DB_PATH.exists():
            return None
        conn = _sqlite_connect(read_only=True)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT api_key, client_name, created_at, rate_limit, is_active, revoked_at, last_used_at
                FROM api_keys
                WHERE api_key = ?
                LIMIT 1;
                """,
                (api_key,),
            )
            row = cur.fetchone()
            if not row:
                return None
            d = {k: row[k] for k in row.keys()}
            if int(d.get("is_active") or 0) != 1:
                return None
            if d.get("revoked_at"):
                return None
            return d
        finally:
            conn.close()


def update_last_used(api_key: str) -> None:
    if DB_URL:
        conn = _pg_connect()
        try:
            cur = conn.cursor()
            cur.execute("UPDATE api_keys SET last_used_at = %s WHERE api_key = %s;", (_utc_now(), api_key))
            conn.commit()
        finally:
            conn.close()
    else:
        if not DB_PATH.exists():
            return
        conn = _sqlite_connect(read_only=False)
        try:
            cur = conn.cursor()
            cur.execute("UPDATE api_keys SET last_used_at = ? WHERE api_key = ?;", (_utc_now(), api_key))
            conn.commit()
        finally:
            conn.close()


def create_key(client_name: str, rate_limit: int = 60) -> Dict:
    api_key = secrets.token_urlsafe(32)
    now = _utc_now()
    if DB_URL:
        conn = _pg_connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO api_keys(api_key, client_name, created_at, rate_limit, is_active, revoked_at, last_used_at) VALUES (%s, %s, %s, %s, 1, NULL, NULL);",
                (api_key, client_name, now, int(rate_limit)),
            )
            conn.commit()
            return {
                "api_key": api_key,
                "client_name": client_name,
                "created_at": now,
                "rate_limit": int(rate_limit),
                "is_active": 1,
                "revoked_at": None,
                "last_used_at": None,
            }
        finally:
            conn.close()
    else:
        conn = _sqlite_connect(read_only=False)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO api_keys(api_key, client_name, created_at, rate_limit, is_active, revoked_at, last_used_at)
                VALUES (?, ?, ?, ?, 1, NULL, NULL);
                """,
                (api_key, client_name, now, int(rate_limit)),
            )
            conn.commit()
            return {
                "api_key": api_key,
                "client_name": client_name,
                "created_at": now,
                "rate_limit": int(rate_limit),
                "is_active": 1,
                "revoked_at": None,
                "last_used_at": None,
            }
        finally:
            conn.close()


def revoke_key(api_key: str) -> bool:
    if DB_URL:
        conn = _pg_connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE api_keys SET revoked_at = %s, is_active = 0 WHERE api_key = %s;",
                (_utc_now(), api_key),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()
    else:
        conn = _sqlite_connect(read_only=False)
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE api_keys SET revoked_at = ?, is_active = 0 WHERE api_key = ?;",
                (_utc_now(), api_key),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()


def set_key_active(api_key: str, active: bool) -> bool:
    if DB_URL:
        conn = _pg_connect()
        try:
            cur = conn.cursor()
            cur.execute("UPDATE api_keys SET is_active = %s WHERE api_key = %s;", (1 if active else 0, api_key))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()
    else:
        conn = _sqlite_connect(read_only=False)
        try:
            cur = conn.cursor()
            cur.execute("UPDATE api_keys SET is_active = ? WHERE api_key = ?;", (1 if active else 0, api_key))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()


def rotate_key(old_api_key: str) -> Optional[Dict]:
    # find old key info even if active; rotation revokes old
    if DB_URL:
        conn = _pg_connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT client_name, rate_limit FROM api_keys WHERE api_key = %s LIMIT 1;", (old_api_key,))
            row = cur.fetchone()
            if not row:
                return None
            client_name, rate_limit = row[0], int(row[1] or 60)
        finally:
            conn.close()

        revoke_key(old_api_key)
        return create_key(client_name=client_name, rate_limit=rate_limit)
    else:
        conn = _sqlite_connect(read_only=False)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT client_name, rate_limit FROM api_keys WHERE api_key = ? LIMIT 1;",
                (old_api_key,),
            )
            row = cur.fetchone()
            if not row:
                return None
            client_name = row["client_name"]
            rate_limit = int(row["rate_limit"] or 60)
        finally:
            conn.close()

        revoke_key(old_api_key)
        return create_key(client_name=client_name, rate_limit=rate_limit)


def list_keys(mask: bool = True) -> List[Dict]:
    if DB_URL:
        conn = _pg_connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT api_key, client_name, created_at, rate_limit, is_active, revoked_at, last_used_at
                FROM api_keys
                ORDER BY created_at DESC;
                """
            )
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            out = []
            for r in rows:
                d = dict(zip(cols, r))
                if mask:
                    k = d["api_key"]
                    d["api_key"] = ("*" * max(0, len(k) - 4)) + k[-4:]
                out.append(d)
            return out
        finally:
            conn.close()
    else:
        if not DB_PATH.exists():
            return []
        conn = _sqlite_connect(read_only=True)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT api_key, client_name, created_at, rate_limit, is_active, revoked_at, last_used_at
                FROM api_keys
                ORDER BY created_at DESC;
                """
            )
            rows = cur.fetchall()
            out = []
            for r in rows:
                d = {k: r[k] for k in r.keys()}
                if mask:
                    k = d["api_key"]
                    d["api_key"] = ("*" * max(0, len(k) - 4)) + k[-4:]
                out.append(d)
            return out
        finally:
            conn.close()
