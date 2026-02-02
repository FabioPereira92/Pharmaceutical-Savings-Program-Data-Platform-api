from __future__ import annotations
from pathlib import Path
import sqlite3
import json
from typing import Optional, Dict, List, Any

from config import get_settings

settings = get_settings()
DB_PATH = settings.coupons_db_path


def _connect():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DB_PATH}")
    uri = f"file:{DB_PATH}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_table_and_column(cur: sqlite3.Cursor):
    # Verify table exists and has ai_extraction column; raise informative error otherwise
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ai_page_extractions';")
    if not cur.fetchone():
        raise sqlite3.OperationalError("Expected table 'ai_page_extractions' not found")
    cur.execute("PRAGMA table_info(ai_page_extractions);")
    cols = [r[1] for r in cur.fetchall()]
    if "ai_extraction" not in cols:
        raise sqlite3.OperationalError("Expected column 'ai_extraction' not found in ai_page_extractions")


def _parse_ai_extraction(val: Any) -> Any:
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return val
    return val


def _row_to_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    d = {k: row[k] for k in row.keys()}
    if "ai_extraction" in d:
        d["ai_extraction"] = _parse_ai_extraction(d["ai_extraction"])
    return d


def get_coupon_by_drug(drug_name: str) -> Optional[Dict[str, Any]]:
    """Return a single deterministic row from ai_page_extractions matching the drug.

    Searches the ai_extraction text using case-insensitive LIKE and returns the first row ordered by rowid.
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        _ensure_table_and_column(cur)

        like_param = f"%{drug_name}%"
        cur.execute(
            "SELECT rowid AS id, ai_extraction FROM ai_page_extractions WHERE LOWER(drug_name) LIKE LOWER(?) ORDER BY rowid LIMIT 1;",
            (like_param,),
        )
        row = cur.fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def list_coupons(limit: int = 50, offset: int = 0, drug_name: Optional[str] = None) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    try:
        _ensure_table_and_column(cur)

        if drug_name:
            like_param = f"%{drug_name}%"
            cur.execute(
                "SELECT rowid AS id, ai_extraction FROM ai_page_extractions WHERE LOWER(drug_name) LIKE LOWER(?) ORDER BY rowid LIMIT ? OFFSET ?;",
                (like_param, limit, offset),
            )
        else:
            cur.execute(
                "SELECT rowid AS id, ai_extraction FROM ai_page_extractions ORDER BY rowid LIMIT ? OFFSET ?;",
                (limit, offset),
            )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def count_coupons(drug_name: Optional[str] = None) -> int:
    conn = _connect()
    cur = conn.cursor()
    try:
        _ensure_table_and_column(cur)
        if drug_name:
            like_param = f"%{drug_name}%"
            cur.execute(
                "SELECT COUNT(*) FROM ai_page_extractions WHERE LOWER(drug_name) LIKE LOWER(?);",
                (like_param,),
            )
        else:
            cur.execute("SELECT COUNT(*) FROM ai_page_extractions;")
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()
