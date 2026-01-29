"""
Database helper functions for goodrx_coupons.db
Provides safe, read-only access and helper queries used by the FastAPI app.
"""
from pathlib import Path
import sqlite3
from typing import Optional, Dict, List

DB_PATH = Path(__file__).parent / "goodrx_coupons.db"


def _connect():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DB_PATH}")
    uri = f"file:{DB_PATH}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def get_coupon_by_drug(drug_name: str) -> Optional[Dict]:
    """Return the first matching coupon row for a drug_name (case-insensitive).

    Keeps backward compatibility with previous helper.
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT * FROM manufacturer_coupons WHERE LOWER(drug_name)=LOWER(?) LIMIT 1;",
            (drug_name,),
        )
        row = cur.fetchone()
        if row:
            return dict(row)

        like_param = f"%{drug_name}%"
        cur.execute(
            "SELECT * FROM manufacturer_coupons WHERE LOWER(drug_name) LIKE LOWER(?) LIMIT 1;",
            (like_param,),
        )
        row = cur.fetchone()
        if row:
            return dict(row)
        return None
    finally:
        conn.close()


def list_coupons(limit: int = 50, offset: int = 0) -> List[Dict]:
    """Return a list of coupon rows as dictionaries (paginated).

    Parameters:
    - limit: maximum number of rows to return
    - offset: number of rows to skip
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT * FROM manufacturer_coupons ORDER BY id LIMIT ? OFFSET ?;",
            (limit, offset),
        )
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def count_coupons() -> int:
    """Return total number of coupons in the dataset."""
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) as c FROM manufacturer_coupons;")
        row = cur.fetchone()
        return int(row[0]) if row is not None else 0
    finally:
        conn.close()
