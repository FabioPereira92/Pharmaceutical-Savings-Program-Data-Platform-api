import os
import sqlite3
import pytest
from pathlib import Path
from fastapi.testclient import TestClient
import sys

# Ensure project root is on sys.path for imports
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _make_api_keys_db(path: Path):
    conn = sqlite3.connect(str(path))
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE api_keys (
      api_key TEXT PRIMARY KEY,
      client_name TEXT NOT NULL,
      created_at TEXT NOT NULL,
      rate_limit INTEGER DEFAULT 5,
      is_active INTEGER DEFAULT 1,
      revoked_at TEXT,
      last_used_at TEXT
    );
    """)
    cur.execute("INSERT INTO api_keys(api_key, client_name, created_at, rate_limit, is_active) VALUES(?,?,?,?,1);",
                ("validkey", "test-client", "2026-01-01T00:00:00Z", 5))
    conn.commit()
    conn.close()

def _make_coupons_db(path: Path):
    conn = sqlite3.connect(str(path))
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE ai_page_extractions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ai_extraction TEXT NOT NULL
    );
    """)
    cur.execute("INSERT INTO ai_page_extractions(ai_extraction) VALUES(?);", ("Eliquis - extraction A",))
    cur.execute("INSERT INTO ai_page_extractions(ai_extraction) VALUES(?);", ("ELIQUIS - extraction B",))
    conn.commit()
    conn.close()

@pytest.fixture()
def client(tmp_path, monkeypatch):
    api_keys_db = tmp_path / "api_keys.db"
    coupons_db = tmp_path / "goodrx_coupons.db"
    _make_api_keys_db(api_keys_db)
    _make_coupons_db(coupons_db)

    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("SEED_DEV_KEY", "false")
    monkeypatch.setenv("API_KEYS_DB_PATH", str(api_keys_db))
    monkeypatch.setenv("COUPONS_DB_PATH", str(coupons_db))
    monkeypatch.setenv("ADMIN_API_KEY", "adminkey")

    # Import after env vars set
    import importlib
    import main
    importlib.reload(main)

    return TestClient(main.app)
