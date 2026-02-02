import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    env: str
    seed_dev_key: bool
    admin_api_key: str | None
    api_keys_db_path: Path
    coupons_db_path: Path
    redis_url: str | None


def get_settings() -> Settings:
    base_dir = Path(__file__).parent
    env = os.getenv("ENV", "dev").lower()
    seed_dev_key = os.getenv("SEED_DEV_KEY", "true" if env == "dev" else "false").lower() == "true"
    admin_api_key = os.getenv("ADMIN_API_KEY")  # required to use /admin/*
    api_keys_db_path = Path(os.getenv("API_KEYS_DB_PATH", str(base_dir / "api_keys.db")))
    coupons_db_path = Path(os.getenv("COUPONS_DB_PATH", str(base_dir / "goodrx_coupons.db")))
    redis_url = os.getenv("REDIS_URL")
    return Settings(
        env=env,
        seed_dev_key=seed_dev_key,
        admin_api_key=admin_api_key,
        api_keys_db_path=api_keys_db_path,
        coupons_db_path=coupons_db_path,
        redis_url=redis_url,
    )