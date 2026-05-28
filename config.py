"""
config.py — central config and DB connection factory
All other modules import from here.
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from loguru import logger

load_dotenv()


class Config:
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB   = os.getenv("POSTGRES_DB", "nyc_311")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "pipeline_user")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "your_password_here")

    NYC_API_BASE_URL = os.getenv(
        "NYC_API_BASE_URL",
        "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
    )
    NYC_API_APP_TOKEN = os.getenv("NYC_API_APP_TOKEN", "")

    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
    MAX_PAGES  = int(os.getenv("MAX_PAGES", "10"))
    LOG_LEVEL  = os.getenv("LOG_LEVEL", "INFO")

    @property
    def db_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )


config = Config()


def get_engine():
    """Return a SQLAlchemy engine connected to PostgreSQL."""
    engine = create_engine(
        config.db_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,   # test connection before use
    )
    return engine


def get_session():
    """Return a SQLAlchemy session factory."""
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


def test_connection() -> bool:
    """Quick connectivity check — returns True if DB is reachable."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✓ Database connection successful")
        return True
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False
