"""Every setting of the pipeline, read once from env vars / .env.

the generator, the streaming job, perf_report and the tests all import from
here so they never disagree on a folder or a table. relative folders in .env
are taken from the lab folder, not from where i happen to run python.
"""
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def lab_path(value: str) -> Path:
    """data/incoming -> <lab>/data/incoming. absolute paths stay as they are."""
    p = Path(value)
    return p if p.is_absolute() else ROOT / p


# keyword arguments for psycopg2.connect(). port is an int, the rest are text.
# the password only ever comes from the env, never from a file in the repo.
PG: dict[str, Any] = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "ecommerce"),
    "user": os.getenv("PGUSER", "spark_app"),
    "password": os.getenv("PGPASSWORD", ""),
}
JDBC_URL = f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}"
JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"

INPUT_DIR = lab_path(os.getenv("INPUT_DIR", "data/incoming"))
CHECKPOINT_DIR = lab_path(os.getenv("CHECKPOINT_DIR", "checkpoint/events"))
METRICS_DIR = lab_path(os.getenv("METRICS_DIR", "metrics"))
TRIGGER = os.getenv("TRIGGER", "5 seconds")
MAX_FILES = int(os.getenv("MAX_FILES", "1"))
