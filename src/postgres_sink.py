"""Part 3: the sink. each micro-batch goes to events_staging by jdbc, then
one sql statement merges it into events.

why two steps: the jdbc writer can only append. if the job dies after the
write but before spark marks the batch done, spark runs the batch again. a
plain append would then add the rows twice. the merge skips ids that are
already in events, so running a batch twice is harmless.
"""
import time
from pathlib import Path

import psycopg2
from pyspark.sql import DataFrame

from src.cleaning import COLS
from src.config import JDBC_URL, METRICS_DIR, PG
from src.metrics import log_jsonl

MERGE_SQL = f"""
    INSERT INTO events ({", ".join(COLS)})
    SELECT {", ".join(COLS)} FROM events_staging
    ON CONFLICT (event_id) DO NOTHING
"""


def truncate_staging() -> None:
    # leftovers from a try that died half way would otherwise get merged again
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE events_staging")


def append_to_staging(batch_df: DataFrame) -> None:
    (batch_df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "events_staging")
        .option("user", PG["user"])
        .option("password", PG["password"])
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())


def merge_staging() -> int:
    """staging -> events in one transaction. returns how many rows were new."""
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute(MERGE_SQL)
        inserted = cur.rowcount
        cur.execute("TRUNCATE events_staging")
    return inserted


def write_batch(batch_df: DataFrame, batch_id: int, metrics_dir: Path = METRICS_DIR) -> None:
    """Runs once per micro-batch, on the driver, with that batch as a normal DataFrame."""
    started = time.time()
    batch_df = batch_df.select(*COLS).persist()      # used twice: count + write
    try:
        rows = batch_df.count()
        if rows == 0:
            return
        truncate_staging()
        append_to_staging(batch_df)
        staged = time.time()
        inserted = merge_staging()
    finally:
        batch_df.unpersist()

    done = time.time()
    log_jsonl("batches.jsonl", {
        "batch_id": batch_id, "rows": rows, "inserted": inserted,
        "skipped_duplicates": rows - inserted,
        "jdbc_ms": round((staged - started) * 1000), "merge_ms": round((done - staged) * 1000),
        "at": time.strftime("%Y-%m-%dT%H:%M:%S")}, metrics_dir)
    print(f"batch {batch_id}: {rows} rows, {inserted} inserted, "
          f"{round(done - started, 1)} s", flush=True)
