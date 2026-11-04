"""Sum up one performance run: spark's progress log + end to end latency from postgres.

    python -m src.perf_report --since 2026-11-04T10:50:00Z [--until ...] [--label "50 rows/s"]

--since/--until are utc. reads metrics/progress.jsonl (written by the
listener in src.metrics) and the events table (ingested_at - event_time = latency).
"""
import argparse
import json
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

from src.config import METRICS_DIR, PG
from src.metrics import Record

LATENCY_SQL = """
    SELECT count(*),
           percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at - event_time)),
           percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at - event_time)),
           max(extract(epoch FROM ingested_at - event_time)),
           count(*) / greatest(extract(epoch FROM max(ingested_at) - min(ingested_at)), 1)
    FROM events WHERE ingested_at BETWEEN %s AND %s"""


@dataclass
class SparkSummary:
    batches: int
    rows: int
    mean_batch_ms: float
    max_batch_ms: int
    mean_rows_per_s: float


def parse_utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def load_progress(path: Path, since: datetime, until: datetime) -> list[Record]:
    """Non-empty batches whose spark timestamp is inside [since, until]."""
    batches: list[Record] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            p: Record = json.loads(line)
            if p["num_input_rows"] and since <= parse_utc(p["timestamp"]) <= until:
                batches.append(p)
    return batches


def summarise(batches: list[Record]) -> SparkSummary:
    if not batches:
        raise ValueError("no non-empty batches in that window")
    trig: list[int] = [b["trigger_ms"] for b in batches]
    proc: list[float] = [b["processed_rows_per_s"] for b in batches if b["processed_rows_per_s"]]
    return SparkSummary(
        batches=len(batches),
        rows=sum(b["num_input_rows"] for b in batches),
        mean_batch_ms=statistics.mean(trig),
        max_batch_ms=max(trig),
        mean_rows_per_s=statistics.mean(proc) if proc else 0.0,
    )


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="latency + throughput summary of one run")
    ap.add_argument("--since", required=True)
    ap.add_argument("--until", default=datetime.now(timezone.utc).isoformat())
    ap.add_argument("--label", default="")
    args = ap.parse_args(argv)
    since, until = parse_utc(args.since), parse_utc(args.until)

    try:
        spark_side = summarise(load_progress(METRICS_DIR / "progress.jsonl", since, until))
    except ValueError as e:
        raise SystemExit(str(e)) from e

    conn = psycopg2.connect(**PG)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(LATENCY_SQL, (since, until))
            row = cur.fetchone()
    finally:
        conn.close()
    if row is None:  # an aggregate with no GROUP BY always gives a row, this is for pyright
        raise SystemExit("no result from the latency query")
    n, p50, p95, worst, rate = row

    print(f"run: {args.label}")
    print(f"  micro-batches with data : {spark_side.batches}")
    print(f"  rows read by Spark      : {spark_side.rows}")
    print(f"  batch duration (ms)     : mean {spark_side.mean_batch_ms:.0f}, max {spark_side.max_batch_ms}")
    print(f"  processed rows/s (Spark): mean {spark_side.mean_rows_per_s:.0f}")
    print(f"  rows landed in Postgres : {n}  ({float(rate):.1f} rows/s)")
    print(f"  latency p50 / p95 / max : {p50:.1f} s / {p95:.1f} s / {float(worst):.1f} s")


if __name__ == "__main__":
    main()
