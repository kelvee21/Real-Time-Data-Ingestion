"""Per batch logs: my own numbers from write_batch and spark's progress report.

both go to metrics/*.jsonl, one json object per line, so perf_report (and
pandas, if i want) can read them back without parsing console output.
"""
import json
from pathlib import Path
from typing import Any

from pyspark.sql.streaming.listener import (
    QueryIdleEvent,
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
    StreamingQueryListener,
)

from src.config import METRICS_DIR

Record = dict[str, Any]


def log_jsonl(name: str, record: Record, metrics_dir: Path = METRICS_DIR) -> None:
    metrics_dir.mkdir(parents=True, exist_ok=True)
    with open(metrics_dir / name, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def progress_record(progress: Record) -> Record:
    """Keep the fields of spark's progress json that the performance report uses."""
    durations: Record = progress.get("durationMs", {})
    return {
        "batch_id": progress["batchId"],
        "timestamp": progress["timestamp"],
        "num_input_rows": progress["numInputRows"],
        "input_rows_per_s": progress.get("inputRowsPerSecond"),
        "processed_rows_per_s": progress.get("processedRowsPerSecond"),
        "trigger_ms": durations.get("triggerExecution"),
        "add_batch_ms": durations.get("addBatch"),
    }


class ProgressLogger(StreamingQueryListener):
    """Saves spark's own progress report for every micro-batch (rates, durations)."""

    def __init__(self, metrics_dir: Path = METRICS_DIR) -> None:
        super().__init__()
        self.metrics_dir = metrics_dir

    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        print(f"query started: id={event.id}", flush=True)

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        progress: Record = json.loads(event.progress.json)
        log_jsonl("progress.jsonl", progress_record(progress), self.metrics_dir)

    def onQueryIdle(self, event: QueryIdleEvent) -> None:
        pass

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        print(f"query stopped: exception={event.exception}", flush=True)
