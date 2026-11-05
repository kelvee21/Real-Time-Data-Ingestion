import json
from pathlib import Path

from src import metrics
from src.metrics import Record

PROGRESS: Record = {
    "batchId": 3, "timestamp": "2026-11-04T10:50:05.000Z", "numInputRows": 100,
    "inputRowsPerSecond": 20.0, "processedRowsPerSecond": 55.5,
    "durationMs": {"triggerExecution": 1800, "addBatch": 1500, "getBatch": 3},
}


def test_progress_record_keeps_the_report_fields() -> None:
    assert metrics.progress_record(PROGRESS) == {
        "batch_id": 3, "timestamp": "2026-11-04T10:50:05.000Z", "num_input_rows": 100,
        "input_rows_per_s": 20.0, "processed_rows_per_s": 55.5,
        "trigger_ms": 1800, "add_batch_ms": 1500,
    }


def test_progress_record_without_durations() -> None:
    p: Record = {k: v for k, v in PROGRESS.items() if k != "durationMs"}
    r = metrics.progress_record(p)
    assert r["trigger_ms"] is None and r["add_batch_ms"] is None


def test_log_jsonl_appends_one_line_per_record(tmp_path: Path) -> None:
    metrics.log_jsonl("x.jsonl", {"a": 1}, tmp_path / "m")
    metrics.log_jsonl("x.jsonl", {"a": 2}, tmp_path / "m")
    lines = (tmp_path / "m" / "x.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["a"] for line in lines] == [1, 2]
