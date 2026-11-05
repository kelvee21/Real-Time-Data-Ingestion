import json
from pathlib import Path

import pytest

from src import perf_report as pr
from src.metrics import Record


def batch(ts: str, rows: int, trig: int, rate: float | None) -> Record:
    return {"batch_id": 0, "timestamp": ts, "num_input_rows": rows,
            "processed_rows_per_s": rate, "trigger_ms": trig}


def write(path: Path, *records: Record) -> Path:
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")
    return path


def test_load_progress_keeps_window_and_skips_empty(tmp_path: Path) -> None:
    f = write(tmp_path / "p.jsonl",
              batch("2026-11-04T10:49:59.000Z", 100, 900, 50.0),   # before the window
              batch("2026-11-04T10:50:05.000Z", 100, 1000, 50.0),
              batch("2026-11-04T10:50:10.000Z", 0, 30, None),      # empty trigger
              batch("2026-11-04T10:50:15.000Z", 200, 2000, 100.0))
    got = pr.load_progress(f, pr.parse_utc("2026-11-04T10:50:00Z"), pr.parse_utc("2026-11-04T10:51:00Z"))
    assert [b["num_input_rows"] for b in got] == [100, 200]


def test_summarise() -> None:
    s = pr.summarise([batch("t", 100, 1000, 50.0), batch("t", 200, 2000, None)])
    assert (s.batches, s.rows, s.max_batch_ms) == (2, 300, 2000)
    assert s.mean_batch_ms == 1500
    assert s.mean_rows_per_s == 50.0     # the batch with no rate is left out


def test_summarise_nothing_raises() -> None:
    with pytest.raises(ValueError):
        pr.summarise([])


def test_parse_utc_accepts_z() -> None:
    assert pr.parse_utc("2026-11-04T10:50:00Z").isoformat() == "2026-11-04T10:50:00+00:00"
