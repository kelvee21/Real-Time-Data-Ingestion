import csv
import random
from pathlib import Path

import pytest

from src import data_generator as gen


def test_event_has_every_field() -> None:
    assert list(gen.make_event()) == gen.FIELDS


def test_event_values_in_range() -> None:
    random.seed(1)
    for _ in range(200):
        e = gen.make_event()
        low, high = gen.CATEGORIES[e["category"]]
        assert low <= e["price"] <= high
        assert e["event_type"] in gen.EVENT_TYPES
        assert 1 <= e["user_id"] <= 500
        assert 1000 <= e["product_id"] <= 1999


def test_view_has_zero_quantity() -> None:
    random.seed(2)
    events = [gen.make_event() for _ in range(300)]
    views = [e for e in events if e["event_type"] == "view"]
    others = [e for e in events if e["event_type"] != "view"]
    assert views and others
    assert all(e["quantity"] == 0 for e in views)
    assert all(1 <= e["quantity"] <= 3 for e in others)


def test_event_ids_are_unique() -> None:
    ids = {gen.make_event()["event_id"] for _ in range(500)}
    assert len(ids) == 500


def test_spoil_breaks_one_field_with_a_bad_value() -> None:
    random.seed(3)
    e = gen.make_event()
    before = dict(e)
    gen.spoil(e)
    changed = [k for k in e if e[k] != before[k]]
    # every good value is a number, a valid type or a real time, never one of
    # the bad strings, so exactly one field differs
    assert len(changed) == 1
    assert changed[0] in gen.SPOILABLE
    assert e[changed[0]] in gen.BAD_VALUES


def test_write_file_leaves_one_complete_csv(tmp_path: Path) -> None:
    name, bad = gen.write_file(tmp_path, rows=25, bad_rate=0.0)
    files = list(tmp_path.iterdir())
    # the "_" temp file is renamed away, spark must never see it
    assert [f.name for f in files] == [name]
    assert bad == 0
    with open(files[0], newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 25
    assert list(rows[0]) == gen.FIELDS


@pytest.mark.parametrize("rate,expected", [(0.0, 0), (1.0, 40)])
def test_bad_rate_edges(tmp_path: Path, rate: float, expected: int) -> None:
    _, bad = gen.write_file(tmp_path, rows=40, bad_rate=rate)
    assert bad == expected


def test_main_writes_n_files(tmp_path: Path) -> None:
    gen.main(["--out", str(tmp_path), "--rows", "5", "--every", "0", "--batches", "3", "--seed", "7"])
    assert len(list(tmp_path.glob("events_*.csv"))) == 3
