"""Part 1: write csv files of fake e-commerce events into a folder, one file every few seconds.

    python -m src.data_generator                          # 100 rows every 2 s, forever (ctrl+c)
    python -m src.data_generator --rows 500 --every 1 --batches 60
    python -m src.data_generator --bad-rate 0.2           # 20% of rows get a broken field

(`python data_generator.py` does the same, the brief wants that file name.)

each file is written under a temp name starting with "_" and then renamed.
spark skips files whose names start with "_" or ".", so it never reads a
half written file. the rename is atomic: the file is there complete or not at all.
"""
import argparse
import csv
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import INPUT_DIR

FIELDS = ["event_id", "event_time", "user_id", "session_id", "event_type",
          "product_id", "category", "price", "quantity"]

# category -> (lowest price, highest price)
CATEGORIES = {"electronics": (50, 1500), "books": (5, 60), "fashion": (10, 250),
              "home": (15, 400), "sports": (10, 600)}
EVENT_TYPES = ["view", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [75, 17, 8]          # most visits are views, few end in a purchase
BAD_VALUES = ["", "-5", "unknown", "NaN"]
SPOILABLE = ["price", "event_type", "user_id", "event_time"]

# one csv row. values are Any because spoil() can put text where a number was
Event = dict[str, Any]


def make_event() -> Event:
    category = random.choice(list(CATEGORIES))
    low, high = CATEGORIES[category]
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    return {
        "event_id": str(uuid.uuid4()),
        # utc with milliseconds, e.g. 2026-11-04 09:42:17.123
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "user_id": random.randint(1, 500),
        "session_id": uuid.uuid4().hex[:12],
        "event_type": event_type,
        "product_id": random.randint(1000, 1999),
        "category": category,
        "price": round(random.uniform(low, high), 2),
        # a view buys nothing, so 0. the db allows >= 0
        "quantity": 0 if event_type == "view" else random.randint(1, 3),
    }


def spoil(event: Event) -> Event:
    """Break one field so the spark job's cleaning rules have something to catch."""
    field = random.choice(SPOILABLE)
    event[field] = random.choice(BAD_VALUES)
    return event


def write_file(out_dir: Path, rows: int, bad_rate: float) -> tuple[str, int]:
    """Write one csv of `rows` events, about bad_rate of them spoiled. returns (name, spoiled)."""
    events = [make_event() for _ in range(rows)]
    bad = 0
    for e in events:
        if random.random() < bad_rate:
            spoil(e)
            bad += 1
    name = f"events_{datetime.now():%Y%m%d_%H%M%S_%f}.csv"
    tmp_path = out_dir / ("_" + name)
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(events)
    os.replace(tmp_path, out_dir / name)     # atomic rename
    return name, bad


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=(__doc__ or " ").splitlines()[0])
    ap.add_argument("--out", type=Path, default=INPUT_DIR)
    ap.add_argument("--rows", type=int, default=100, help="rows per file")
    ap.add_argument("--every", type=float, default=2.0, help="seconds between files")
    ap.add_argument("--batches", type=int, default=0, help="files to write; 0 = forever")
    ap.add_argument("--bad-rate", type=float, default=0.02, help="share of rows to spoil")
    ap.add_argument("--seed", type=int, help="fix the random numbers, for repeatable tests")
    args = ap.parse_args(argv)
    out: Path = args.out
    rows: int = args.rows
    batches: int = args.batches

    if args.seed is not None:
        random.seed(args.seed)
    out.mkdir(parents=True, exist_ok=True)
    written = 0
    try:
        while batches == 0 or written < batches:
            name, bad = write_file(out, rows, args.bad_rate)
            written += 1
            print(f"{name}: {rows} rows ({bad} spoiled)", flush=True)
            time.sleep(args.every)
    except KeyboardInterrupt:
        pass
    print(f"done: {written} files, {written * rows} rows")


if __name__ == "__main__":
    main()
