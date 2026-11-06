# 03 — data_generator.py, Line by Line

The generator is Part 1 of the lab: it fakes an e-commerce site's click
stream by writing CSV files into a folder. This page explains every line,
and why it is built so that test results can be predicted exactly.

> **Where the code lives now.** The generator is `src/data_generator.py`. The
> root `data_generator.py` only calls its `main()`, because the brief names that
> file, so every command here still works. `tests/test_data_generator.py`
> checks the rules this page explains (ranges, quantity 0 for views, the
> atomic rename).

> **Help:** why a folder of files works as a stream source is in [02 · Batch vs streaming](02-batch-vs-streaming.md#h-why-a-folder-of-files-is-the-source).

## What the script does

```
every --every seconds, until --batches files are written:
    make --rows fake events
    spoil about --bad-rate of them (break one field each)
    write them to data/incoming/_events_<time>.csv
    rename to data/incoming/events_<time>.csv      <- Spark can see it now
    print "events_<time>.csv: 100 rows (13 spoiled)"
```

The header of the file shows the three ways to run it:

```python
python data_generator.py                          # 100 rows every 2 s, forever (Ctrl+C to stop)
python data_generator.py --rows 500 --every 1 --batches 60
python data_generator.py --bad-rate 0.2           # 20% of rows get a broken field
```

## 1. The event shape

```python
FIELDS = ["event_id", "event_time", "user_id", "session_id", "event_type",
          "product_id", "category", "price", "quantity"]
```

These nine names are the CSV header, in this order. The Spark job lists the
same nine names in `COLS`, and the `events` table has the same nine columns
plus `ingested_at`. A real row from run A:

```text
event_id,event_time,user_id,session_id,event_type,product_id,category,price,quantity
07c0a1ea-3bd2-419b-a83c-997b20edc2e8,2026-11-04 10:35:40.222,141,ccc1c2832361,view,1250,electronics,373.66,0
```

| Column | Example | Meaning |
|---|---|---|
| `event_id` | `07c0a1ea-...` | unique id of this event (a UUID, see below). The key used against duplicates |
| `event_time` | `2026-11-04 10:35:40.222` | when it happened, UTC, with milliseconds |
| `user_id` | `141` | who (1 to 500) |
| `session_id` | `ccc1c2832361` | the visit (12 hex characters) |
| `event_type` | `view` | `view`, `add_to_cart` or `purchase` |
| `product_id` | `1250` | which product (1000 to 1999) |
| `category` | `electronics` | product group |
| `price` | `373.66` | unit price, inside the category's range |
| `quantity` | `0` | 0 for a view, 1 to 3 otherwise |

## 2. make_event(): one fake event

```python
CATEGORIES = {"electronics": (50, 1500), "books": (5, 60), "fashion": (10, 250),
              "home": (15, 400), "sports": (10, 600)}
EVENT_TYPES = ["view", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [75, 17, 8]          # most visits are views; few end in a purchase
```

`CATEGORIES` is a dictionary: category name → (lowest price, highest price).
So a book never costs 900. Realistic ranges make later analysis queries
believable.

`EVENT_WEIGHTS` makes the types uneven, like a real shop: out of 100 events,
about 75 views, 17 add-to-carts and 8 purchases.

```python
def make_event() -> Event:
    category = random.choice(list(CATEGORIES))
    low, high = CATEGORIES[category]
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
```

| Call | What it does |
|---|---|
| `random.choice(list(CATEGORIES))` | pick one category, all equally likely |
| `low, high = CATEGORIES[category]` | unpack the price range |
| `random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)` | pick with the given weights. `choices` returns a **list** (it can pick several), so `[0]` takes the one item |

```python
    return {
        "event_id": str(uuid.uuid4()),
        # UTC with milliseconds, e.g. 2026-11-04 09:42:17.123
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "user_id": random.randint(1, 500),
        "session_id": uuid.uuid4().hex[:12],
        "event_type": event_type,
        "product_id": random.randint(1000, 1999),
        "category": category,
        "price": round(random.uniform(low, high), 2),
        "quantity": 0 if event_type == "view" else random.randint(1, 3),
    }
```

- **`uuid.uuid4()`** makes a *UUID*, a 128-bit random identifier such as
  `07c0a1ea-3bd2-419b-a83c-997b20edc2e8`. The chance of two being equal is
  practically zero, so the id can be created here, at the source, with no
  central counter. That matters: because the id is born with the event, a
  re-sent copy of the event carries the **same** id, and the database can
  recognise it as a duplicate. This is called a **natural key**.
- **`datetime.now(timezone.utc)`** asks for the current time **in UTC**,
  not the laptop's local time (UTC+2). `%f` prints microseconds (6 digits);
  `[:-3]` cuts the last 3, leaving milliseconds. The Spark job parses
  exactly this pattern: `'yyyy-MM-dd HH:mm:ss.SSS'`.
- **`random.randint(1, 500)`** includes both ends: 1 and 500 are possible.
- **`random.uniform(low, high)`** gives a decimal number in the range;
  `round(..., 2)` keeps cents.
- **`quantity`**: a view has no quantity, so 0; a cart or purchase gets 1–3.

## 3. spoil(): bad data on purpose

Real input is dirty. To prove the job's cleaning rules work, the generator
breaks some rows itself.

```python
BAD_VALUES = ["", "-5", "unknown", "NaN"]

def spoil(event: Event) -> Event:
    """Break one field, so the Spark job's cleaning rules have something to catch."""
    field = random.choice(["price", "event_type", "user_id", "event_time"])
    event[field] = random.choice(BAD_VALUES)
    return event
```

One spoiled row = one of four fields replaced by one of four bad values. The
key design point: **every one of the 16 combinations breaks a rule in the
job**. Here is the proof, using the rules from `clean()`
([04](04-streaming-job-line-by-line.md#h-4-clean-types-and-rules)):

| Field \ value | `""` (empty) | `"-5"` | `"unknown"` | `"NaN"` |
|---|---|---|---|---|
| `price` | read as null → fails `price >= 0` | casts to -5.00 → fails `price >= 0` | `try_cast` gives null → fails | null (a DECIMAL has no NaN) → fails |
| `event_type` | null → fails `isin(...)` | `"-5"` not in the list → fails | not in list → fails | lower-cased `"nan"`, not in list → fails |
| `user_id` | null → fails `user_id > 0` | **casts to -5 → fails `user_id > 0`** | null → fails | null → fails |
| `event_time` | null → fails `isNotNull` | `try_to_timestamp` gives null → fails | null → fails | null → fails |

Real spoiled rows from the first file of run A:

```text
31f4ae09-...,2026-11-04 10:35:40.223,-5,74e114ea1782,view,1223,home,104.57,0
d8144642-...,2026-11-04 10:35:40.223,326,f3928ceabeed,view,1854,home,unknown,0
7e1f0460-...,2026-11-04 10:35:40.223,72,c382114d781f,view,1252,home,,0
5e0c70f7-...,-5,198,99f74782a30e,view,1390,home,244.42,0
```

Because every spoil is fatal to the row, the expected output is exact:

> **rows reaching PostgreSQL = rows generated − rows spoiled**

The generator prints the spoiled count per file, so you can add them up. In
run A (20 files, `--bad-rate 0.2`): 2,000 rows, 385 spoiled, so exactly
**1,615** rows must land. They did, after a bug fix: the `user_id = "-5"` cell
in bold above is the case the first version of the job missed. That story is
in [07](07-testing-the-pipeline.md#h-tc-04-the-test-that-found-a-real-bug).

If a spoil could produce a row that is still valid (say, a random new price),
you could no longer predict the count, and the test would only be "roughly
right". Making every spoil break a rule is what turns the test into a yes/no
check.

The counting loop in `write_file`:

```python
    events = [make_event() for _ in range(rows)]
    bad = 0
    for e in events:
        if random.random() < bad_rate:
            spoil(e)
            bad += 1
```

`random.random()` returns a number in [0, 1). It is below 0.2 about 20% of
the time, so about 20% of rows are spoiled. Each row is spoiled at most once,
so `bad` counts rows, not fields. The default `--bad-rate` is 0.02 (2%),
which the performance runs used.

## 4. write_file(): write to _tmp, then rename

```python
    name = f"events_{datetime.now():%Y%m%d_%H%M%S_%f}.csv"
    tmp_path = os.path.join(out_dir, "_" + name)
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(events)
    os.replace(tmp_path, os.path.join(out_dir, name))     # atomic rename
    return name, bad
```

Line by line:

| Line | Meaning |
|---|---|
| `datetime.now():%Y%m%d_%H%M%S_%f` | file name from the **local** clock down to microseconds, e.g. `events_20261104_123540_223838.csv`. Unique, and sorts in time order |
| `"_" + name` | the temporary name starts with `_` |
| `newline=""` | required by Python's `csv` module, otherwise Windows gets blank lines between rows |
| `csv.DictWriter(..., fieldnames=FIELDS)` | writes dictionaries as CSV rows, columns in `FIELDS` order |
| `writeheader()` | the first line is the column names; the job reads with `header=True` |
| `os.replace(tmp, final)` | rename the finished file to its real name |

**Why the two steps?** Spark lists the folder every trigger. Writing a file
takes a moment. Without the trick, Spark could list the file when only half of
it is on disk, read 37 rows, mark the file as done, and never look at it again.
63 rows lost.

The trick relies on two facts:

1. **Spark's file source ignores files whose names start with `_` or `.`.**
   (Hadoop treats them as hidden.) So the half-written `_events_...csv` is
   invisible.
2. **A rename within one folder is atomic**: it happens in one step. Other
   programs see the old name or the new name, never something in between. So
   when `events_...csv` appears, it is already complete.

`os.replace` is used rather than `os.rename` because on Windows `os.rename`
fails if the target exists, while `os.replace` overwrites. TC-01 checked the
result: 20 files, each with a header and 100 rows, and 0 files starting with
`_` left behind.

TC-07 used the same trick by hand to inject a duplicate file: copy to a
`_` name, then rename.

## 5. Command-line flags

```python
    ap.add_argument("--out", default=os.getenv("INPUT_DIR", "data/incoming"))
    ap.add_argument("--rows", type=int, default=100, help="rows per file")
    ap.add_argument("--every", type=float, default=2.0, help="seconds between files")
    ap.add_argument("--batches", type=int, default=0, help="files to write; 0 = forever")
    ap.add_argument("--bad-rate", type=float, default=0.02, help="share of rows to spoil")
    ap.add_argument("--seed", type=int, help="fix the random numbers, for repeatable tests")
```

`argparse` turns command-line words into `args.rows`, `args.every` and so on
(`--bad-rate` becomes `args.bad_rate`). `load_dotenv()` runs first, so
`--out` defaults to the same `INPUT_DIR` the Spark job reads. The two
programs cannot point at different folders by accident.

| Flag | Default | Used in tests as |
|---|---|---|
| `--rows` | 100 | 100 (functional), 1,000 (P3) |
| `--every` | 2.0 s | 2 (functional, P1, P2), 1 (P3) |
| `--batches` | 0 = forever | 20 (TC-01/04), 5 (TC-06), 15 (hard kill), 3 (TC-08) |
| `--bad-rate` | 0.02 | 0.2 for functional tests (more to catch), 0.02 for performance |
| `--seed` | none | 42 |

Input rate = `--rows` ÷ `--every`: 100 ÷ 2 = 50 rows/s; 1,000 ÷ 1 = 1,000 rows/s.

The main loop:

```python
    os.makedirs(args.out, exist_ok=True)
    written = 0
    try:
        while args.batches == 0 or written < args.batches:
            name, bad = write_file(args.out, args.rows, args.bad_rate)
            written += 1
            print(f"{name}: {args.rows} rows ({bad} spoiled)", flush=True)
            time.sleep(args.every)
    except KeyboardInterrupt:
        pass
    print(f"done: {written} files, {written * args.rows} rows")
```

- `while args.batches == 0 or written < args.batches`: forever when 0,
  otherwise stop after N files.
- `flush=True` prints immediately, even when output is redirected to a log
  file (the `reports/logs/run*_gen.log` files were made that way).
- `except KeyboardInterrupt: pass`: Ctrl+C ends the loop quietly and still
  prints the `done:` summary. Because each file appears by rename, stopping
  mid-way never leaves a broken visible file.

## 6. --seed: repeatable tests

```python
    if args.seed is not None:
        random.seed(args.seed)
```

`random` is a *pseudo*-random generator: a formula that produces a sequence
that looks random. The **seed** is its starting point. Same seed, same
sequence. With `--seed 42`, the 20 files have the same categories, prices,
event types, and, most importantly, **the same rows spoiled in the same
way**.

Proof from this lab: the buggy run (`reports/logs/bug_run/runA_gen.log`) and the
fixed run (`reports/logs/runA_gen.log`) printed identical spoiled counts, file by
file (13, 19, 20, 18, 26, ...; total 385). So the only thing that changed
between 1,642 rows and 1,615 rows was the job's code. That is what makes a
before/after comparison fair.

What the seed does **not** fix:

- `event_id` and `session_id` come from `uuid.uuid4()`, which uses the
  operating system's randomness, not `random`. They differ every run.
- `event_time` and the file names come from the clock.

That is fine: the tests count rows and check rules; they never look up a
specific id.

## Self-check

1. Why does every one of the 16 spoil combinations have to break a rule?
   (So the expected count is exactly generated − spoiled; otherwise the test
   cannot give a yes/no answer.)
2. Which spoil combination did the first job version miss, and why?
   (`user_id = "-5"`: it casts to a valid INT, and the filter only checked
   for null.)
3. What would go wrong if the generator wrote `events_x.csv` directly?
   (Spark could read a half-written file, mark it done, and lose the rest.)
4. Why is `event_id` created in the generator rather than by the database?
   (A resent copy then has the same id, so duplicates can be detected.)
5. With `--seed 42`, are the `event_id`s the same in two runs? (No; they
   come from `uuid4`, not `random`. The spoiled rows and counts are the same.)
