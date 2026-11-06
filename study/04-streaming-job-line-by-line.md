# 04 — spark_streaming_to_postgres.py, Line by Line

The streaming job is Parts 2 and 3 of the lab: it watches the folder, cleans
each new file and writes the rows to PostgreSQL. This page goes through all
202 lines in the order they run.

> **Where the code lives now.** The lab was split into modules like Lab 1
> (`src/`, `tests/`, `analysis.ipynb`). The code below is unchanged in what it
> does, only where it is: `src/config.py` reads `.env`, `src/spark_session.py`
> builds Spark, `src/cleaning.py` has the schema, `read_stream()` and
> `clean()` (now split into `cast_types()`, `drop_bad_rows()` and
> `drop_duplicates()`), `src/postgres_sink.py` has `MERGE_SQL` and
> `write_batch()`, `src/metrics.py` has `ProgressLogger`, and `src/main.py`
> starts the query (`start_query()`, plus a `--once` flag). The root file
> `spark_streaming_to_postgres.py` only calls `src.main.main()`, so every
> command on this page still works. Line numbers below are from the old
> single file.

> **Help:** the ideas behind the words used here (micro-batch, trigger, sink, watermark) are in [02 · Batch vs streaming](02-batch-vs-streaming.md). Every Spark function used is also listed A–Z in [12 · Streaming API and SQL reference](12-streaming-and-sql-reference.md).

## The shape of the file

The module docstring gives the whole flow in three lines:

```text
new CSV files -> read as strings -> cast + validate -> drop duplicates
  -> foreachBatch: JDBC insert into events_staging -> SQL merge into events
```

| Part | Lines | Job |
|---|---|---|
| settings | 24–52 | read `.env`, build the JDBC URL, define the schema and the merge SQL |
| `build_spark()` | 55–65 | start Spark with the right settings |
| `read_stream()` | 68–74 | the source: the CSV folder |
| `clean()` | 77–99 | cast, validate, de-duplicate |
| `write_batch()` | 108–147 | the sink: runs once per micro-batch |
| `ProgressLogger` | 150–170 | saves Spark's progress report per batch |
| `main()` | 173–198 | wires it together, starts and stops the query |

## 1. Configuration from .env

```python
load_dotenv()

PG: dict[str, Any] = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "ecommerce"),
    "user": os.getenv("PGUSER", "spark_app"),
    "password": os.getenv("PGPASSWORD", ""),
}
JDBC_URL = f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}"
INPUT_DIR = os.getenv("INPUT_DIR", "data/incoming")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "checkpoint/events")
TRIGGER = os.getenv("TRIGGER", "5 seconds")
MAX_FILES = int(os.getenv("MAX_FILES", "1"))
METRICS_DIR = Path("metrics")
```

- `load_dotenv()` copies each `NAME=value` line of `.env` into the process
  environment. It does **not** overwrite a variable that is already set. That
  is how TC-08 could run `PGPORT=5499 python spark_streaming_to_postgres.py`:
  the command-line value wins over `.env`.
- `os.getenv(name, default)` reads a variable or falls back to the default.
  Environment values are always text, hence `int(...)` for the port and
  `MAX_FILES`.
- `PG` is one dictionary used twice: `psycopg2.connect(**PG)` unpacks it into
  keyword arguments (`host=..., port=...`), and its parts build `JDBC_URL`,
  e.g. `jdbc:postgresql://localhost:5432/ecommerce`.
- The defaults equal `.env.example`, so the job behaves the same with or
  without those lines. Only the password has no useful default.

```python
COLS = ["event_id", "event_time", "user_id", "session_id", "event_type",
        "product_id", "category", "price", "quantity"]
EVENT_TYPES = ["view", "add_to_cart", "purchase"]

RAW_SCHEMA = StructType([StructField(c, StringType()) for c in COLS])
```

A **schema** is the list of column names and types. `RAW_SCHEMA` says: nine
columns, **all strings**. The comment in the code explains why:
*a bad value then becomes null at the cast step instead of failing the whole
file at read time.* If `price` were declared as a decimal here, the CSV reader
would have to decide what to do with `unknown` in the middle of reading.
Reading text first and converting later puts that decision in our hands, row
by row.

```python
MERGE_SQL = f"""
    INSERT INTO events ({", ".join(COLS)})
    SELECT {", ".join(COLS)} FROM events_staging
    ON CONFLICT (event_id) DO NOTHING
"""
```

The f-string fills in the nine column names, so the merge can never list
columns in a different order from `COLS`. What the SQL does is explained in
[05](05-postgres-jdbc-and-the-merge.md#h-12-why-staging-merge-not-a-plain-append).

## 2. build_spark(): the SparkSession

```python
def build_spark() -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder
             .appName("ecommerce-events-to-postgres")
             .master("local[*]")
             .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")  # JDBC driver
             .config("spark.sql.shuffle.partitions", "4")     # default 200 is too many for small batches
             .config("spark.sql.session.timeZone", "UTC")     # the generator writes UTC times
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
```

A **SparkSession** is your connection to the Spark engine. Every DataFrame
is made through it.

| Setting | What it does | Why here |
|---|---|---|
| `PYSPARK_PYTHON = sys.executable` | tells Spark which Python to start for its worker processes | the same interpreter that runs this script, so the same packages. `setdefault` keeps any value you already set |
| `appName(...)` | a label shown in logs and the Spark UI (`http://localhost:4040` while it runs) | easy to recognise |
| `master("local[*]")` | run on this machine, one worker thread per CPU core | no cluster in this lab |
| `spark.jars.packages` | download this Java library from Maven Central and put it on Spark's classpath | the PostgreSQL JDBC driver. First run needs internet; then it is cached |
| `spark.sql.shuffle.partitions = 4` | how many pieces Spark splits data into after a *shuffle* (moving rows between workers by key) | `dropDuplicates` shuffles by `event_id`. The default 200 would mean 200 tiny tasks per 100-row batch, and 200 state-store folders. 4 is enough; the checkpoint shows `state/0/0` to `state/0/3` |
| `spark.sql.session.timeZone = UTC` | the time zone Spark uses to read and show timestamps that have no zone | the CSV says `10:35:40.222` with no zone. With this setting Spark reads it as UTC, which is what the generator meant |
| `setLogLevel("WARN")` | hide Spark's INFO chatter | the console shows our `batch ...` lines and real warnings only |

`getOrCreate()` returns the existing session if one is already running in
this Python process. That is a trap in notebooks: settings like
`spark.jars.packages` only apply when the session is first created.

## 3. read_stream(): the source

```python
def read_stream(spark: SparkSession) -> DataFrame:
    os.makedirs(INPUT_DIR, exist_ok=True)             # the file source fails on a missing folder
    return (spark.readStream
            .schema(RAW_SCHEMA)                       # streaming sources need the schema up front
            .option("header", True)
            .option("maxFilesPerTrigger", MAX_FILES)  # cap on files per micro-batch
            .csv(INPUT_DIR))
```

| Line | Meaning |
|---|---|
| `os.makedirs(..., exist_ok=True)` | create the folder if missing. Without it, a fresh checkout failed with `PATH_NOT_FOUND` before the generator had made the folder |
| `spark.readStream` | like `spark.read`, but returns a **streaming** DataFrame: a description of an endless input, not data |
| `.schema(RAW_SCHEMA)` | streaming sources cannot infer a schema; you must give it |
| `.option("header", True)` | the first line of each file is column names, not data |
| `.option("maxFilesPerTrigger", MAX_FILES)` | at most this many new files per micro-batch. With 1, one file = one batch |
| `.csv(INPUT_DIR)` | watch this folder for new CSV files |

Nothing is read yet. A streaming DataFrame is a plan; it only runs after
`writeStream...start()`.

## 4. clean(): types and rules

### Casting

```python
    typed = (raw
        .withColumn("event_time", F.expr("try_to_timestamp(event_time, 'yyyy-MM-dd HH:mm:ss.SSS')"))
        .withColumn("user_id", F.expr("try_cast(user_id AS INT)"))
        .withColumn("product_id", F.expr("try_cast(product_id AS INT)"))
        .withColumn("price", F.expr("try_cast(price AS DECIMAL(10,2))"))
        .withColumn("quantity", F.expr("try_cast(quantity AS INT)"))
        .withColumn("event_type", F.lower(F.trim("event_type")))
        .withColumn("category", F.lower(F.trim("category"))))
```

- `withColumn(name, expr)` replaces the column with a new value.
- `F.expr("...")` lets you write a Spark SQL expression as a string.
- **`try_cast(x AS INT)`** converts text to a number, and returns **null**
  if it cannot. A plain `CAST` behaves differently in Spark 4: *ANSI mode*
  (strict SQL-standard behaviour) is on by default, so `CAST('unknown' AS INT)`
  **raises an error** and would stop the whole query. `try_cast` turns a bad
  value into null so the row can be filtered out instead.
- **`try_to_timestamp(text, pattern)`** is the same idea for dates. The
  pattern `yyyy-MM-dd HH:mm:ss.SSS` matches the generator's format exactly
  (`SSS` = milliseconds). `-5` or `unknown` give null.
- `DECIMAL(10,2)`: up to 10 digits, 2 after the point. Exact money, no
  floating-point rounding. Matches `NUMERIC(10,2)` in PostgreSQL.
- `F.lower(F.trim(...))`: strip spaces and lower-case, so ` View ` would
  still count as `view`.

### The rules

```python
    valid = typed.filter(
        F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
        & (F.col("user_id") > 0)                      # "> 0" also rejects null; "-5" casts to a valid INT
        & (F.col("product_id") > 0)
        & F.col("event_type").isin(EVENT_TYPES)
        & (F.col("price") >= 0)                       # null price fails this too
        & (F.col("quantity").isNull() | (F.col("quantity") >= 0)))
```

`filter` keeps rows where the condition is true. `&` is AND, `|` is OR. In
PySpark each comparison must be in brackets, because `&` binds tighter than
`>` in Python.

| Rule | Rejects |
|---|---|
| `event_id` not null | rows with no id (cannot de-duplicate) |
| `event_time` not null | empty or unparseable times |
| `user_id > 0` | null **and** negative ids. A comparison with null is *unknown*, and `filter` drops unknown, so `> 0` covers both |
| `product_id > 0` | same for products |
| `event_type` in the list | empty, misspelled, `nan` |
| `price >= 0` | null and negative prices |
| `quantity` null or `>= 0` | negative quantities; a missing quantity is allowed |

The comment on the `user_id` line is the scar from TC-04. The first version
said `F.col("user_id").isNotNull()`. The spoiled value `"-5"` casts to the
valid integer -5, is not null, and passed: 27 bad rows reached the table. The
same rules now also exist as `CHECK` constraints in PostgreSQL. Full story in
[07](07-testing-the-pipeline.md#h-tc-04-the-test-that-found-a-real-bug).

## 5. Watermark and dropDuplicates

```python
    # Same event_id seen twice within 10 minutes of event time -> keep one.
    # The watermark tells Spark when it may forget old ids, so memory stays bounded.
    return (valid
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["event_id", "event_time"]))
```

**`dropDuplicates`** on a stream must remember every key it has seen, across
batches, to know if a new row is a repeat. That memory is **state**, and Spark
saves it in the checkpoint (`checkpoint/events/state/`).

Without a limit, state grows forever. The **watermark** is the limit:

> watermark = (latest `event_time` seen so far) − 10 minutes

Spark assumes no row older than the watermark will still arrive, and it
deletes remembered keys older than that. Because `event_time` is part of the
de-duplication key, Spark can tell which stored keys are old enough to drop.

So the job removes duplicates **that arrive within about 10 minutes of each
other** (in event time). TC-07 proved it: a copied file with 98 valid rows
gave 0 new rows. A copy arriving later would get past this step, and then the
database's primary key stops it. More on bounded state in
[06](06-reliability-checkpoints-exactly-once.md#h-watermark-bounded-state).

(Spark 3.5+ also has `dropDuplicatesWithinWatermark(["event_id"])`, which
keys on the id alone. This job uses the classic form.)

## 6. write_batch(): the sink

`foreachBatch` calls this function once per micro-batch. It runs on the
**driver** (the main Python process) and gets the batch as an ordinary
DataFrame, plus the batch id.

```python
    started = time.time()
    batch_df = batch_df.select(*COLS).persist()      # used twice: count + write
    rows = batch_df.count()
    if rows == 0:
        batch_df.unpersist()
        return
```

- `select(*COLS)`: the nine columns, in table order.
- **`persist()`**: keep the computed rows in memory after their first use.
  The batch is used twice (count, then write). Without `persist`, Spark would
  re-read the CSV and redo all the casting for the second use.
- `count()` is the number of clean rows in this batch.
- Empty batch: nothing to do. This happens when every row was a duplicate
  (TC-07's batch 21) or for Spark's own no-data batches. No line is printed
  and nothing goes to `batches.jsonl`.

```python
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE events_staging")       # clear leftovers from a failed earlier try
    (batch_df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "events_staging")
        .option("user", PG["user"])
        .option("password", PG["password"])
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())
    staged = time.time()
```

1. Empty the staging table. If an earlier attempt died after loading staging
   but before the merge, its rows are still there; this clears them.
2. Bulk-write the batch into `events_staging` through JDBC. `mode("append")`
   adds rows, never drops the table. `driver` names the Java class inside the
   jar that `spark.jars.packages` downloaded.

TC-08 failed on exactly this first `psycopg2.connect` (the traceback
points into `write_batch` at this line): `port 5499 failed: Connection refused`. An exception here stops
the query **before** the batch is recorded as done, which is what makes the
retry safe.

```python
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:   # one transaction
        cur.execute(MERGE_SQL)
        inserted = cur.rowcount
        cur.execute("TRUNCATE events_staging")
    batch_df.unpersist()
```

- `with conn:` in psycopg2 means **one transaction**: everything inside is
  committed together at the end of the block, or rolled back together if an
  exception is raised. (It does not close the connection; that happens when
  the function returns.)
- `cur.rowcount` after `INSERT ... SELECT` is the number of rows actually
  inserted. Rows skipped by `ON CONFLICT DO NOTHING` are not counted. That is
  how the log can say `83 rows, 0 inserted`.
- `unpersist()` frees the cached batch.

```python
    done = time.time()
    log_jsonl("batches.jsonl", {
        "batch_id": batch_id, "rows": rows, "inserted": inserted,
        "skipped_duplicates": rows - inserted,
        "jdbc_ms": round((staged - started) * 1000), "merge_ms": round((done - staged) * 1000),
        "at": time.strftime("%Y-%m-%dT%H:%M:%S")})
    print(f"batch {batch_id}: {rows} rows, {inserted} inserted, "
          f"{round(done - started, 1)} s", flush=True)
```

One JSON line per non-empty batch in `metrics/batches.jsonl`, for example the
normal and the replayed batch 42:

```json
{"batch_id": 42, "rows": 83, "inserted": 83, "skipped_duplicates": 0, "jdbc_ms": 1188, "merge_ms": 57, "at": "2026-11-04T12:43:27"}
{"batch_id": 42, "rows": 83, "inserted": 0, "skipped_duplicates": 83, "jdbc_ms": 5875, "merge_ms": 125, "at": "2026-11-04T12:45:50"}
```

`jdbc_ms` covers count + truncate + JDBC write (about 1.2 s normally, about
6 s on the first batch after start-up); `merge_ms` is the merge transaction
(about 55 ms). `log_jsonl` opens the file in append mode (`"a"`) and creates
`metrics/` if needed. **JSON Lines** = one JSON object per line, easy to append
to and to read back line by line.

## 7. ProgressLogger: Spark's own report

```python
class ProgressLogger(StreamingQueryListener):
    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        print(f"query started: id={event.id}", flush=True)

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        p = json.loads(event.progress.json)
        log_jsonl("progress.jsonl", {
            "batch_id": p["batchId"], "timestamp": p["timestamp"],
            "num_input_rows": p["numInputRows"],
            "input_rows_per_s": p.get("inputRowsPerSecond"),
            "processed_rows_per_s": p.get("processedRowsPerSecond"),
            "trigger_ms": p["durationMs"].get("triggerExecution"),
            "add_batch_ms": p["durationMs"].get("addBatch")})

    def onQueryIdle(self, event: QueryIdleEvent) -> None:
        pass

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        print(f"query stopped: exception={event.exception}", flush=True)
```

A **listener** is an object Spark calls when something happens to a query.
`spark.streams.addListener(ProgressLogger())` in `main()` registers it.
`onQueryIdle` (called when a trigger finds no new data) does nothing here;
it is written out so all four events are visible.

After each micro-batch Spark builds a **progress report** (the same JSON you
see as `query.lastProgress`). We keep six fields:

| Field | Meaning |
|---|---|
| `numInputRows` | rows the source handed to this batch |
| `inputRowsPerSecond` | rows arriving per second since the last batch |
| `processedRowsPerSecond` | rows ÷ this batch's duration |
| `durationMs.triggerExecution` | whole batch time, start to end |
| `durationMs.addBatch` | time spent in the sink, i.e. our `write_batch` |

Note: `numInputRows` is a bit below the rows in the file: 98 for batch 0 of
run A, not 100. Spark pushes the `IS NOT NULL` checks down into the CSV
reader, so rows with an empty `event_time` or `user_id` are dropped before
being counted. These two logs feed `src/perf_report.py` and
[08 · Performance and tuning](08-performance-and-tuning.md).

`event.id` is the query's permanent id, stored in `checkpoint/events/metadata`.
Every restart of this lab printed the same `id=2887773f-...`: same checkpoint,
same query.

## 8. main(): starting and stopping the query

```python
    spark = build_spark()
    spark.streams.addListener(ProgressLogger())
    query = (clean(read_stream(spark))
             .writeStream
             .queryName("events_to_postgres")
             .foreachBatch(write_batch)
             .option("checkpointLocation", CHECKPOINT_DIR)   # which files are done + dedup state
             .trigger(processingTime=TRIGGER)
             .start())
```

| Call | Meaning |
|---|---|
| `clean(read_stream(spark))` | source → cleaning plan |
| `.writeStream` | start describing the output side |
| `.queryName(...)` | a name, visible in the Spark UI |
| `.foreachBatch(write_batch)` | the sink is our function |
| `.option("checkpointLocation", ...)` | where Spark saves progress and state. **This one line is what makes restarts safe** ([06](06-reliability-checkpoints-exactly-once.md)) |
| `.trigger(processingTime="5 seconds")` | a micro-batch every 5 s |
| `.start()` | actually start; returns a `StreamingQuery` handle and runs in the background |

Output mode is not set, so it is the default, **append**.

```python
    try:
        if args.run_seconds:
            query.awaitTermination(args.run_seconds)
            query.stop()                                     # clean stop: finishes the current batch
        else:
            query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
    spark.stop()
```

- `awaitTermination()` blocks the main thread while the query runs in the
  background. With a number, it waits at most that many seconds.
- `--run-seconds 120` was used in tests so a run ends by itself at a known
  point. Then `query.stop()` and `spark.stop()` shut down cleanly; the log
  shows `query stopped: exception=None`.
- Ctrl+C raises `KeyboardInterrupt`, handled the same way.
- If a batch is cut short by a stop or a crash, it has no commit file, so
  it runs again on the next start. Nothing is lost
  ([06](06-reliability-checkpoints-exactly-once.md#h-crash-before-or-after-the-commit)).
- On stop, Spark may log `Failed to cancel job group ... Cannot find active
  jobs for it`. It means there was no running job to cancel. Harmless.

## Self-check

1. Why is every column read as a string first? (So a bad value becomes null
   at the `try_cast` step, row by row, instead of failing the read.)
2. Why `try_cast` and not `CAST` on Spark 4? (ANSI mode makes `CAST` raise an
   error on bad input and stop the query; `try_cast` returns null.)
3. Why is `event_time` in the `dropDuplicates` key? (With the watermark, it
   lets Spark tell which stored ids are old enough to forget, so state stays
   bounded.)
4. What does `persist()` save in `write_batch`? (Re-reading and re-cleaning
   the batch for the second action: count, then write.)
5. How can `batches.jsonl` show `"rows": 83, "inserted": 0`? (A replayed
   batch: all 83 `event_id`s were already in `events`, so `ON CONFLICT DO
   NOTHING` skipped them and `rowcount` was 0.)
