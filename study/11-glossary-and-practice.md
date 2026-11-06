# 11 — Glossary, Practice Questions and Hands-on Drills

Use this page last, as a rehearsal. Cover the answers, say yours out loud,
then compare. The drills run read-only queries on the real `events` table
and small Python scripts on the real metrics files.

> **Help:** this glossary covers concepts. Every PySpark method and SQL keyword used in the code is in [12 · Streaming and SQL reference](12-streaming-and-sql-reference.md). The longer explanations are in [09 · The hard parts](09-hard-concepts-explained.md).

## A. Glossary (one line each)

| Term | Meaning |
|---|---|
| **Event** | One user action: a view, an add-to-cart or a purchase. One row in `events` |
| **Batch processing** | Process a fixed, finished set of data in one go |
| **Stream processing** | Process data continuously as it arrives, with no end |
| **Spark** | An engine that splits data work across many cores or machines |
| **PySpark** | The Python interface to Spark |
| **Structured Streaming** | Spark's streaming API: the same DataFrame code, run again on each new slice of data |
| **DataFrame** | A table of rows and named, typed columns that Spark processes |
| **Driver** | The main process that plans the work. `write_batch` runs here |
| **Executor / task** | The workers that run pieces of a job, one task per partition |
| **`local[*]`** | Run Spark on this machine only, using every CPU core |
| **Partition** | One slice of a DataFrame. Each is processed by one task |
| **Shuffle** | Moving rows between partitions so equal keys meet. `dropDuplicates` needs one |
| **Lazy evaluation** | Spark only builds a plan until an action (`count`, `save`) forces it to run |
| **Action** | An operation that runs the plan and returns or writes data (`count()`, `save()`) |
| **Source** | Where a streaming query reads from. Here: the CSV file source on `data/incoming/` |
| **Sink** | Where a streaming query writes. Here: `foreachBatch` |
| **Micro-batch** | One small batch job that Spark runs per trigger on newly arrived data |
| **Trigger** | How often a micro-batch starts (`processingTime="5 seconds"`) |
| **`maxFilesPerTrigger`** | The most files one micro-batch may read (`MAX_FILES`) |
| **Backlog** | Files that have arrived but are not processed yet |
| **Falling behind** | Data arrives faster than it is processed, so the backlog grows |
| **Checkpoint** | Folder where Spark records offsets, commits and state, so it can restart |
| **Offset** | A position in the source. For files: which files belong to batch N |
| **Commit (checkpoint)** | The marker Spark writes after a batch's sink succeeded |
| **State / state store** | Data Spark keeps between batches. Here: event ids seen recently |
| **Watermark** | Newest event time seen minus an allowed delay (10 minutes). Older data is "too late" |
| **Event time** | When the action happened (`event_time`, set by the generator) |
| **Processing time** | When the system handles the event (Spark's clock) |
| **Ingestion time** | When the row landed in the database (`ingested_at`) |
| **At-least-once** | Every batch is delivered; some may be delivered twice after a failure |
| **Exactly-once** | Every batch takes effect exactly once. Here: at-least-once + idempotent write |
| **Idempotent** | Doing it twice has the same effect as doing it once |
| **Natural key** | An id that comes with the data (`event_id`), not invented by the database |
| **Primary key** | Column whose values must be unique and not null (`event_id`) |
| **Staging table** | A plain landing table for a batch before it is merged (`events_staging`) |
| **Merge / upsert** | Insert new rows and skip (or update) existing ones in one statement |
| **`ON CONFLICT DO NOTHING`** | PostgreSQL: if the key already exists, skip the row instead of failing |
| **JDBC** | Java's standard way to talk to databases. Spark writes to PostgreSQL through it |
| **JDBC driver** | The library for one database: `org.postgresql:postgresql:42.7.4` |
| **psycopg2** | The Python library the job uses to run the merge SQL |
| **Transaction** | A group of SQL statements that succeed or fail together |
| **`TIMESTAMPTZ`** | PostgreSQL type for an absolute instant (stored in UTC) |
| **Schema-on-read** | Types are decided when reading, not stored in the file |
| **ANSI mode** | Strict SQL rules in Spark 4: a bad cast throws instead of returning null |
| **`try_cast`** | A cast that returns null for a bad value instead of throwing |
| **Atomic rename** | A rename that happens all at once: readers see no file or the whole file |
| **Filter pushdown** | Running a filter inside the reader so fewer rows are loaded |
| **Least privilege** | Give a login only the rights it needs (`spark_app`) |
| **Listener** | A class Spark calls on query events (`ProgressLogger.onQueryProgress`) |
| **Latency** | Time from event to row in the database (`ingested_at − event_time`) |
| **Throughput** | Rows handled per second |
| **p50 / p95** | The median, and the value only 5% of rows exceed |
| **Fixed cost** | Work paid once per batch whatever its size (~3 s here) |
| **Dead-letter table** | A table for rows that could not be loaded, kept for inspection (not built yet) |
| **winutils / `HADOOP_HOME`** | Helper files Spark needs on Windows to write checkpoints |

## B. Practice questions (with model answers)

**1. What does one micro-batch do in this job, start to end?**
Lists new files (up to `MAX_FILES`), reads them as strings, casts and
filters (`clean()`), drops duplicates, then `write_batch` appends to
`events_staging` by JDBC and merges into `events`. Then Spark writes the
commit.

**2. How many files can P1's settings process per minute? How many
arrive?**
12 processed (one per 5 s), 30 arrive (one per 2 s).

**3. What are the four bad values the generator uses, and which one got
through the first filter?**
`""`, `"-5"`, `"unknown"`, `"NaN"`. `"-5"` in `user_id`: it casts to a
valid integer.

**4. Name the five folders in a checkpoint and what each holds.**
`metadata` (query id), `offsets` (what each batch covers), `sources`
(file names per batch), `commits` (finished batches), `state` (dedup ids).

**5. What would happen if you dropped `.withWatermark(...)` but kept
`dropDuplicates`?**
It would still work, but state would keep every `event_id` forever and
grow without limit.

**6. Why does `events_staging` have no constraints?**
So the JDBC bulk insert never fails half way. The rules are applied when
rows move into `events`.

**7. A reviewer runs `SELECT event_time FROM events LIMIT 1` and sees
`+02`. Is the data wrong?**
No. The server's display zone is UTC+2. The column is `TIMESTAMPTZ`, so
the instant is right. `SET timezone='UTC'` shows it in UTC.

**8. What does `inserted = cur.rowcount` count?**
The rows the `INSERT ... ON CONFLICT DO NOTHING` actually inserted. Rows
skipped by the conflict are not counted.

**9. In TC-08, why did no data get lost?**
The batch failed before Spark wrote its commit. The files and the offset
stayed, so the next start re-ran batch 44.

**10. Why is P2's p50 about 5.4 s?**
A file waits on average half a trigger (2.5 s) for a batch to start, and
the batch takes ~3 s.

**11. Why does `batches.jsonl` show `at` two hours later than
`progress.jsonl`'s `timestamp` for the same batch?**
`at` is `time.strftime(...)` in local time (UTC+2). `timestamp` is
Spark's UTC time.

**12. Why might `numInputRows` and the generator's row count differ?**
Spark pushes not-null checks into the CSV reader, so some bad rows are
dropped before being counted (5,984 of 6,000 in P2).

## C. Hands-on drills

### Setup

All SQL drills are read-only. In Git Bash:

```bash
PSQL="/c/Program Files/PostgreSQL/18/bin/psql.exe"
"$PSQL" -w -h localhost -U postgres -d ecommerce
```

In PowerShell:

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -w -h localhost -U postgres -d ecommerce
```

Then, first thing in the session, so times show in UTC like the CSV
files:

```sql
SET timezone = 'UTC';
```

The table held **138,629** rows when these outputs were taken (all test
and performance runs together), with 138,629 distinct `event_id`s.

### Drill 1: Events per type per minute

```sql
SELECT date_trunc('minute', event_time) AS minute, event_type, count(*) AS n
FROM events
WHERE event_time >= '2026-11-04 11:03:00+00' AND event_time < '2026-11-04 11:05:00+00'
GROUP BY 1, 2
ORDER BY 1, 2;
```

```
         minute         | event_type  |  n
------------------------+-------------+------
 2026-11-04 11:03:00+00 | add_to_cart |  489
 2026-11-04 11:03:00+00 | purchase    |  246
 2026-11-04 11:03:00+00 | view        | 2196
 2026-11-04 11:04:00+00 | add_to_cart |  376
 2026-11-04 11:04:00+00 | purchase    |  183
 2026-11-04 11:04:00+00 | view        | 1699
```

This window is run P2. `date_trunc('minute', ...)` cuts each time down
to the start of its minute. `GROUP BY 1, 2` means "group by the first
and second columns of the SELECT". The 11:04 minute is smaller because
the generator stopped at 11:04:45. Question: what share are views? (Whole
table: 75.1% views, 16.8% add-to-cart, 8.1% purchases, close to the
generator's weights 75 / 17 / 8.)

### Drill 2: Purchase revenue by category

```sql
SELECT category,
       count(*)                AS purchases,
       sum(quantity)           AS items,
       sum(price * quantity)   AS revenue,
       round(avg(price), 2)    AS avg_price
FROM events
WHERE event_type = 'purchase'
GROUP BY category
ORDER BY revenue DESC;
```

```
  category   | purchases | items |  revenue   | avg_price
-------------+-----------+-------+------------+-----------
 electronics |      2323 |  4619 | 3569663.68 |    774.72
 sports      |      2241 |  4450 | 1381029.40 |    308.02
 home        |      2206 |  4426 |  908789.50 |    205.49
 fashion     |      2185 |  4336 |  559356.74 |    129.64
 books       |      2214 |  4400 |  145054.16 |     32.98
```

Check it against the generator: `CATEGORIES` gives electronics prices
from 50 to 1,500, so a mean near 775 is right. Books are 5 to 60, mean
~33. Quantity for a purchase is 1 to 3, mean 2, so items ≈ 2 × purchases.
Revenue is exact to the cent because `price` is `NUMERIC(10,2)`.

### Drill 3: Latency percentiles for one run

```sql
SELECT round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at - event_time))::numeric, 1) AS p50,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at - event_time))::numeric, 1) AS p95,
       round(max(extract(epoch FROM ingested_at - event_time))::numeric, 1) AS worst,
       count(*)
FROM events
WHERE ingested_at BETWEEN '2026-11-04 11:02:30+00' AND '2026-11-04 11:05:30+00';
```

```
 p50 | p95  | worst | count
-----+------+-------+-------
 5.4 | 13.4 |  15.4 |  5876
```

The same numbers `perf_report.py` printed for P2. `::numeric` is needed
because `round(x, 1)` needs a `numeric`, and `percentile_cont` returns a
`double precision`. Try changing the window to P1
(`'2026-11-04 10:49:50+00'` to `'2026-11-04 10:53:00+00'`) and watch p50
jump to about a minute.

### Drill 4: Top users

```sql
SELECT user_id,
       count(*)                                        AS events,
       count(*) FILTER (WHERE event_type = 'purchase') AS purchases
FROM events
GROUP BY user_id
ORDER BY events DESC, user_id
LIMIT 5;
```

```
 user_id | events | purchases
---------+--------+-----------
     472 |    322 |        24
     258 |    319 |        14
      59 |    315 |        30
     107 |    315 |        34
     161 |    312 |        25
```

`FILTER (WHERE ...)` counts only the rows that match, inside the same
group. The generator picks `user_id` uniformly from 1 to 500, so with
138,629 rows each user averages ~277 events. A top of 322 is normal
random spread, not a "power user".

### Drill 5: Prove the data is clean

```sql
SELECT count(*) AS rule_breakers FROM events
WHERE user_id <= 0 OR product_id <= 0 OR price < 0 OR quantity < 0
   OR event_type NOT IN ('view', 'add_to_cart', 'purchase');
-- 0

SELECT count(*), count(DISTINCT event_id) FROM events;   -- 138629 | 138629
SELECT count(*) FROM events_staging;                      -- 0
```

Three facts in three lines: no row breaks a rule, no duplicates, and the
staging table was emptied after the last merge.

### Drill 6: Mean batch duration per run, with pandas

`metrics/progress.jsonl` has one JSON object per line, so pandas reads it
with `lines=True`. Save as a script in the project folder and run with
`python`:

```python
import pandas as pd

df = pd.read_json("metrics/progress.jsonl", lines=True)
df["timestamp"] = pd.to_datetime(df["timestamp"])

runs = {"P1": ("2026-11-04T10:49:50Z", "2026-11-04T10:53:00Z"),
        "P2": ("2026-11-04T11:02:30Z", "2026-11-04T11:05:30Z"),
        "P3": ("2026-11-04T11:06:00Z", "2026-11-04T11:09:30Z")}

for name, (start, end) in runs.items():
    w = df[(df.timestamp >= start) & (df.timestamp <= end) & (df.num_input_rows > 0)]
    print(name, len(w), "batches,", w.num_input_rows.sum(), "rows,",
          "mean", round(w.trigger_ms.mean()), "ms,",
          "median", round(w.trigger_ms.median()), "ms")
```

Real output:

```
P1 36 batches, 3592 rows, mean 3037 ms, median 2768 ms
P2 24 batches, 5984 rows, mean 3306 ms, median 2809 ms
P3 15 batches, 119684 rows, mean 3746 ms, median 3127 ms
```

The means match `reports/performance_metrics.md`. The medians are lower because
the slow first batch (10 to 12.5 s) pulls the mean up. Which number
describes the steady state better? (The median.)

### Drill 7: The same with Spark (batch, not streaming)

A normal `spark.read` works on the same file. This starts Spark (about
25 to 30 s), so run it in a terminal with `HADOOP_HOME` set:

```python
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[*]")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
p = spark.read.json("metrics/progress.jsonl")
p = p.withColumn("ts", F.to_timestamp("timestamp"))

(p.filter(F.col("num_input_rows") > 0)
  .withColumn("run", F.when(F.col("ts").between("2026-11-04 10:49:50", "2026-11-04 10:53:00"), "P1")
                      .when(F.col("ts").between("2026-11-04 11:02:30", "2026-11-04 11:05:30"), "P2")
                      .when(F.col("ts").between("2026-11-04 11:06:00", "2026-11-04 11:09:30"), "P3"))
  .filter(F.col("run").isNotNull())
  .groupBy("run")
  .agg(F.count("*").alias("batches"), F.round(F.avg("trigger_ms")).alias("mean_ms"))
  .orderBy("run")
  .show())
spark.stop()
```

Why the `timeZone` line: the window strings have no zone, so Spark reads
them in the *session* time zone. Without it the session zone is the
laptop's (UTC+2), the windows are two hours off, and the result is
empty. This is the code-level version of the +2 hours story.

Real output:

```
+---+-------+-------+
|run|batches|mean_ms|
+---+-------+-------+
| P1|     36| 3037.0|
| P2|     24| 3306.0|
| P3|     15| 3746.0|
+---+-------+-------+
```

Same numbers as pandas. `when(...).when(...)` is SQL's `CASE WHEN`;
a row matching no window gets null and is filtered out.

### Drill 8: Read a checkpoint by hand

```bash
ls checkpoint/events/commits | sort -n | tail -3
cat checkpoint/perf_P3/sources/0/5 | head -3
cat checkpoint/perf_P3/offsets/5 | head -2
```

The `sources` file lists the CSV files batch 5 read. The `offsets` file's
second line holds `batchWatermarkMs` (the watermark) and
`batchTimestampMs`. Convert the watermark with Python:
`datetime.fromtimestamp(1793789819007 / 1000, timezone.utc)` →
`2026-11-04 10:56:59.007+00:00`. Look only; never edit a checkpoint of a
job you care about.

## D. Two-minute walkthrough script

1. **Problem (15 s).** "An online shop wants user events in a database
   within seconds, not in tomorrow's batch."
2. **Flow (35 s).** "A generator writes CSV files, renamed in atomically.
   Spark Structured Streaming reads new files every 5 seconds, reads
   everything as text, casts with try_cast, filters bad rows, and drops
   duplicates with a 10-minute watermark. foreachBatch appends each batch
   to a staging table and merges it into events with ON CONFLICT DO
   NOTHING."
3. **Safety (30 s).** "The checkpoint plus the idempotent merge means a
   crash or replay never loses or doubles rows. Tested: forced replay of
   83 rows inserted 0; database outage recovered with no loss; a bug
   that let user_id -5 through was caught by an exact row count and
   fixed in Spark and with CHECK constraints."
4. **Numbers (25 s).** "At 50 rows a second, p50 latency 5.4 s once
   maxFilesPerTrigger was 5. With 1 it fell behind. At 1,000 rows a
   second it kept up with 10-second batches."
5. **Limits (15 s).** "One laptop, 2-minute runs, a file source instead
   of Kafka, rejected rows not kept yet."
