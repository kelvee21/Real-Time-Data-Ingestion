# 10 — Defending This Project in Code Review

A reviewer will check three things: do you know **why** each piece is
there, can you back it with **numbers** from your own runs, and do you
know the **weak spots**. Below are likely questions, grouped by area, with
short model answers grounded in this repo. Say them in your own words.

> **Help:** if a question uses a term you can't explain, the long answer is in [09 · The hard parts](09-hard-concepts-explained.md). For a line of code, see [04 · The streaming job line by line](04-streaming-job-line-by-line.md) or [03 · The generator line by line](03-generator-line-by-line.md). Every function is in [12 · Reference](12-streaming-and-sql-reference.md).

## The 60-second opening

> "A Python generator writes CSV files of fake shop events into a folder,
> one file every few seconds. A Spark Structured Streaming job watches
> that folder. Every 5 seconds it reads the new files, converts the text
> to proper types, drops invalid rows and duplicates, and writes the batch
> to PostgreSQL in two steps: a bulk insert into a staging table, then one
> SQL merge into `events` that skips any `event_id` already there. A
> checkpoint lets the job restart where it stopped, and the merge makes a
> replayed batch harmless. I tested nine cases, including a crash, a
> forced replay and a database outage, and measured latency at three
> loads: p50 5.4 seconds at 50 rows a second with the right settings."

## Generator (`data_generator.py`)

**Q: Why CSV files in a folder and not Kafka?**
A: The spec asks for it (Part 1: "creates fake e-commerce events (CSV
files)", Part 2: "monitor the folder"). A folder is the simplest
replayable source: files stay on disk, so after a crash Spark can read
them again. Kafka would be the production choice; the Spark code would
change only in `read_stream()`.

**Q: How do you stop Spark reading a half-written file?**
A: `write_file()` writes to `_events_....csv`, then `os.replace()` renames
it. Spark ignores names starting with `_`, and the rename is atomic.
TC-01 found 0 leftover `_` files after 20 files.

**Q: Why does the generator spoil rows on purpose?**
A: To test the cleaning rules. `spoil()` puts `""`, `"-5"`, `"unknown"`
or `"NaN"` into one of `price`, `event_type`, `user_id`, `event_time`. It
prints how many it spoiled per file, so the expected row count is exact:
rows written minus rows spoiled. In TC-04 that is 2,000 − 385 = 1,615.

**Q: Why `--seed`?**
A: `random.seed(args.seed)` makes the same "random" values every run, so
a test can be repeated. TC-01 and TC-04 used `--seed 42`. (The
`event_id`s still differ: `uuid.uuid4()` does not use Python's `random`.)

**Q: Why is `event_id` a UUID made by the generator?**
A: It is the natural key. Every event has one unique id from the moment it
exists, so the database can tell a replayed row from a new one. A
database `SERIAL` id could not do that: a replay would get a new number.

## Streaming job (`spark_streaming_to_postgres.py`)

**Q: Why read every column as a string?**
A: `RAW_SCHEMA` is all `StringType`. In Spark 4, ANSI mode is on, so
`CAST('unknown' AS INT)` throws and would stop the stream. Reading text
and then using `try_cast` turns a bad value into null, and the filter
drops the row.

**Q: Why must you give a schema at all?**
A: A streaming source cannot scan the files first to guess types, because
the files don't exist yet when the query starts. `readStream.schema(...)`
is required for file sources.

**Q: Walk me through the filter.**
A: `clean()` keeps a row only if `event_id` and `event_time` are not null,
`user_id > 0`, `product_id > 0`, `event_type` is one of `view`,
`add_to_cart`, `purchase`, `price >= 0`, and `quantity` is null or
`>= 0`. A comparison with null is not true, so `> 0` and `>= 0` also
reject nulls.

**Q: Tell me about the bug you found.**
A: TC-04 first gave 1,642 rows, not 1,615. All 27 extra rows had
`user_id = -5`. The filter only checked "is not null", and `"-5"` casts to
a valid integer. Fix: `> 0` in Spark, plus `CHECK (user_id > 0)` and
`CHECK (product_id > 0)` in the table. After the fix: 1,615 rows, 0
breaking a rule. The bad run is kept in `reports/logs/bug_run/`.

**Q: Why `dropDuplicates` in Spark if the primary key already stops
duplicates?**
A: Two layers. `dropDuplicates` removes repeats inside the stream before
they cost a JDBC write: in TC-07 a copied file of 98 rows produced 0
rows to write. The primary key catches anything older than the 10-minute
watermark, and replays.

**Q: Why is `event_time` in the dedup keys?**
A: `.dropDuplicates(["event_id", "event_time"])` with
`.withWatermark("event_time", "10 minutes")`. Spark can only forget an id
once it knows its event time is older than the watermark. Without the
watermark column in the keys, state would grow forever.

**Q: Why `foreachBatch`?**
A: There is no streaming JDBC sink. `foreachBatch` hands me each batch
as a normal DataFrame, so I can use the batch JDBC writer and then run
my own SQL with psycopg2.

**Q: Why `persist()` in `write_batch`?**
A: `batch_df` is used twice: `count()` and the JDBC write. Without
`persist()`, Spark would read and clean the files twice. `unpersist()`
frees it at the end.

**Q: Why `spark.sql.shuffle.partitions = 4`?**
A: `dropDuplicates` shuffles. The default 200 partitions would mean 200
tiny tasks for ~100 rows. 4 suits a laptop. Note it is fixed in the
checkpoint once the query has run.

**Q: Why set the session time zone to UTC?**
A: The generator writes UTC times without a zone. With
`spark.sql.session.timeZone = UTC`, `try_to_timestamp` reads them as UTC.

## PostgreSQL (`postgres_setup.sql`)

**Q: Why a staging table?**
A: Spark's JDBC writer only does plain `INSERT`. On a replay it would hit
the primary key and fail the batch. So I append to `events_staging`,
which has no constraints, then run `INSERT ... SELECT ... ON CONFLICT
(event_id) DO NOTHING` and `TRUNCATE` in one transaction.

**Q: Why `TIMESTAMPTZ`?**
A: With plain `TIMESTAMP`, times were stored 2 hours late: the JDBC
driver converts with the JVM's local zone (UTC+2), not Spark's UTC
session zone. `TIMESTAMPTZ` stores an absolute instant. TC-03: values
match the CSV exactly with `SET timezone='UTC'`.

**Q: Why `CHECK` constraints when Spark already filters?**
A: Defence in depth. The TC-04 bug showed that the Spark filter can be
wrong. The database is the last place a bad row can be stopped, and it
also protects against any other writer.

**Q: Why `NUMERIC(10,2)` for price, not `FLOAT`?**
A: Money must be exact. A float cannot store 0.10 exactly; `NUMERIC`
can. Spark casts to `DECIMAL(10,2)` to match.

**Q: What are the two indexes for?**
A: `idx_events_time` and `idx_events_type`, for reports that filter by
time range or event type. The primary key index on `event_id` is also
what makes `ON CONFLICT` fast.

**Q: Is the setup script safe to run twice?**
A: Yes. `CREATE DATABASE` and `CREATE ROLE` have no `IF NOT EXISTS`, so
the script builds the command with `SELECT ... WHERE NOT EXISTS` and runs
it with `\gexec` only when needed. Tables and indexes use
`IF NOT EXISTS`.

**Q: What can `spark_app` do?**
A: `SELECT, INSERT` on `events`; `SELECT, INSERT, TRUNCATE` on
`events_staging`. Nothing else: no update, delete, drop. It is not a
superuser. The password is only in the gitignored `.env`.

## Reliability

**Q: What happens if the job crashes mid-batch?**
A: The batch has an `offsets` file but no `commits` file. On restart
Spark re-runs it. If the merge had already committed, `ON CONFLICT DO
NOTHING` skips every row. Tested with `taskkill /F`: all 1,176 valid
rows arrived, no duplicates.

**Q: Prove the replay is harmless.**
A: TC-05. I deleted `checkpoint/events/commits/42` and restarted. Spark
re-ran batch 42: 83 rows, 0 inserted. Total stayed 3,190, distinct ids
3,190.

**Q: What if the database is down?**
A: TC-08. With `PGPORT=5499` (nothing listening) the first psycopg2
call raised `OperationalError: ... Connection refused`, the query
stopped with exit code 1, and batch 44 had an offset but no commit.
Restarted with normal settings: batch 44 re-ran, then 45 and 46. Total
reached the expected 3,428, no duplicates.

**Q: Does the job retry by itself?**
A: No. It stops, and the next start recovers. In production a supervisor
(a service manager, Kubernetes, Airflow) would restart it.

**Q: What if a file arrives twice?**
A: TC-07. Spark sees a new file name and reads it, but `dropDuplicates`
recognises every id: 98 rows read, 0 written. If it came more than 10
minutes later, the primary key would skip the rows instead.

## Testing (`reports/test_cases.md`)

**Q: How did you know the expected row count exactly?**
A: The generator prints the spoiled count per file, and every kind of
spoiling breaks a rule. So rows in PostgreSQL must equal rows generated
minus rows spoiled. For P2: 6,000 − 124 = 5,876, and 5,876 landed.

**Q: Which test failed?**
A: TC-04 first (the `-5` bug), and TC-09 at P1's settings (fell behind).
I kept both failures in the document with the cause and the fix.

**Q: Are the tests automated?**
A: No, they are a manual plan with real results, which is what the spec
asks for. The checks are SQL queries I can rerun. Automating them with
pytest and a throwaway database would be the next step.

## Performance (`reports/performance_metrics.md`)

**Q: What is your latency?**
A: Run P2 (50 rows/s, 5 s trigger, 5 files per batch): p50 5.4 s, p95
13.4 s, max 15.4 s. Measured as `ingested_at − event_time` in
PostgreSQL, so it covers the whole path.

**Q: Why did P1 fall behind?**
A: One file every 2 s arrived; at most one file per 5 s was processed.
2.5 files in for every one out. After 2 minutes 24 of 60 files were
waiting and latency reached 116 s. Each batch took only ~3 s of its 5 s,
so the cap was the bottleneck, not the machine.

**Q: Did Spark warn you?**
A: Only once per run, for the slow first batch: `Current batch is falling
behind`. That warning means one batch took longer than the trigger. It
says nothing about a growing backlog. Latency is the real signal.

**Q: What is your throughput?**
A: P3 at 1,000 rows/s: 902 rows/s landed, 2,495 rows/s processed by
Spark inside a batch. Batches of ~10,000 rows took 3.7 s, batches of
~100 rows 3.0 s: almost all fixed cost.

**Q: Why is `numInputRows` lower than the rows written?**
A: Spark pushes not-null checks into the CSV reader, so some bad rows are
dropped before they're counted: 5,984 of 6,000 in P2.

## Deliverables

**Q: Is everything the spec lists there?**
A: Yes, nine items: `data_generator.py`, `spark_streaming_to_postgres.py`,
`postgres_setup.sql`, `postgres_connection_details.txt`,
`reports/project_overview.md`, `reports/user_guide.md`, `reports/test_cases.md`,
`reports/performance_metrics.md`, `reports/system_architecture.png`. The diagram is drawn
by `src/diagram.py`, so it can be regenerated.

**Q: The spec says `postgres_connection_details.txt` has the password.
Does it?**
A: It has host, port, database, user, the JDBC URL and the psql command,
and says where the password is instead of the password itself: it is
chosen when `postgres_setup.sql` runs (`-v app_password=...`) and kept
only in `.env`, which is gitignored. `.env.example` shows the variable
names with `PGPASSWORD=` empty. A password in a committed text file would
be a leak.

**Q: How would someone else run it?**
A: [reports/user_guide.md](../reports/user_guide.md): install, run
`postgres_setup.sql` with psql, copy `.env.example` to `.env`, start the
job, start the generator, check with psql.

## Weak spots to admit honestly

Say these before you are asked. Each one with the fix.

| Weak spot | Why it matters | What I would do |
|---|---|---|
| **Single machine** | Generator, Spark and PostgreSQL shared one laptop's CPU and disk. Numbers won't transfer to a cluster | Rerun on a cluster with a separate DB server |
| **2-minute runs, one per setting** | Can't show slow growth (dedup state, folder listing) or run-to-run spread | Hour-long runs, three repeats each |
| **`MAX_FILES=1` default falls behind at 50 rows/s** | Someone running the defaults under load gets growing latency | Kept for readable tests. Document it (done); for real use `MAX_FILES=5` or more |
| **File source, not Kafka** | Folder listing gets slower as files pile up; no partitions, no consumer groups | Swap `read_stream()` to `spark.readStream.format("kafka")`; archive processed files (`cleanSource`) meanwhile |
| **TC-08 used a wrong port, not a real outage** | Proves the "can't connect" path. Not a server dying mid-transaction | Stop the PostgreSQL service during a batch on a test server |
| **Staging table assumes one writer** | Two jobs would `TRUNCATE` each other's rows | One staging table per job, or a temp table per batch, or a `batch_id` column |
| **Rejected rows are dropped silently** | Nobody can see *which* rows failed or why; only the count gap shows it | A dead-letter table (below) |
| **No automatic restart** | After TC-08 the job stayed down until started by hand | Run it under a supervisor with restart-on-failure |
| **psycopg2 `with connect()` doesn't close** | In psycopg2, the `with` block commits or rolls back but leaves the connection open until garbage collection | Call `conn.close()` explicitly, or use a connection pool |
| **Late events beyond 10 minutes** | `dropDuplicates` with a watermark drops rows older than the watermark | Widen the watermark, or dedup only in PostgreSQL |

**How to add a dead-letter table.** "Dead letter" means rows that could
not be delivered, kept aside for inspection.

1. In `clean()`, keep the typed rows and compute a reason instead of only
   filtering:

   ```python
   reason = (F.when(F.col("event_time").isNull(), "bad event_time")
              .when(~(F.col("user_id") > 0) | F.col("user_id").isNull(), "bad user_id")
              .when(~F.col("event_type").isin(EVENT_TYPES), "bad event_type")
              .when(F.col("price").isNull() | (F.col("price") < 0), "bad price"))
   ```

2. Keep the raw text too (read it before the casts), so you can see the
   original value.
3. In `write_batch`, split the batch: rows with `reason` null go to the
   normal path; the rest are appended by JDBC to an `events_rejected`
   table (`raw_line TEXT, reason TEXT, batch_id BIGINT, rejected_at
   TIMESTAMPTZ DEFAULT now()`).
4. Grant `spark_app` `INSERT` on it.
5. New test: the count in `events_rejected` must equal the generator's
   spoiled count (minus rows Spark dropped in the reader).

## Self-check

1. In one sentence, why is a replayed batch harmless? [`event_id` is the
   primary key and the merge uses `ON CONFLICT (event_id) DO NOTHING`.]
2. Give P2's latency numbers. [p50 5.4 s, p95 13.4 s, max 15.4 s.]
3. Name two weak spots and their fixes. [E.g. single writer on staging →
   per-job staging table; silent drops → dead-letter table.]
4. What did TC-05 delete, and what did the replay insert?
   [`checkpoint/events/commits/42`; 83 rows read, 0 inserted.]
