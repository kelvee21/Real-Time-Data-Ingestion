# 12 — Streaming API and SQL Reference (A–Z)

Every PySpark function, method and option, and every SQL and psql keyword
used in this project's code, in alphabetical order. Each entry gives the
form used here, what it does, the line from the project, and one gotcha.
Use your browser's find (Ctrl+F) to jump to a word.

> **Help:** the code is read in order in [04 · The streaming job line by line](04-streaming-job-line-by-line.md) and [05 · PostgreSQL, JDBC and the merge](05-postgres-jdbc-and-the-merge.md). Concepts behind the entries are in [09 · The hard parts](09-hard-concepts-explained.md).

Files: **job** = `spark_streaming_to_postgres.py`, **gen** =
`data_generator.py`, **setup** = `postgres_setup.sql`, **perf** =
`src/perf_report.py`.

---

## Part 1: PySpark (streaming and DataFrame)

### Building the session

| Name | As used | What it does | Line in the project | Gotcha |
|---|---|---|---|---|
| `SparkSession.builder` | `SparkSession.builder` | Starts describing a Spark session | job `build_spark()` | Nothing runs until `getOrCreate()` |
| `.appName(name)` | `.appName("ecommerce-events-to-postgres")` | Name shown in the Spark UI and logs | job | Not the query name; that is `queryName` |
| `.master(url)` | `.master("local[*]")` | Where Spark runs. `local[*]` = this machine, all cores | job | `local` without `[*]` uses one core only |
| `.config(key, value)` | `.config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")` | Downloads the JDBC driver from Maven at start-up | job | Needs internet the first time; cached in `~/.ivy2.5.2` after |
| `spark.sql.shuffle.partitions` | `.config(..., "4")` | Partitions after a shuffle (`dropDuplicates`) | job | Default 200. Saved in the checkpoint: a stateful query keeps the old value |
| `spark.sql.session.timeZone` | `.config(..., "UTC")` | Zone for parsing and showing timestamps in Spark SQL | job | Does **not** change the JVM zone the JDBC driver uses (the +2 h story) |
| `.getOrCreate()` | `.getOrCreate()` | Returns the running session or starts one (~25 to 30 s here) | job | If a session already exists, new `.config` values may be ignored |
| `sparkContext.setLogLevel(level)` | `spark.sparkContext.setLogLevel("WARN")` | Hides INFO lines | job | Messages printed before this line still appear |
| `spark.stop()` | `spark.stop()` | Shuts Spark down | job `main()` | On Windows it prints a harmless "Failed to delete ... postgresql-42.7.4.jar" error |

### Schema types

| Name | As used | What it does | Line | Gotcha |
|---|---|---|---|---|
| `StructType([...])` | `StructType([StructField(c, StringType()) for c in COLS])` | A table schema: an ordered list of fields | job `RAW_SCHEMA` | Spark matches schema columns to CSV columns by **position**, not by header name. `COLS` must be in the same order as the generator's `FIELDS` |
| `StructField(name, type)` | `StructField(c, StringType())` | One column. Nullable by default | job | — |
| `StringType()` | `StringType()` | Text type | job | Reading as text is deliberate: casts come later with `try_cast` |

### Reading the stream

| Name | As used | What it does | Line | Gotcha |
|---|---|---|---|---|
| `spark.readStream` | `spark.readStream` | Starts a streaming read (vs `spark.read` for batch) | job `read_stream()` | Returns a reader, not data |
| `.schema(s)` | `.schema(RAW_SCHEMA)` | Gives the column types up front | job | **Required** for streaming file sources: there are no files to infer from yet |
| `.option("header", True)` | `.option("header", True)` | First line of each file is column names, skip it | job | Every file has its own header (`writer.writeheader()` in gen) |
| `.option("maxFilesPerTrigger", n)` | `.option("maxFilesPerTrigger", MAX_FILES)` | Most files per micro-batch | job | `MAX_FILES=1` fell behind at 50 rows/s (P1). See 08 |
| `.csv(path)` | `.csv(INPUT_DIR)` | Watch this folder for CSV files | job | Ignores files starting with `_` or `.`; the folder must exist (hence `os.makedirs`) |

### Transforming

| Name | As used | What it does | Line | Gotcha |
|---|---|---|---|---|
| `F` | `from pyspark.sql import functions as F` | The module of column functions | job | Convention; `F.col`, not `col` |
| `.withColumn(name, expr)` | `.withColumn("user_id", F.expr("try_cast(user_id AS INT)"))` | Adds or replaces a column | job `clean()` | Same name = replace. Each call adds a step to the plan |
| `F.expr(sql)` | `F.expr("try_cast(price AS DECIMAL(10,2))")` | Writes a column as a SQL string | job | Typos are only caught when the query starts |
| `try_cast(x AS T)` | `try_cast(user_id AS INT)` | Cast that returns null on a bad value | job | `"-5"` is a *good* INT: it passes (TC-04 bug) |
| `try_to_timestamp(s, fmt)` | `try_to_timestamp(event_time, 'yyyy-MM-dd HH:mm:ss.SSS')` | Text to timestamp with a pattern; null if it does not match | job | Read in the session zone (UTC here). `SSS` = milliseconds |
| `DECIMAL(10,2)` | in `try_cast(price AS DECIMAL(10,2))` | Exact number, 10 digits, 2 after the point | job | `"NaN"` becomes null: decimals have no NaN |
| `F.lower(col)` / `F.trim(col)` | `F.lower(F.trim("event_type"))` | Lower-case / remove outer spaces | job | `" View "` becomes `"view"` and passes `isin` |
| `F.col(name)` | `F.col("user_id")` | Refers to a column | job | — |
| `.filter(cond)` | `typed.filter(...)` | Keeps rows where the condition is **true** | job | Null counts as not true, so the row is dropped |
| `&` / `|` / `~` | `F.col("event_id").isNotNull() & (F.col("user_id") > 0)` | AND / OR / NOT on columns | job | Wrap each comparison in brackets: `&` binds tighter than `>` |
| `.isNotNull()` / `.isNull()` | `F.col("quantity").isNull()` | Null tests | job | `== None` does not work on columns |
| `.isin(list)` | `F.col("event_type").isin(EVENT_TYPES)` | Value is in the list | job | Null is not in any list |
| `>`, `>=` on columns | `F.col("price") >= 0` | Comparison, gives true/false/null | job | `null >= 0` is null, so the row is dropped (wanted here) |
| `.withWatermark(col, delay)` | `.withWatermark("event_time", "10 minutes")` | Sets how late data may be; lets state be cleaned | job | Rows older than the watermark are dropped by `dropDuplicates` |
| `.dropDuplicates(cols)` | `.dropDuplicates(["event_id", "event_time"])` | Keeps the first row per key; in streaming, remembers keys in state | job | Include the watermark column in the keys, or state never shrinks |
| `.select(*cols)` | `batch_df.select(*COLS)` | Keeps these columns in this order | job `write_batch()` | `*COLS` unpacks the list into arguments |
| `.persist()` / `.unpersist()` | `batch_df.select(*COLS).persist()` | Keep the computed data in memory for reuse / free it | job | Without it, `count()` and `save()` would each re-read the files |
| `.count()` | `rows = batch_df.count()` | Number of rows. An **action**: runs the plan | job | — |

### Writing the stream

| Name | As used | What it does | Line | Gotcha |
|---|---|---|---|---|
| `.writeStream` | `clean(...).writeStream` | Starts describing the streaming output | job `main()` | Nothing runs until `.start()` |
| `.queryName(name)` | `.queryName("events_to_postgres")` | Name of the query in logs and errors | job | Shows in the TC-08 error: `Query events_to_postgres [id = ...]` |
| `.foreachBatch(fn)` | `.foreachBatch(write_batch)` | Calls `fn(batch_df, batch_id)` once per micro-batch | job | At-least-once: `fn` may run twice for the same `batch_id` |
| `.option("checkpointLocation", dir)` | `.option("checkpointLocation", CHECKPOINT_DIR)` | Where offsets, commits and state are kept | job | One per query. Never share; never edit by hand except in tests |
| `.trigger(processingTime=...)` | `.trigger(processingTime=TRIGGER)` | Start a batch every N seconds | job | A batch longer than N delays the next; Spark warns "falling behind" |
| `.start()` | `.start()` | Starts the query in the background; returns a `StreamingQuery` | job | Returns at once; the script must then wait |
| `query.awaitTermination(t)` | `query.awaitTermination(args.run_seconds)` | Waits for the query to end, or `t` seconds | job | Raises if the query failed (TC-08's `StreamingQueryException`) |
| `query.stop()` | `query.stop()` | Stops the query cleanly | job | Used by `--run-seconds` and Ctrl+C |

### Batch JDBC write (inside `foreachBatch`)

| Name | As used | What it does | Line | Gotcha |
|---|---|---|---|---|
| `.write.format("jdbc")` | `batch_df.write.format("jdbc")` | Batch writer to a database | job | No streaming version exists, hence `foreachBatch` |
| `.option("url", ...)` | `JDBC_URL = "jdbc:postgresql://localhost:5432/ecommerce"` | Which server and database | job | Built from `PGHOST`/`PGPORT`/`PGDATABASE` |
| `.option("dbtable", ...)` | `"events_staging"` | Target table | job | If missing, Spark tries `CREATE TABLE`, which `spark_app` may not do: good |
| `.option("user"/"password", ...)` | `PG["user"]`, `PG["password"]` | Login | job | From `.env`; never hard-code |
| `.option("driver", ...)` | `"org.postgresql.Driver"` | Java class of the driver | job | `ClassNotFoundException` = the package download failed |
| `.mode("append")` | `.mode("append")` | Add rows, keep existing ones | job | `"overwrite"` would drop and recreate the table |
| `.save()` | `.save()` | Runs the write (an action) | job | Each partition writes in its own transaction |

### Progress listener

| Name | As used | What it does | Line | Gotcha |
|---|---|---|---|---|
| `StreamingQueryListener` | `class ProgressLogger(StreamingQueryListener)` | Base class for query event callbacks | job | The callbacks run on the driver; keep them fast |
| `spark.streams.addListener(l)` | `spark.streams.addListener(ProgressLogger())` | Registers the listener | job `main()` | Add it before `.start()` to see the first batch |
| `onQueryStarted(event)` | prints `event.id` | Called once at start | job | — |
| `onQueryProgress(event)` | `json.loads(event.progress.json)` | Called after each batch with its report | job | Field names are camelCase: `numInputRows`, `durationMs` |
| `onQueryIdle(event)` | `pass` | Called when a trigger finds no new data | job | Added in Spark 3.5; defined here so every callback is covered |
| `onQueryTerminated(event)` | prints `event.exception` | Called when the query ends | job | `exception=None` = clean stop |

---

## Part 2: Python libraries used around Spark

| Name | As used | What it does | Line | Gotcha |
|---|---|---|---|---|
| `argparse` | `ap.add_argument("--rows", type=int, default=100)` | Command-line options | gen, job, perf | `--bad-rate` becomes `args.bad_rate` |
| `csv.DictWriter` | `writer.writeheader(); writer.writerows(events)` | Writes dicts as CSV rows | gen | Open with `newline=""` on Windows or you get blank lines |
| `datetime.now(timezone.utc)` | `.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]` | Current UTC time as text, ms precision | gen | `%f` is microseconds; `[:-3]` cuts to ms |
| `load_dotenv()` | `load_dotenv()` | Reads `.env` into environment variables | gen, job, perf | Does not overwrite variables already set, so `PGPORT=5499 python ...` wins |
| `os.getenv(k, default)` | `os.getenv("MAX_FILES", "1")` | Reads a setting | job | Always a string: hence `int(...)` |
| `os.replace(a, b)` | `os.replace(tmp_path, ...)` | Atomic rename (overwrites `b`) | gen | Atomic only on the same disk |
| `psycopg2.connect(**PG)` | `with psycopg2.connect(**PG) as conn, conn.cursor() as cur:` | Opens a connection; the `with` block is a transaction | job | `with` commits or rolls back but does **not** close the connection |
| `cur.execute(sql, params)` | `cur.execute(MERGE_SQL)` | Runs SQL | job, perf | Use `%s` parameters, never string formatting, for values |
| `cur.rowcount` | `inserted = cur.rowcount` | Rows affected by the last statement | job | For `ON CONFLICT DO NOTHING`: only rows actually inserted |
| `cur.fetchone()` | `n, p50, p95, worst, rate = cur.fetchone()` | One result row as a tuple | perf | Numbers come back as `Decimal`: hence `float(...)` |
| `random.choices(seq, weights)` | `random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]` | Weighted pick (75/17/8) | gen | Returns a list; `[0]` takes the item |
| `uuid.uuid4()` | `str(uuid.uuid4())` | Random unique id | gen | Not affected by `random.seed` |
| `statistics.mean` | `statistics.mean(trig)` | Average | perf | Includes the slow first batch |

---

## Part 3: SQL and psql

### Symbols and psql commands

| Item | As used | What it does | Line | Gotcha |
|---|---|---|---|---|
| `--` | `-- 1. Database.` | Comment to end of line | setup | — |
| `\set ON_ERROR_STOP on` | first line | psql stops at the first error | setup | Without it psql carries on after errors |
| `:'var'` | `:'app_password'` | Inserts a `-v` variable as a quoted literal | setup | Set with `psql -v app_password='...'` |
| `\gexec` | `SELECT 'CREATE DATABASE ecommerce' WHERE NOT EXISTS (...)\gexec` | Runs each result value as a SQL command | setup | No row = nothing runs: the "if not exists" trick |
| `\connect db` | `\connect ecommerce` | Switch database inside the script | setup | Plain SQL cannot switch databases |
| `\d table` | `\d events` | Describe columns, keys, indexes | TC-03 | psql only |
| `::type` | `round(x::numeric, 1)` | Cast | drills | `round(double, 1)` does not exist; cast to `numeric` |
| `%L` | `format('CREATE ROLE ... PASSWORD %L', ...)` | Quote as a literal inside `format()` | setup | Safe against quotes in the password |
| `%s` | `BETWEEN %s AND %s` | psycopg2 placeholder, not SQL | perf | Filled by the driver, safely quoted |

### psql options (command line)

| Option | Meaning | Example |
|---|---|---|
| `-h` / `-p` | host / port | `-h localhost -p 5432` |
| `-U` / `-d` | user / database | `-U postgres -d ecommerce` |
| `-v name=value` | set a variable | `-v app_password='...'` |
| `-f file` | run a file | `-f postgres_setup.sql` |
| `-c "sql"` | run one command | `-c "SELECT count(*) FROM events;"` |
| `-w` | never ask for a password (fail instead) | used in the drills |

### A–C

| Keyword | As used | What it does | Line | Gotcha |
|---|---|---|---|---|
| `AND` / `OR` | `WHERE user_id <= 0 OR product_id <= 0 ...` | Both true / either true | drill 5 | `AND` binds tighter than `OR`: add brackets when mixing |
| `avg(x)` | `round(avg(price), 2)` | Mean | drill 2 | Ignores nulls |
| `BETWEEN a AND b` | `WHERE ingested_at BETWEEN %s AND %s` | Inclusive range | perf | Inclusive at both ends |
| `CHECK (cond)` | `user_id INT NOT NULL CHECK (user_id > 0)` | Row rule; insert fails if false | setup | A null makes the check *unknown*, which passes: pair with `NOT NULL` |
| `count(*)` / `count(DISTINCT x)` | `count(*)`, `count(DISTINCT event_id)` | Rows / different values | perf, tests | `count(x)` skips nulls |
| `CREATE DATABASE` | built by `\gexec` | New database | setup | Has no `IF NOT EXISTS`; cannot run inside a transaction |
| `CREATE INDEX IF NOT EXISTS` | `idx_events_time ON events (event_time)` | Index for fast lookups by a column | setup | Speeds reads, slows each insert a little |
| `CREATE ROLE ... LOGIN PASSWORD` | `CREATE ROLE spark_app LOGIN PASSWORD %L` | A user that can log in | setup | Roles are server-wide, not per database |
| `CREATE TABLE IF NOT EXISTS` | `CREATE TABLE IF NOT EXISTS events (...)` | Create only if missing | setup | Does not alter an existing table to match |

### D–I

| Keyword | As used | What it does | Line | Gotcha |
|---|---|---|---|---|
| `date_trunc('minute', t)` | `date_trunc('minute', event_time)` | Cut a time down to the minute | drill 1 | Uses the session zone for day/hour boundaries |
| `DEFAULT now()` | `ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()` | Fill when the insert does not give a value | setup | `now()` = start of the **transaction**, same for every row in the merge |
| `DO NOTHING` | `ON CONFLICT (event_id) DO NOTHING` | Skip the conflicting row | job | Counts as 0 in `rowcount` |
| `EXCLUDING CONSTRAINTS` | in `LIKE events ...` | Copy columns but not CHECKs | setup | `NOT NULL` is still copied (it belongs to the column) |
| `extract(epoch FROM interval)` | `extract(epoch FROM ingested_at - event_time)` | Interval to seconds | perf | Returns `numeric` in PostgreSQL 14+ |
| `FILTER (WHERE ...)` | `count(*) FILTER (WHERE event_type = 'purchase')` | Aggregate only matching rows | drill 4 | — |
| `format(fmt, ...)` | `format('CREATE ROLE spark_app LOGIN PASSWORD %L', :'app_password')` | Build a string safely | setup | `%I` for names, `%L` for values |
| `GRANT ... ON ... TO` | `GRANT SELECT, INSERT ON events TO spark_app` | Give rights | setup | `CONNECT` on the database and `USAGE` on the schema are needed too |
| `greatest(a, b)` | `greatest(extract(...), 1)` | Larger value | perf | Avoids dividing by zero |
| `GROUP BY` | `GROUP BY category` / `GROUP BY 1, 2` | One output row per group | drills | Numbers refer to SELECT positions |
| `IN (...)` | `CHECK (event_type IN ('view', 'add_to_cart', 'purchase'))` | In the list | setup | — |
| `INCLUDING DEFAULTS` | in `LIKE events ...` | Copy `DEFAULT now()` too | setup | — |
| `INSERT INTO ... SELECT` | `INSERT INTO events (...) SELECT ... FROM events_staging` | Copy rows from a query | job `MERGE_SQL` | Column lists must line up by position |
| `INT` / `TEXT` / `NUMERIC(10,2)` | column types | 4-byte integer / any length text / exact decimal | setup | Use `NUMERIC` for money, never `float` |

### L–W

| Keyword | As used | What it does | Line | Gotcha |
|---|---|---|---|---|
| `LIKE table` | `CREATE TABLE ... events_staging (LIKE events ...)` | Copy another table's columns | setup | A copy at creation time; later changes to `events` do not follow |
| `LIMIT n` | `LIMIT 5` | First n rows | drills | Use with `ORDER BY` or the rows are arbitrary |
| `max` / `min` | `max(ingested_at) - min(ingested_at)` | Largest / smallest | perf | — |
| `NOT EXISTS (subquery)` | `WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'spark_app')` | True if the subquery has no rows | setup | `SELECT 1`: the value does not matter |
| `NOT NULL` | `event_time TIMESTAMPTZ NOT NULL` | Value required | setup | — |
| `ON CONFLICT (col)` | `ON CONFLICT (event_id)` | What to do when the unique key exists | job | Needs a unique index or PK on that column |
| `ORDER BY` | `ORDER BY revenue DESC` | Sort | drills | — |
| `percentile_cont(f) WITHIN GROUP (ORDER BY x)` | `percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(...))` | The value at fraction f of the sorted list, interpolated | perf | `percentile_disc` returns a real row value instead; returns `double precision` |
| `pg_database` / `pg_roles` | `SELECT 1 FROM pg_database WHERE datname = 'ecommerce'` | System catalogs of databases / roles | setup | — |
| `PRIMARY KEY` | `event_id TEXT PRIMARY KEY` | Unique + not null + index | setup | The base of idempotency |
| `round(x, n)` | `round(avg(price), 2)` | Round to n decimals | drills | Needs `numeric` |
| `SELECT` | everywhere | Read data | — | — |
| `SET timezone = 'UTC'` | before checking times | Display times in UTC for this session | TC-03, drills | Only display; stored instants do not change |
| `sum(x)` | `sum(price * quantity)` | Total | drill 2 | — |
| `TIMESTAMPTZ` | `event_time TIMESTAMPTZ NOT NULL` | Absolute instant | setup | Plain `TIMESTAMP` stored times +2 h off via JDBC |
| `TRUNCATE` | `TRUNCATE events_staging` | Empty a table instantly | job | Takes a strong lock; needs the `TRUNCATE` grant |
| `USAGE ON SCHEMA` | `GRANT USAGE ON SCHEMA public TO spark_app` | May see objects in the schema | setup | Without it, table grants are useless |
| `WHERE` | `WHERE event_type = 'purchase'` | Row filter | everywhere | — |
