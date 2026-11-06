# 05 — PostgreSQL, JDBC and the Merge

The database side of the pipeline: `postgres_setup.sql` block by block, what
JDBC is, and why each micro-batch is written in two steps (staging table, then
`INSERT ... ON CONFLICT DO NOTHING`) instead of one.

> **Help:** how to run the script and fill `.env` is in [01 · Setup](01-setup-postgres-and-spark.md#h-5-create-the-database-role-and-tables). The Python side of the write is in [04 · The streaming job line by line](04-streaming-job-line-by-line.md#h-6-write-batch-the-sink).

## 1. How the script is run

```sql
-- Run once as a superuser with psql (it uses psql features: -v variables, \gexec, \connect):
--   psql -h localhost -p 5432 -U postgres -v app_password='pick-a-password' -f postgres_setup.sql
-- Put the same password in .env as PGPASSWORD. Safe to re-run: every step checks first.

\set ON_ERROR_STOP on
```

The file mixes two languages:

- **SQL**, which the server runs (`CREATE TABLE`, `GRANT`...).
- **psql meta-commands**, which start with a backslash and are handled by the
  `psql` client itself (`\set`, `\gexec`, `\connect`). They do not work in
  pgAdmin's Query Tool.

`\set ON_ERROR_STOP on` makes psql stop at the first error. Without it, psql
prints the error and carries on with the next statement, which can leave half
a setup behind.

## 2. Creating the database: the \gexec trick

```sql
SELECT 'CREATE DATABASE ecommerce'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'ecommerce')\gexec
```

**The problem:** `CREATE TABLE` has `IF NOT EXISTS`, but `CREATE DATABASE`
does not. And `CREATE DATABASE` cannot run inside a function or a `DO` block,
so the usual "IF ... THEN CREATE" code is not possible.

**The trick, read from the inside out:**

1. `pg_database` is a system catalog: a table listing every database.
2. The `SELECT` returns the **text** `'CREATE DATABASE ecommerce'` as one row,
   but only if no database of that name exists. Otherwise it returns zero rows.
3. `\gexec` (instead of `;`) tells psql: run the query, then **execute each
   value in the result as an SQL statement**.

So: database missing → one row → psql runs `CREATE DATABASE ecommerce`.
Database present → no rows → nothing runs. Re-running the script is safe.

## 3. The application role and %L quoting

```sql
SELECT format('CREATE ROLE spark_app LOGIN PASSWORD %L', :'app_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'spark_app')\gexec
```

A **role** is PostgreSQL's word for a user or group. `LOGIN` means it can
connect. The pipeline gets its own role so Spark never runs as the
superuser `postgres` (see [§10](#h-10-grants-and-least-privilege)).

Same `\gexec` pattern, plus two kinds of quoting:

| Piece | Who handles it | Result |
|---|---|---|
| `:'app_password'` | **psql**, before sending | replaced by the `-v app_password=...` value as a quoted SQL string: `'pick-a-password'` |
| `format('... %L', value)` | **the server** | `%L` inserts the value as a correctly quoted *literal*. A password like `it's` becomes `'it''s'` (the inner quote doubled) |

`CREATE ROLE ... PASSWORD` does not accept a query parameter, so the password
has to be pasted into the command text. `%L` makes that safe: whatever the
password contains, it cannot break out of the string and run other SQL (that
attack is called *SQL injection*).

Because of `WHERE NOT EXISTS`, re-running the script with a different password
does **not** change the existing role's password. Use
`ALTER ROLE spark_app PASSWORD '...';` for that.

## 4. \connect

```sql
\connect ecommerce
```

Everything so far ran in the default `postgres` database. `\connect` makes
psql switch to `ecommerce`, so the tables below are created there. (In
PostgreSQL a connection belongs to one database; you cannot `USE` another
like in MySQL.)

## 5. The events table

```sql
CREATE TABLE IF NOT EXISTS events (
    event_id     TEXT PRIMARY KEY,           -- natural key from the generator: makes re-delivery harmless
    event_time   TIMESTAMPTZ NOT NULL,       -- when the user acted (TIMESTAMPTZ = an absolute instant)
    user_id      INT NOT NULL CHECK (user_id > 0),
    session_id   TEXT,
    event_type   TEXT NOT NULL CHECK (event_type IN ('view', 'add_to_cart', 'purchase')),
    product_id   INT NOT NULL CHECK (product_id > 0),
    category     TEXT,
    price        NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    quantity     INT CHECK (quantity >= 0),
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()   -- when the row landed: latency = ingested_at - event_time
);
```

| Column | Type | Why this type |
|---|---|---|
| `event_id` | `TEXT PRIMARY KEY` | the UUID from the generator. **PRIMARY KEY** = unique and not null, backed by an index. This one line is what makes duplicates impossible |
| `event_time` | `TIMESTAMPTZ` | an absolute moment ([§6](#h-6-timestamptz-vs-timestamp-the-2-hour-story)) |
| `user_id`, `product_id`, `quantity` | `INT` | whole numbers, matching Spark's `INT` |
| `session_id`, `event_type`, `category` | `TEXT` | strings of any length. In PostgreSQL `TEXT` is as fast as `VARCHAR(n)` |
| `price` | `NUMERIC(10,2)` | exact decimal for money, matching Spark's `DECIMAL(10,2)`. Never use `REAL`/`FLOAT` for money: 0.1 + 0.2 is not exactly 0.3 in floating point |
| `ingested_at` | `TIMESTAMPTZ DEFAULT now()` | filled by the database when the row is inserted. The merge does not list it, so the default applies |

`now()` is the start time of the current transaction. All rows of one merge
share the same `ingested_at`, which is fine: they landed together.
`ingested_at - event_time` is the end-to-end **latency** used in
[08 · Performance and tuning](08-performance-and-tuning.md).

## 6. TIMESTAMPTZ vs TIMESTAMP: the +2 hour story

PostgreSQL has two timestamp types:

| Type | Stores | Shown as |
|---|---|---|
| `TIMESTAMP` (without time zone) | a wall-clock reading, like a photo of a clock with no idea which city it was in | exactly as stored |
| `TIMESTAMPTZ` (with time zone) | an **absolute instant** (internally in UTC) | converted to the session's `timezone` setting |

**What happened earlier in the week.** The first version of the table used
plain `TIMESTAMP`. The generator wrote `10:35:40` UTC; Spark read it as UTC
(session time zone set to UTC). But the rows in PostgreSQL said `12:35:40`:
two hours later.

**Why.** For a plain `TIMESTAMP` column, the PostgreSQL JDBC driver sends the
value as the wall-clock time **in the JVM's local time zone**. This laptop's
JVM runs in South Africa time, UTC+2. The instant "10:35:40 UTC" is
"12:35:40" on a South African clock, so that is what was stored, with nothing
to say it was local time. Every latency measured from it would have been
off by 2 hours.

**The fix.** Make the column `TIMESTAMPTZ`. Now the driver sends an instant
with its offset, and PostgreSQL stores the true moment. TC-03 checked it:

```sql
SET timezone = 'UTC';
SELECT event_time, price FROM events LIMIT 3;
-- event_time matches the CSV strings exactly, no 2-hour shift
```

Without the `SET`, psql shows the same instants in the server's zone, e.g.
`2026-11-04 12:35:40.222+02`. The `+02` tells you it is the same moment, just
displayed differently.

**Rule of thumb:** store event times as `TIMESTAMPTZ`, generate them in UTC,
and set Spark's session time zone to UTC.

## 7. CHECK constraints

A `CHECK (condition)` makes the database refuse any row for which the
condition is false. The table repeats the Spark job's rules:

| Spark filter | PostgreSQL constraint |
|---|---|
| `user_id > 0` | `user_id INT NOT NULL CHECK (user_id > 0)` |
| `product_id > 0` | `product_id INT NOT NULL CHECK (product_id > 0)` |
| `event_type.isin(...)` | `CHECK (event_type IN ('view', 'add_to_cart', 'purchase'))` |
| `price >= 0` | `price ... NOT NULL CHECK (price >= 0)` |
| `quantity` null or `>= 0` | `quantity INT CHECK (quantity >= 0)` (a CHECK on null passes) |

Why say it twice? **Defence in depth.** TC-04 found that the Spark filter let
27 rows with `user_id = -5` through. The `user_id > 0` and `product_id > 0`
CHECKs were added in the same fix, so even if a future code change weakens
the filter, the database still refuses bad rows. The filter keeps bad rows
out quietly; the CHECK is the last line that cannot be bypassed.

Note: a CHECK violation in the merge would fail the whole `INSERT ... SELECT`,
the batch would fail, and the job would stop. That is loud, which is right
for "the code has a bug". The Spark filter is there so this never happens
in normal running.

## 8. The staging table

```sql
CREATE TABLE IF NOT EXISTS events_staging (LIKE events INCLUDING DEFAULTS EXCLUDING CONSTRAINTS);
```

`LIKE events` copies the column names and types. `INCLUDING DEFAULTS` copies
`DEFAULT now()`. `EXCLUDING CONSTRAINTS` leaves out the CHECKs. No indexes are
copied either (that would need `INCLUDING INDEXES`), so there is **no primary
key**: the same `event_id` may appear twice in staging. (PostgreSQL always
copies `NOT NULL` with `LIKE`; that is harmless because Spark never sends nulls
in those columns.)

The comment in the script states the purpose: *so the bulk JDBC insert never
fails half way; the merge into events applies the rules.* Section 12 explains
why that matters.

## 9. Indexes

```sql
CREATE INDEX IF NOT EXISTS idx_events_time ON events (event_time);
CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);
```

An **index** is a sorted lookup structure that lets PostgreSQL find rows
without reading the whole table. `events` has three:

| Index | Made by | Used for |
|---|---|---|
| `events_pkey` on `event_id` | `PRIMARY KEY`, automatically | uniqueness, and the `ON CONFLICT (event_id)` check (it needs a unique index on that column) |
| `idx_events_time` | the script | reports by time range, `ORDER BY event_time` |
| `idx_events_type` | the script | reports filtering by `event_type` |

The cost: every insert also updates three indexes. With merges of 100–10,000
rows taking 56–145 ms, that cost is small here.

## 10. GRANTs and least privilege

```sql
GRANT CONNECT ON DATABASE ecommerce TO spark_app;
GRANT USAGE ON SCHEMA public TO spark_app;
GRANT SELECT, INSERT ON events TO spark_app;
GRANT SELECT, INSERT, TRUNCATE ON events_staging TO spark_app;
```

**Least privilege** = give a program only the rights its job needs.

| Grant | Why `spark_app` needs it |
|---|---|
| `CONNECT` on the database | to log in to `ecommerce` |
| `USAGE` on schema `public` | to see objects inside the schema |
| `SELECT, INSERT` on `events` | insert via the merge; select for checks and `perf_report.py` |
| `SELECT, INSERT, TRUNCATE` on `events_staging` | JDBC inserts, the merge reads it, `write_batch` empties it |

What it does **not** get: `UPDATE`, `DELETE` or `TRUNCATE` on `events`, and no
right to create or drop tables. A bug in the job cannot wipe or change stored
events. The tables are owned by `postgres`. A side effect: resetting the lab
(`TRUNCATE events`) must be done as `postgres`
([01](01-setup-postgres-and-spark.md#h-reset-to-a-clean-slate)).

## 11. What JDBC is

**JDBC** (Java Database Connectivity) is Java's standard way to talk to a
database. Every database vendor ships a **JDBC driver**: a jar file that
speaks that database's network protocol. Spark runs on Java, so its built-in
database reader and writer use JDBC.

The pieces in this job:

| Piece | Value |
|---|---|
| driver jar | `org.postgresql:postgresql:42.7.4`, fetched by `spark.jars.packages` |
| driver class | `org.postgresql.Driver` (the `driver` option) |
| URL | `jdbc:postgresql://localhost:5432/ecommerce` |
| target | `dbtable = events_staging`, `mode("append")` |

When Spark writes a DataFrame through JDBC, each **partition** (piece of the
DataFrame) opens its own connection and inserts its rows in batches, in its
own transaction. That is fast, but it is **not all-or-nothing**: if one
partition fails, others may already have committed.

The Python side (truncate, merge) uses **psycopg2** instead. It is simpler for
running a few SQL statements from the driver process.

## 12. Why staging + merge, not a plain append

The simplest sink would be: JDBC-append each batch straight into `events`.
Here is why that is not good enough.

**Problem 1: replays.** Spark promises *at-least-once* delivery to
`foreachBatch`: after a crash it may run the same batch again
([06](06-reliability-checkpoints-exactly-once.md)). A plain append of a
replayed batch into `events` hits the primary key: `duplicate key value
violates unique constraint "events_pkey"`. The batch fails, the query stops,
the restart replays the same batch, it fails again. **Stuck for ever.**
Without the primary key it would not get stuck; it would silently store
duplicates. Spark's JDBC writer has no "skip duplicates" option.

**Problem 2: partial writes.** As above, a JDBC append is several separate
transactions. A failure halfway leaves some of the batch written.

**The two-step design solves both:**

```
batch_df ──JDBC append──> events_staging      (no PK, no CHECKs: never fails on content)
                               │
                               │  one transaction:
                               │    INSERT INTO events (...) SELECT ... FROM events_staging
                               │      ON CONFLICT (event_id) DO NOTHING;
                               │    TRUNCATE events_staging;
                               ▼
                            events             (PK + CHECKs)
```

```sql
INSERT INTO events (event_id, event_time, user_id, session_id, event_type, product_id, category, price, quantity)
SELECT event_id, event_time, user_id, session_id, event_type, product_id, category, price, quantity FROM events_staging
ON CONFLICT (event_id) DO NOTHING
```

- **`INSERT ... SELECT`** copies rows from one table to another inside the
  database, in one statement.
- **`ON CONFLICT (event_id) DO NOTHING`**: for each row, if a row with that
  `event_id` already exists in `events`, skip it silently instead of
  raising an error. This is called an **idempotent** write: running it twice
  has the same effect as running it once.
- A partial JDBC write only ever reaches staging, and the next attempt starts
  with `TRUNCATE events_staging`, so leftovers are cleared.

The proof is TC-05: batch 42 was forced to run twice.

```json
{"batch_id": 42, "rows": 83, "inserted": 83, "skipped_duplicates": 0, ...}
{"batch_id": 42, "rows": 83, "inserted": 0, "skipped_duplicates": 83, ...}
```

Total stayed 3,190 and distinct ids stayed 3,190.

## 13. Transactions in write_batch

A **transaction** is a group of statements that succeed or fail as one unit:
either all of their changes are saved (**commit**) or none are
(**rollback**). In psycopg2, `with psycopg2.connect(...) as conn:` opens a
transaction and commits it at the end of the block, or rolls back if an
exception escapes.

`write_batch` has two such blocks:

| Block | Statements | Why grouped this way |
|---|---|---|
| first | `TRUNCATE events_staging` | clear leftovers before the JDBC load |
| second | merge + `TRUNCATE events_staging` | the rows enter `events` **and** staging is emptied, together. If the merge fails, staging keeps its rows and nothing reaches `events`; the replay truncates and reloads |

In PostgreSQL `TRUNCATE` is transactional, so a rollback undoes it too.

## 14. Queries to check the results

Open psql on the project database:

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -U spark_app -d ecommerce
```

```sql
\d events                          -- columns, types, CHECKs, indexes
SET timezone = 'UTC';              -- show times as the generator wrote them

-- Totals and duplicates: the two numbers must be equal
SELECT count(*) AS total, count(DISTINCT event_id) AS distinct_ids FROM events;

-- Rows breaking any rule: must be 0
SELECT count(*) FROM events
WHERE user_id <= 0 OR product_id <= 0 OR price < 0 OR quantity < 0
   OR event_type NOT IN ('view', 'add_to_cart', 'purchase');

-- Staging must be empty between batches
SELECT count(*) FROM events_staging;

-- Latest arrivals and their latency
SELECT event_id, event_time, ingested_at, ingested_at - event_time AS latency
FROM events ORDER BY ingested_at DESC LIMIT 5;

-- Mix of event types (about 75 / 17 / 8 %)
SELECT event_type, count(*), round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM events GROUP BY event_type ORDER BY 2 DESC;

-- Latency percentiles, the way src/perf_report.py computes them
SELECT percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at - event_time)) AS p50_s,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at - event_time)) AS p95_s
FROM events;
```

After TC-04's fix these gave 1,615 / 1,615, 0 rule-breakers, and an empty
staging table. After TC-08 the total was 3,428, with no duplicates.

## Self-check

1. Why does `CREATE DATABASE` need the `\gexec` trick? (It has no
   `IF NOT EXISTS` and cannot run inside a `DO` block; `\gexec` runs the
   command text only when the `SELECT` returns a row.)
2. What does `%L` protect against? (Broken quoting and SQL injection when the
   password is pasted into the `CREATE ROLE` text.)
3. Why were times shifted by +2 hours with `TIMESTAMP`? (The JDBC driver
   wrote the wall-clock time in the JVM's zone, UTC+2; `TIMESTAMPTZ` stores
   the true instant.)
4. Why can't the batch be JDBC-appended straight into `events`? (A replay hits
   the primary key and fails for ever, and a JDBC append is not all-or-nothing.)
5. Which rights does `spark_app` lack on `events`, and why? (`UPDATE`,
   `DELETE`, `TRUNCATE`: a bug in the job cannot change or wipe stored data.)
