# 02 — Batch vs Streaming: the Ideas Behind the Job

The concepts you need before reading the code: what "streaming" means,
how Spark turns a stream into small batches, and the words (trigger, source,
sink, output mode, event time) that appear on every later page.

> **Help:** the code that uses each idea is walked through in [04 · The streaming job line by line](04-streaming-job-line-by-line.md). Short definitions of every term are in [11 · Glossary, practice, drills](11-glossary-and-practice.md).

## Batch vs streaming

**Batch processing** means: the data is already complete, you run a program
over all of it once, and the program ends. Last week's ETL was batch: load
everything from the source tables, transform, write, done.

**Stream processing** means: the data never "finishes". New records keep
arriving, and the program runs forever, handling each new piece soon after it
arrives.

| | Batch | Streaming |
|---|---|---|
| Input | a fixed set (a table, a folder of yesterday's files) | an endless flow (new files, messages, clicks) |
| Program runs | once, then stops | continuously, until you stop it |
| Answer is | final when the job ends | updated again and again |
| Typical delay from event to result | hours (nightly job) | seconds |
| Hard parts | volume | remembering what was already done, duplicates, late data, crashes mid-way |

In this lab the "endless flow" is the generator dropping a new CSV file into
`data/incoming/` every 2 seconds. The job has to notice each new file, clean
its rows and put them in PostgreSQL within seconds.

## The unbounded table model

Spark Structured Streaming's big idea: **treat the stream as a table that keeps
growing**. It is called an *unbounded* table, because it has no last row.

```
 data/incoming/                     the "input table" Spark imagines
 ─────────────                      ─────────────────────────────────
 events_..._123540.csv  ──────>     row 1    ┐
                                    ...      │ file 1 (100 rows)
                                    row 100  ┘
 events_..._123542.csv  ──────>     row 101  ┐
                                    ...      │ file 2 (100 rows)
                                    row 200  ┘
 (next file arrives)    ──────>     row 201 ... (keeps growing)
```

You write your query **as if it were a normal query on a normal table**:
`filter`, `withColumn`, `dropDuplicates`. Spark works out how to run it
*incrementally*: each time new rows appear, it processes only those rows and
adds the result to the output. That is why `clean()` in our job looks exactly
like batch PySpark code. The only streaming-specific words are `readStream`
and `writeStream`.

## Micro-batches and triggers

Spark does not process rows one at a time. It wakes up regularly, collects
everything new since last time, and processes that as a small batch: a
**micro-batch**. Each micro-batch gets a number, the **batch id**: 0, 1, 2...

```
time ──────────────────────────────────────────────────────────────>
       │ 5 s │ 5 s │ 5 s │ 5 s │
       ▼     ▼     ▼     ▼     ▼
     batch0 batch1 batch2 batch3 ...
     file 1 file 2 file 3 file 4        (MAX_FILES=1: at most one file each)
```

The **trigger** decides when a micro-batch starts. Our job uses

```python
.trigger(processingTime=TRIGGER)      # TRIGGER = "5 seconds" from .env
```

`processingTime="5 seconds"` means: start a new micro-batch every 5 seconds.
If a batch takes longer than 5 s, the next one starts as soon as it ends and
Spark warns `Current batch is falling behind`. In run A the first batch took
11,809 ms (start-up cost), so the warning appeared once. If there is nothing
new, Spark usually skips the batch.

Other triggers exist: `availableNow=True` (process everything that is there,
then stop, handy for back-fills) and continuous mode (experimental). This lab
only uses `processingTime`.

A second knob limits how much one batch takes:

```python
.option("maxFilesPerTrigger", MAX_FILES)   # MAX_FILES = 1 from .env
```

With `MAX_FILES=1`, one batch = one file, which makes tests easy to follow.
It also caps speed at one file per 5 s, which is too slow for the generator's
one file per 2 s. That is exactly why performance run P1 fell behind (see
[08 · Performance and tuning](08-performance-and-tuning.md)).

## Sources and sinks

Every streaming query reads from a **source** and writes to a **sink**.

| | Built-in examples | In this lab |
|---|---|---|
| **Source** (where rows come from) | files in a folder (CSV, JSON, Parquet), Kafka, a socket, a test "rate" source | **files**: `spark.readStream.csv("data/incoming")` |
| **Sink** (where results go) | files, Kafka, console, memory, `foreach`, `foreachBatch` | **`foreachBatch`**: our own function writes each batch to PostgreSQL |

A source must be able to say "here is what is new since position X", so
Spark can record positions and replay after a crash. The file source does this
by keeping a list of every file it has already handed out (see
[06](06-reliability-checkpoints-exactly-once.md)).

## Why a folder of files is the source

Real systems usually stream from a message broker such as **Kafka** (a server
that stores an ordered log of messages and lets readers resume from a
position). The lab spec asks for CSV files instead. That is a reasonable
stand-in:

- No extra server to install. A folder works on any laptop.
- You can open the input in a text editor and count rows by hand. That is how
  the exact expected counts in [07](07-testing-the-pipeline.md) are known.
- Replays are easy to set up: copy a file, delete a checkpoint entry.

The file source has rules you must follow, and the code follows them:

1. **Files must appear complete.** Spark may list the folder while a file is
   half written. The generator writes to a name starting with `_` and then
   renames it in one step. Spark ignores names starting with `_` or `.`.
   ([03](03-generator-line-by-line.md#h-4-write-file-write-to-tmp-then-rename))
2. **Files are never changed after they appear.** Spark tracks files by
   path. If you append to an already-read file, the new rows are never seen.
3. **The schema must be given up front.** A stream cannot scan "all the data"
   to guess column types, because all the data does not exist yet.

## Output modes, and why this job uses foreachBatch

The **output mode** says what each micro-batch sends to the sink:

| Mode | What goes to the sink each batch | Typical use |
|---|---|---|
| **append** (the default) | only rows that are new and final | row-by-row pipelines like ours: each event is written once |
| **update** | only result rows that changed since the last batch | running counts per key |
| **complete** | the whole result table, every time | small aggregate tables (e.g. totals per category) |

Our query does no aggregation (no `groupBy`), so **append** is right: each
clean event is emitted once, in the batch where it arrived.

Why not a ready-made "PostgreSQL sink"? Spark has **no built-in streaming
JDBC sink**. `foreachBatch` solves that: it calls your Python function once per
micro-batch, passing the batch as an ordinary (non-streaming) DataFrame plus
its batch id:

```python
.foreachBatch(write_batch)      # write_batch(batch_df, batch_id)
```

Inside, you can use anything that works on a normal DataFrame, including
Spark's batch JDBC writer, and you can run extra SQL before and after. Our
`write_batch` uses that freedom for a two-step write: bulk-insert into a
staging table, then a SQL merge that skips duplicates. The why is in
[05](05-postgres-jdbc-and-the-merge.md#h-12-why-staging-merge-not-a-plain-append).

## Event time vs processing time

Two different clocks matter in streaming:

| Clock | Meaning | In this lab |
|---|---|---|
| **Event time** | when the thing actually happened in the real world | `event_time` column, written by the generator in UTC, e.g. `2026-11-04 10:35:40.222` |
| **Processing time** | when the pipeline handled it | the trigger clock; and `ingested_at` (`DEFAULT now()` when the row lands in PostgreSQL) |

They differ because of delay. In a real system an event can travel through
phones, networks and queues and arrive minutes late, or out of order.

One file shows the two clocks side by side. The generator's first file in
run A is called `events_20261104_123540_223838.csv` (file name from the
laptop's **local** clock, UTC+2) and its first row has
`event_time = 2026-11-04 10:35:40.222` (**UTC**). Same moment, two clocks.
That is why the job sets `spark.sql.session.timeZone` to `UTC` and the table
uses `TIMESTAMPTZ`
([05](05-postgres-jdbc-and-the-merge.md#h-6-timestamptz-vs-timestamp-the-2-hour-story)).

Event time is what you use for **business logic**: de-duplication with a
watermark ("how late may a duplicate still arrive?") is defined on
`event_time`, not on when Spark saw the row. The difference between the two
clocks is the **latency**: `ingested_at - event_time`.

## Latency vs throughput (first look)

- **Latency**: how long one event waits from happening to being stored.
  Measured as `ingested_at - event_time`.
- **Throughput**: how many rows per second the pipeline handles.

They pull against each other. A micro-batch has a fixed cost (planning,
opening a database connection, a transaction) of roughly 3 s here, whether it
holds 100 rows or 10,000. So:

| Run | Input | Trigger / files per batch | Landed rows/s | Latency p50 |
|---|---|---|---|---|
| P2 | 50 rows/s | 5 s / 5 | 53.3 | 5.4 s |
| P3 | 1,000 rows/s | 10 s / 10 | 902.1 | 16.6 s |

Bigger, less frequent batches: more rows per second, but each event waits
longer. A pipeline that cannot keep up at all (P1) has *growing* latency,
because the queue of waiting files only gets longer. The full analysis is in
[08 · Performance and tuning](08-performance-and-tuning.md).

## Self-check

1. In one sentence, what is a micro-batch? (The set of new input rows Spark
   collects and processes together at one trigger.)
2. Why must the schema be given to `readStream`? (The data does not exist
   yet, so Spark cannot scan it to infer types.)
3. Why does this job use `foreachBatch` instead of a built-in sink? (There is
   no streaming JDBC sink; `foreachBatch` gives a normal DataFrame so we can
   use the batch JDBC writer plus our own SQL.)
4. Which clock is `event_time`, and which is `ingested_at`? (Event time;
   processing/landing time. Their difference is the latency.)
5. With `MAX_FILES=1`, a 5 s trigger and a new file every 2 s, what happens?
   (Files arrive faster than they are taken; the backlog and latency grow.)
