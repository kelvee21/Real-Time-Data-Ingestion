# 09 — The Hard Parts, Explained Simply

These are the ideas in this project that are easy to *say* and hard to
*explain* when a reviewer asks "why?". Each section has four parts:

- **Plain idea.** The concept without jargon.
- **In this project.** Where it shows up in your files, with real numbers.
- **Line to say in review.** A short answer to practise out loud.
- **Follow-up trap.** The next question a reviewer is likely to ask.

> **Help:** the code these concepts live in is walked through in [04 · The streaming job line by line](04-streaming-job-line-by-line.md) and [05 · PostgreSQL, JDBC and the merge](05-postgres-jdbc-and-the-merge.md). The tests that prove them are in [07 · Testing the pipeline](07-testing-the-pipeline.md).

---

## 1. The micro-batch model

**Plain idea.** Spark Structured Streaming does not handle events one at
a time. It treats the stream as a table that keeps growing. Every few
seconds it takes the rows that are new since last time and runs a normal
batch query on just those rows. That small batch is a **micro-batch**.
The same query runs again and again on each new slice.

**In this project.** The "new rows" are new CSV files in
`data/incoming/`. Each micro-batch reads the new files, runs `clean()`
(casts, filter, dedup), and hands the result to `write_batch(batch_df,
batch_id)`. Batch numbers count up: the functional runs went from batch 0
to batch 47 on one checkpoint, and P1 continued from 48 to 83.

**Line to say in review.** "Spark runs my query as a series of small
batch jobs, one per trigger, each on the files that arrived since the last
one. So I get batch tools, like JDBC and SQL, with a delay of a few
seconds."

**Follow-up trap.** "So it's not real time?" → It is near real time:
p50 latency 5.4 s in run P2. Spark also has a "continuous" mode with
millisecond latency, but it is experimental and does not support
`dropDuplicates` or `foreachBatch`. For an e-commerce dashboard, seconds
are enough.

---

## 2. The trigger interval

**Plain idea.** The trigger is a clock. It says how often a new
micro-batch may start. With a 5 s trigger, Spark checks for new data at
0 s, 5 s, 10 s... If a batch takes longer than 5 s, the next one starts
as soon as it finishes.

**In this project.** `.trigger(processingTime=TRIGGER)` with
`TRIGGER = os.getenv("TRIGGER", "5 seconds")`. P3 used `"10 seconds"`.
Steady-state batches took ~2.7 to 3.7 s, so they fit inside the slot.
The first batch of each run took 10 to 12.5 s and caused the only
`Current batch is falling behind` warning.

**Line to say in review.** "The trigger sets the rhythm. Half a trigger
is the average wait before a file is picked up, so it sets the floor on
latency. P2's p50 of 5.4 s is half of 5 s plus a ~3 s batch."

**Follow-up trap.** "Why not a 1-second trigger?" → Each batch has about
3 s of fixed cost (planning, connections, checkpoint). With a 1 s
trigger, batches would run back to back and latency would not improve.
See [08 · Fixed cost vs per-row cost](08-performance-and-tuning.md#h-4-fixed-cost-vs-per-row-cost).

---

## 3. `maxFilesPerTrigger` and backlog

**Plain idea.** `maxFilesPerTrigger` caps how many files one batch may
take. Files above the cap wait for a later batch. The waiting files are
the **backlog**. If files arrive faster than `cap ÷ trigger`, the backlog
grows without end.

**In this project.** `.option("maxFilesPerTrigger", MAX_FILES)`, default
`MAX_FILES=1`. Run P1: one file every 2 s arrives, one file per 5 s is
processed, so 2.5 files arrive for every one processed. After 2 minutes,
24 of 60 files were still waiting, and latency reached 116.4 s. Run P2
changed only `MAX_FILES` to 5: p50 latency 5.4 s.

**Line to say in review.** "Capacity is files per batch divided by the
trigger. At MAX_FILES=1 that was 0.2 files a second against 0.5 arriving,
so P1 fell behind even though each batch used only 3 of its 5 seconds.
The cap was the bottleneck, not the CPU."

**Follow-up trap.** "Then why not remove the cap?" → Without a cap, the
first batch after a long outage would take every waiting file at once.
That one batch could be huge and slow, or run out of memory. A cap keeps
batches a predictable size while still allowing catch-up, as long as it
is set above the arrival rate.

---

## 4. The checkpoint

**Plain idea.** A checkpoint is the job's notebook on disk. Before a
batch runs, Spark writes down *what* it is about to process. After the
batch succeeds, it writes down *that it finished*. On restart Spark reads
the notebook and carries on from where it was.

**In this project.** `.option("checkpointLocation", CHECKPOINT_DIR)`,
default `checkpoint/events`. Inside:

| Folder | Holds |
|---|---|
| `metadata` | The query id. That is why every functional run printed the same `id=2887773f-...` |
| `offsets/N` | Written **before** batch N: which source position and watermark it covers |
| `sources/0/N` | The exact file names batch N reads |
| `commits/N` | Written **after** batch N's sink succeeded |
| `state/` | The `dropDuplicates` memory: event ids seen recently, in 4 partitions |

TC-06: after a restart, batch numbers continued at 21, and none of the 20
old files was read again.

**Line to say in review.** "The checkpoint records which files each
batch read, and whether the batch finished. Offsets are written before
the batch, the commit after. A batch with an offset but no commit is
re-run on restart."

**Follow-up trap.** "What if I delete the checkpoint?" → Spark forgets
everything and reads every file in the folder again. No duplicates reach
`events`, because the primary key and `ON CONFLICT DO NOTHING` skip them.
Also: never share one checkpoint between two queries.

---

## 5. At-least-once delivery and an idempotent sink

**Plain idea.** **At-least-once** means every batch is delivered, but
some might be delivered twice. This happens when the job writes a batch
and then dies before writing the commit. **Idempotent** means doing
something twice has the same effect as doing it once. At-least-once plus
an idempotent write gives the *effect* of exactly-once.

**In this project.** `foreachBatch` gives at-least-once. The write is
made idempotent by the merge:

```sql
INSERT INTO events (...) SELECT ... FROM events_staging
ON CONFLICT (event_id) DO NOTHING
```

TC-05 forced a replay by deleting `checkpoint/events/commits/42`. Spark
re-ran batch 42: `{"rows": 83, "inserted": 0, "skipped_duplicates": 83}`.
The total stayed 3,190. A hard kill (`taskkill /F`) mid-run lost nothing
and duplicated nothing either.

**Line to say in review.** "Spark can replay a batch, so the database
write must be safe to repeat. Every event has a unique `event_id`, it's
the primary key, and the merge skips ids that are already there. I proved
it by deleting a commit file: 83 rows replayed, 0 inserted."

**Follow-up trap.** "Is that true exactly-once?" → It is exactly-once in
*effect* for the `events` table. Side effects outside it are not
protected: `batches.jsonl` got a second line for batch 42. Exactly-once
needs both a replayable source (files + checkpoint) and an idempotent or
transactional sink.

---

## 6. The watermark

**Plain idea.** A watermark is Spark's answer to "how late can data
be?". Spark tracks the newest event time it has seen. The watermark is
that time minus a delay. Anything older than the watermark is treated as
"too late", and state about it can be thrown away.

**In this project.** `.withWatermark("event_time", "10 minutes")`. In
`checkpoint/perf_P3/offsets/5` the watermark is `1793789819007` ms, which
is 10:56:59.007 UTC: exactly 10 minutes before 11:06:59.007, the newest
`event_time` in the batch before it.

**Line to say in review.** "The watermark is newest event time minus ten
minutes. It lets Spark forget event ids older than that, so the
deduplication memory stays small instead of growing forever."

**Follow-up trap.** "What happens to an event that arrives 15 minutes
late?" → `dropDuplicates` with a watermark **drops** rows older than the
watermark. That loss is silent. Here the generator always writes current
times, so it never happened, but with real late data you would widen the
delay or deduplicate only in PostgreSQL.

---

## 7. `dropDuplicates` state, and why it needs a watermark

**Plain idea.** To know whether an id is a repeat, Spark must remember
every id it has seen. That memory is called **state**, and it is saved in
the checkpoint. Without a limit, state grows with every event forever.
The watermark gives the limit.

**In this project.**

```python
return (valid
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id", "event_time"]))
```

`event_time` is in the key list on purpose. Spark can only evict an
entry when it knows the entry's event time, so the watermark column must
be one of the keys. TC-07 copied an already-processed file: batch 21
read 98 rows and passed **0** on, because every id was still in state.

**Line to say in review.** "dropDuplicates keeps the ids it has seen in
state. Including event_time in the keys, with a ten-minute watermark,
lets Spark drop ids older than ten minutes, so state stays bounded. The
primary key is the second line of defence for anything older."

**Follow-up trap.** "Same `event_id`, different `event_time`?" → In
Spark those are two different keys, so both pass. The primary key then
keeps the first one and skips the second. (Spark 3.5+ also has
`dropDuplicatesWithinWatermark(["event_id"])` for exactly this case.)

---

## 8. Event time vs processing time

**Plain idea.** **Event time** is when the thing happened (the user
clicked). **Processing time** is when the system handles it. They differ
by the delay in the pipeline, and on a bad day by minutes or hours.

**In this project.** Three clocks:

| Time | Column / setting | Set by |
|---|---|---|
| Event time | `event_time` | The generator: `datetime.now(timezone.utc)` when the event is made |
| Processing time | `.trigger(processingTime=...)` | Spark's clock, when a batch starts |
| Ingestion time | `ingested_at DEFAULT now()` | PostgreSQL, when the merge transaction starts |

Latency is `ingested_at − event_time`. The watermark uses event time.
Reports should group by `event_time`.

**Line to say in review.** "Reports and the watermark use event time,
when the user acted. The trigger uses processing time. The gap between
event time and ingested_at is my end-to-end latency."

**Follow-up trap.** "Why not just use `now()` for event time?" → After a
two-minute outage, a whole batch would be stamped with the recovery time.
A "sales per minute" chart would show a false spike. Event time stays
correct whatever the delay.

---

## 9. `foreachBatch` vs built-in sinks

**Plain idea.** A **sink** is where a streaming query writes. Spark has
built-in streaming sinks (files, Kafka, console, memory), but **no
streaming JDBC sink**. `foreachBatch` gives you each micro-batch as an
ordinary DataFrame plus its batch id, and you write it however you like.

**In this project.** `.foreachBatch(write_batch)`. Inside, `batch_df` is
a normal DataFrame, so it can use the batch JDBC writer
(`batch_df.write.format("jdbc")`) and then run SQL with psycopg2.

**Line to say in review.** "There's no streaming JDBC sink, so I use
foreachBatch. It lets me write each batch with the normal JDBC writer and
then run my own merge SQL. The price is at-least-once delivery, which the
primary key handles."

**Follow-up trap.** "Where does `write_batch` run?" → On the **driver**
(the main Python process), once per batch. The JDBC write inside it is
still split across executor tasks, one per partition. psycopg2 runs on
the driver only.

---

## 10. Staging table + merge

**Plain idea.** Write the batch into an empty side table first
(**staging**), then move it into the real table with one SQL statement.
The bulk load stays simple and fast, and all the rules apply in one
transaction at the end.

**In this project.** `write_batch()`:

1. `TRUNCATE events_staging` (clear leftovers of a failed earlier try)
2. JDBC `mode("append")` into `events_staging`. It has no constraints
   (`LIKE events INCLUDING DEFAULTS EXCLUDING CONSTRAINTS`), so it never
   fails half way
3. In one transaction: `INSERT ... SELECT ... ON CONFLICT (event_id) DO
   NOTHING`, then `TRUNCATE events_staging`

Median times: JDBC append 1.2 s, merge 57 ms.

**Line to say in review.** "Spark's JDBC writer can only do plain
inserts. A replayed batch would hit the primary key and fail. So I land
the batch in staging, then merge with ON CONFLICT DO NOTHING in one
transaction. Replays become no-ops instead of errors."

**Follow-up trap.** "Is that an upsert?" → Strictly no: an upsert
updates existing rows (`DO UPDATE`). Events never change once they
happen, so "insert if absent" is the right rule. And: "What if two jobs
run at once?" → They would share `events_staging` and truncate each
other's rows. The design assumes a single writer.

---

## 11. `TIMESTAMPTZ` vs `TIMESTAMP` (the +2 hours story)

**Plain idea.** `TIMESTAMP` (without time zone) stores a wall-clock
reading like `09:42`, with no idea which zone it was in. `TIMESTAMPTZ`
stores an absolute instant. It converts input to UTC and shows it in the
session's zone.

**In this project.** Earlier in the week the columns were `TIMESTAMP`.
The generator writes UTC times, and Spark's session zone is UTC
(`spark.sql.session.timeZone`). But the JDBC driver converts timestamps
using the **JVM's own zone**, which on this laptop is UTC+2. So
`09:42:17` UTC was stored as `11:42:17`, two hours off. Changing
`event_time` and `ingested_at` to `TIMESTAMPTZ` fixed it: TC-03 shows
the values match the CSV exactly with `SET timezone='UTC'`. You can see
the two zones in the files: generator file names use local time
(`events_20261104_130700_...`), `event_time` values use UTC (11:07:00).

**Line to say in review.** "I store event times as TIMESTAMPTZ, an
absolute instant. With plain TIMESTAMP, the JDBC driver wrote in the
JVM's local zone and every time was two hours late. TIMESTAMPTZ removes
the guessing."

**Follow-up trap.** "psql shows `+02` on my times. Is it still wrong?"
→ No. The server's zone is Africa/Johannesburg, so psql *displays* in
UTC+2. The stored instant is right. `SET timezone = 'UTC'` displays the
same instant in UTC.

---

## 12. Schema-on-read with strings, then `try_cast` (ANSI mode)

**Plain idea.** **Schema-on-read** means the file has no types; the
reader decides them. A streaming source needs its schema given up front.
If you declare `user_id` as an integer and a file contains `unknown`,
the read itself can fail. Reading everything as text, then converting
column by column, moves the problem into your own code where you can
handle it.

**ANSI mode** is a stricter SQL rule set, on by default in Spark 4. In
ANSI mode, `CAST('unknown' AS INT)` throws an error instead of returning
null. One bad value would stop the whole stream. `try_cast` returns null
instead.

**In this project.** `RAW_SCHEMA` makes all nine columns `StringType`.
Then `clean()`:

```python
.withColumn("user_id", F.expr("try_cast(user_id AS INT)"))
.withColumn("event_time", F.expr("try_to_timestamp(event_time, 'yyyy-MM-dd HH:mm:ss.SSS')"))
```

The generator's bad values are `""`, `"-5"`, `"unknown"`, `"NaN"`. The
first, third and fourth become null and fail the filter. `"-5"` is a
valid integer, which is exactly the TC-04 bug: 27 rows with
`user_id = -5` got in until the filter changed from "is not null" to
`> 0`.

**Line to say in review.** "Every column is read as a string, then
converted with try_cast. A bad value becomes null and the row is
filtered out, instead of a cast error stopping the whole stream."

**Follow-up trap.** "Why does `> 0` reject nulls?" → In SQL, `NULL > 0`
is unknown, not true, and a filter keeps only rows where the condition
is true. That is the comment on line 90 of the job.

---

## 13. Atomic file rename

**Plain idea.** Writing a file takes time. If a reader looks halfway, it
sees half a file. A **rename** on the same disk is **atomic**: it
happens all at once, so a reader sees either no file or the complete
file.

**In this project.** `write_file()` writes `_events_....csv`, then:

```python
os.replace(tmp_path, os.path.join(out_dir, name))     # atomic rename
```

Spark's file source ignores names starting with `_` or `.`, so it never
lists the temporary file. TC-01: 20 files, 0 leftover `_` files. TC-07
used the same trick when copying a duplicate file.

**Line to say in review.** "The generator writes to an underscore name
and renames when done. Spark skips underscore files and the rename is
atomic, so Spark never reads half a file."

**Follow-up trap.** "What would go wrong without it?" → Spark could
list the file while 40 of 100 rows are written, read those 40, and mark
the file as done in the checkpoint. The other 60 rows would never be
read.

---

## 14. The least-privilege database role

**Plain idea.** **Least privilege** means each program gets only the
rights it needs. If the program is buggy or its password leaks, the
damage is limited.

**In this project.** `postgres_setup.sql` creates `spark_app`, a normal
login (not a superuser), and grants:

| Table | Rights |
|---|---|
| `events` | `SELECT`, `INSERT` |
| `events_staging` | `SELECT`, `INSERT`, `TRUNCATE` |

Checked live in `information_schema.role_table_grants`: exactly those
five. No `UPDATE`, `DELETE` or `TRUNCATE` on `events`. The tables are
owned by `postgres`, so `spark_app` cannot drop or alter them. The
password lives only in the gitignored `.env`.

**Line to say in review.** "The job logs in as spark_app, which can add
and read events and empty its own staging table, nothing else. It cannot
delete or change events, so a bug cannot wipe history."

**Follow-up trap.** "Why does it need `SELECT` on `events`?" →
`perf_report.py` reads `events` with the same `.env` login to measure
latency. The merge itself reads `events_staging`, which needs `SELECT`
there.

---

## 15. Filter pushdown and why `numInputRows` is low

**Plain idea.** **Pushdown** means the optimizer moves a filter as close
to the data source as it can, so fewer rows travel through the rest of
the query. Spark can also *infer* extra filters: if you keep only
`user_id > 0`, then `user_id` cannot be null, so it adds an "is not
null" check and pushes it into the CSV reader.

**In this project.** Run P2: the generator wrote 6,000 rows, but Spark's
`numInputRows` was **5,984**. The 16 missing rows had an empty value in
a checked column and were dropped inside the reader, before Spark counted
input rows. Of the 124 spoiled rows, the other 108 were removed by the
filter in `clean()`. 6,000 − 124 = 5,876 rows landed, exactly.

**Line to say in review.** "numInputRows is a bit below rows written
because Spark pushes the not-null checks into the CSV scan. Those rows
are dropped before they're counted. That's why I check correctness from
the generator's spoiled count against PostgreSQL, not from Spark's input
count."

**Follow-up trap.** "How would you see the pushed filters?" → Call
`batch_df.explain()` inside `write_batch`. The scan line shows
`PushedFilters: [IsNotNull(...), ...]`.

---

## Self-check

1. What is written to the checkpoint before a batch runs, and what after?
   [Before: `offsets/N` (and `sources/0/N`). After the sink succeeds:
   `commits/N`.]
2. Why must `event_time` be in the `dropDuplicates` key list? [Spark can
   only evict a state entry once it knows its event time is older than
   the watermark.]
3. What makes a replayed batch harmless? [`event_id` is the primary key
   and the merge uses `ON CONFLICT (event_id) DO NOTHING`.]
4. Why did plain `TIMESTAMP` store times 2 hours off? [The JDBC driver
   converted using the JVM's local zone, UTC+2, not the session's UTC.]
5. Why did `"-5"` pass the first filter? [It is a valid integer, so
   `try_cast` gives `-5`, not null. "Is not null" was true.]
