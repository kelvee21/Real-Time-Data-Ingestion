# 06 — Reliability: Checkpoints, Replays and "Exactly Once"

How the pipeline survives being stopped, killed or cut off from the database
without losing or doubling a single row. This page opens the real
`checkpoint/events/` folder, follows one micro-batch through it, and ties each
guarantee to a test that proved it.

> **Help:** the code lines this page refers to are in [04 · The streaming job line by line](04-streaming-job-line-by-line.md), and the merge SQL in [05](05-postgres-jdbc-and-the-merge.md#h-12-why-staging-merge-not-a-plain-append).

## The problem: a job can die at any moment

A streaming job runs for days. In that time it will be stopped for updates,
the machine will reboot, the database will be unreachable for a while. For each
of those, two questions:

1. **Loss**: after a restart, does every row that arrived still get written?
2. **Duplicates**: does any row get written twice?

A batch job can just be re-run from the start. A streaming job cannot: it has
to remember exactly **where it was**. Spark keeps that memory in the
**checkpoint folder**, set by one line:

```python
.option("checkpointLocation", CHECKPOINT_DIR)   # which files are done + dedup state
```

## The checkpoint folder, opened up

This is the real folder after all the tests:

```text
checkpoint/events/
├── metadata            {"id":"2887773f-bc1c-4ad3-b679-05876c549e9a"}
├── offsets/            10, 11, ..., 110     one file per batch: what the batch WILL process
├── commits/            10, 11, ..., 110     one file per batch: the batch is DONE
├── sources/0/          one file per input file Spark has accepted
└── state/0/            the de-duplication memory
    ├── _metadata       {"operatorName":"dedupe", ... "numPartitions":4}
    ├── 0/ 1/ 2/ 3/     one folder per shuffle partition
    │     1.delta ... 111.delta, 13.snapshot, 28.snapshot, ...
```

| Part | What it holds | Real example |
|---|---|---|
| `metadata` | the query's permanent id | every restart printed `query started: id=2887773f-...`: same checkpoint, same query |
| `offsets/N` | written **before** batch N runs: which input it covers, the watermark, key settings | see below |
| `commits/N` | written **after** batch N's sink (`write_batch`) returned without error | `{"nextBatchWatermarkMs":1793788313042}` |
| `sources/0/K` | the file source's own log: entry K = one accepted file path | `sources/0/40` → `.../events_20261104_124153_042533.csv` |
| `state/0/p/` | for partition `p`, the `event_id`s seen recently. Each batch writes a `.delta` (changes); every so often a `.snapshot` (full copy) | `state/0/0/41.snapshot` |

Why only 10 to 110? Spark keeps the last 100 batches of history and deletes
older files (setting `spark.sql.streaming.minBatchesToRetain`, default 100).

### Inside offsets/42

```text
v1
{"batchWatermarkMs":1793788312030,"batchTimestampMs":1793789005485,"conf":{..., "spark.sql.shuffle.partitions":"4", ...}}
{"logOffset":40}
```

| Field | Value | Meaning |
|---|---|---|
| `batchWatermarkMs` | 1793788312030 = 10:31:52.030 UTC | the watermark for this batch ([below](#h-watermark-bounded-state)) |
| `batchTimestampMs` | 1793789005485 = 10:43:25.485 UTC | when the batch was planned (processing time) |
| `conf` | includes `shuffle.partitions = 4` | settings **frozen** for this checkpoint. The state is split into 4 partitions; changing the setting later has no effect on this checkpoint |
| `logOffset` | 40 | "up to and including entry 40 of `sources/0`". Batch 41 had 39, so batch 42 = exactly one file, entry 40 |

Batch ids and `logOffset` numbers drift apart because some batches read no
file. Batches 20, 27, 43 and 47 have the same `logOffset` as the batch before
them. Those are **no-data batches**: Spark runs one when the watermark has
moved, so the de-duplication step can clear out expired ids even though no new
file arrived. (Batch 21 is different: it read the duplicate file in TC-07
and produced 0 new rows.)

## The life of one micro-batch

```
 1. list data/incoming, pick up to MAX_FILES new files
 2. append them to sources/0/K
 3. write offsets/N                      <- "I am about to do batch N = these files"
 4. read files → clean() → dropDuplicates (reads state version N, writes N+1 delta)
 5. call write_batch(batch_df, N)        <- staging + merge into PostgreSQL
 6. write commits/N                      <- "batch N is finished"
```

Steps 3 and 6 are the key. `offsets/` is a **write-ahead log**: the plan is
saved before the work starts. `commits/` records completion. On every start,
Spark compares the two:

| On restart Spark finds... | It does... |
|---|---|
| `offsets/N` and `commits/N` | batch N is finished; plan batch N+1 from new files |
| `offsets/N` but **no** `commits/N` | batch N was interrupted; **run batch N again, with exactly the same files** (read from `offsets/N` and `sources/0`) and the state as it was before N |

That second row is the whole recovery mechanism.

## At-least-once + idempotent sink = effectively exactly-once

Three delivery guarantees are usually named:

| Guarantee | Meaning |
|---|---|
| **at-most-once** | each row is written 0 or 1 times: may lose data, never duplicates |
| **at-least-once** | each row is written 1 or more times: never loses data, may duplicate |
| **exactly-once** | each row is written exactly 1 time |

For `foreachBatch`, Spark gives **at-least-once**: if the job dies after
`write_batch` finished but before `commits/N` was written, batch N runs
again. Spark cannot know that our database already has the rows.

We turn that into **effectively exactly-once** by making the sink
**idempotent**: writing the same batch twice leaves the same result as writing
it once. The recipe has three parts, and the pipeline has all three:

| Ingredient | In this pipeline |
|---|---|
| a **replayable source**: the same batch can be re-read exactly | the file source + `offsets/` + `sources/0` say which files batch N was |
| **deterministic processing**: the same input gives the same output | `clean()` is pure rules; the de-dup state is versioned per batch |
| an **idempotent sink** keyed on a **stable id** | `INSERT ... ON CONFLICT (event_id) DO NOTHING`, with `event_id` created by the generator |

"Effectively" because the batch really is processed twice; only the result in
`events` is as if it ran once. Side outputs that are not idempotent show the
second run: `metrics/batches.jsonl` has **two** lines for batch 42.

## Crash before or after the commit

Where exactly the job dies decides what happens next. All cases end with the
right rows in `events`:

| The job dies... | State left behind | On restart | Tested by |
|---|---|---|---|
| before `offsets/N` is written | nothing about N | N is planned from scratch; the files are still "new" | (normal restart) |
| after `offsets/N`, before the merge | maybe some rows in `events_staging` | N re-runs; `TRUNCATE events_staging` clears leftovers; rows inserted once | hard kill (runC/runD) |
| inside the merge transaction | transaction rolled back; `events` unchanged | N re-runs, inserts everything | covered by the transaction ([05 §13](05-postgres-jdbc-and-the-merge.md#h-13-transactions-in-write-batch)) |
| after the merge commit, before `commits/N` | rows already in `events` | N re-runs; `ON CONFLICT` skips all of them | TC-05 replay |
| `write_batch` raises an error (DB down) | `offsets/N`, no `commits/N` | N re-runs when the DB is back | TC-08 |
| after `commits/N` | N is done | next batch | TC-06 restart |

## Watermark: bounded state

`dropDuplicates` must remember ids it has seen. Kept for ever, that memory
grows with every event, and the checkpoint grows with it. The watermark bounds
it:

```python
.withWatermark("event_time", "10 minutes")
.dropDuplicates(["event_id", "event_time"])
```

> **watermark** = (largest `event_time` seen so far) − 10 minutes

Spark treats anything older than the watermark as "too late to matter" and
deletes stored ids with an `event_time` below it. Real numbers from the
checkpoint:

- Batch 41's file had events up to about `10:41:52.030` UTC.
- So batch 42's watermark is `10:31:52.030` (`batchWatermarkMs` in
  `offsets/42`), exactly 10 minutes earlier.
- After batch 42, `commits/42` says the next watermark is `10:31:53.042`:
  batch 42's newest event, minus 10 minutes.

Ids older than that are forgotten, so the state holds roughly the last 10
minutes of events, not the whole history.

What this means for duplicates:

| Duplicate arrives... | Caught by |
|---|---|
| within ~10 minutes (event time) of the original | Spark's `dropDuplicates`: the row never reaches PostgreSQL (TC-07: 98 rows read, 0 passed on) |
| later than that | the `events` primary key: `ON CONFLICT (event_id) DO NOTHING` skips it |

Two layers, each covering the other's gap. The 10 minutes is a trade-off:
longer = more duplicates caught in Spark, but more state to keep.

The watermark also affects **late rows**. A row whose `event_time` is already
below the watermark when it arrives is dropped by a stateful operator like
`dropDuplicates`. In this lab the generator stamps events "now", so no row is
ever 10 minutes late. In a real system with phones that go offline, you would
size the watermark from how late real data arrives.

## The evidence

All numbers come from `reports/test_cases.md`, `metrics/batches.jsonl` and `reports/logs/`.

### TC-06: a clean restart does not re-read old files

The job stopped after batch 20 with 20 files in the folder. On restart with
the same checkpoint, the first new batch was 21, and batches 22–26 each held
one of the 5 new files. None of the 20 old files was read again: they are
listed in `sources/0`.

### TC-07: a duplicated file adds nothing

An already-processed file was copied to `events_zz_duplicate_copy.csv`. It is a
new **path**, so the file source accepted it (batch 21 read 98 rows). Every
`event_id` was still in the de-dup state, so 0 rows passed on and
`write_batch` returned early. Total: exactly the expected 2,014.

### Hard kill: the job is killed mid-batch

The job was killed with `taskkill /F` (no clean shutdown) while the generator
wrote 15 files. Evidence of the kill in the checkpoint: a 0-byte temporary file
`state/0/0/.29.delta.ab800ee7-....TID1.tmp`, the state write of batch 28 that
never finished. Its log (`runC_job.log`) has no `batch` line at all.

On restart (`runD_job.log`) Spark found `offsets/28` without `commits/28` and
began with batch 28: `78 rows, 78 inserted`. Batches 28 to 42 then took the
15 files. Their rows add up to 1,176, which is exactly 1,500 generated − 324
spoiled. Total `count = count(DISTINCT event_id) = 3,190`. Nothing lost,
nothing doubled.

### TC-05: forced replay of a finished batch

This is the "died after the merge, before the commit" case, the one the
idempotent sink exists for. It cannot be timed by hand, so it was created
(in a test environment only):

1. Stop the job. `events` has 3,190 rows, batch 42 (83 rows) written and
   committed.
2. Delete `checkpoint/events/commits/42`, plus empty batch 43's offset and
   commit files, so 42 is the last batch and looks unfinished.
3. Start the job.

```json
{"batch_id": 42, "rows": 83, "inserted": 0, "skipped_duplicates": 83, ...}
```

Spark re-ran batch 42 with the same 83 rows; the merge skipped all 83. Total
still 3,190, distinct ids 3,190.

### TC-08: database unreachable

With `PGPORT=5499` (nothing listening), the first `psycopg2.connect` in
`write_batch` raised `Connection refused`. The error left `write_batch`, so
Spark did **not** write the commit. The query stopped (exit code 1) with
`offsets/44` and no `commits/44`. Restarted with the right port, batch 44
re-ran (`80 rows, 80 inserted`), then 45 and 46. 300 rows − 62 spoiled = 238
= 80 + 82 + 76. Total 3,428, no duplicates.

Note the job **stops** when the database is down; it does not wait and retry.
Something outside (a person, a service manager) must restart it. No data is
lost meanwhile: the files wait in the folder.

## Rules for not breaking the checkpoint

- **One checkpoint per query, one query per checkpoint.** Two jobs on one
  checkpoint folder corrupt each other's logs.
- **Do not edit or delete checkpoint files** in a real system. TC-05 did it on
  purpose, in a test setup, to create a replay.
- **Delete the checkpoint only as part of a full reset** (with the input files
  and the table, see [01](01-setup-postgres-and-spark.md#h-reset-to-a-clean-slate)).
  A fresh checkpoint treats every file in the folder as new.
- **Changing the query can make an old checkpoint unusable.** Adding a
  filter rule is fine. Changing the de-dup key, the watermark column or the
  number of shuffle partitions changes the stored state's layout; then you need
  a new checkpoint.
- Keep input files until their batch is committed. Deleting a file that is
  in an uncommitted `offsets/N` breaks the replay.

## Self-check

1. What is the difference between `offsets/N` and `commits/N`? (`offsets` is
   written before the batch runs, recording its input; `commits` after the
   sink succeeded.)
2. Spark finds `offsets/44` but no `commits/44`. What does it do? (Re-runs
   batch 44 with the same files and the pre-44 state.)
3. Why is the pipeline "effectively" exactly-once and not just exactly-once?
   (Spark may run a batch twice; the idempotent merge makes the second run
   change nothing in `events`.)
4. A duplicate file arrives 30 minutes after the original. What stops it?
   (The watermark has let the ids go from Spark's state; the primary key with
   `ON CONFLICT DO NOTHING` skips them.)
5. Why are batches 20, 27, 43 and 47 missing from `batches.jsonl`? (No-data
   batches: no new file, run so the watermark can clear old state; `rows == 0`,
   so nothing is logged.)
