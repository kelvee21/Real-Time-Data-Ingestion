# 08 — Performance and Tuning

This page explains what the numbers in `reports/performance_metrics.md` mean, where
each one comes from, why run P1 fell behind and P2 did not, and which
settings change the result. Every number here comes from the real runs on
4 November 2026.

> **Help:** if "micro-batch" or "trigger" is still new, read [02 · Batch vs streaming](02-batch-vs-streaming.md) first. The code being measured is explained in [04 · The streaming job line by line](04-streaming-job-line-by-line.md).

## 1. Three words: latency, throughput, batch duration

| Word | Plain meaning | Unit | In this project |
|---|---|---|---|
| **Latency** | How long one event waits between happening and being queryable | seconds | `ingested_at − event_time` for each row in `events` |
| **Throughput** | How many rows the system moves per second | rows/s | rows landed ÷ seconds of the run |
| **Batch duration** | How long Spark takes to finish one micro-batch | ms | `triggerExecution` in Spark's progress report |

An analogy: a bus stop. **Batch duration** is how long the bus takes to
load and drive. The **trigger** is how often a bus leaves. **Throughput**
is passengers moved per hour. **Latency** is how long one passenger
waited at the stop plus the ride.

Two more words you need:

- **Input rate.** How fast data arrives. Here it is set by the generator:
  `--rows` ÷ `--every`. `--rows 100 --every 2` is 50 rows/s.
- **Backlog.** Files that have arrived but are not processed yet. If the
  backlog grows, latency grows with it. This is called **falling behind**.

The rule that matters most: a streaming job **keeps up** only if it can
process data at least as fast as data arrives. If not, the backlog grows
forever, no matter how fast each single batch is.

## 2. Where each number comes from

There are three sources. Each one sees a different part of the pipeline.

| Source | Written by | Sees |
|---|---|---|
| `metrics/progress.jsonl` | `ProgressLogger` (a Spark listener) | Spark's view: rows read, batch duration |
| `metrics/batches.jsonl` | `write_batch()` | The database writes inside each batch |
| `events.ingested_at` | PostgreSQL `DEFAULT now()` | The end-to-end view: event to row |

### Spark's progress report (the listener)

A **listener** is a class whose methods Spark calls when something
happens. `ProgressLogger` extends `StreamingQueryListener`. After every
micro-batch Spark calls `onQueryProgress(event)`, and `event.progress.json`
holds a JSON report. The job keeps six fields of it:

```python
def onQueryProgress(self, event: QueryProgressEvent) -> None:
    p = json.loads(event.progress.json)
    log_jsonl("progress.jsonl", {
        "batch_id": p["batchId"], "timestamp": p["timestamp"],
        "num_input_rows": p["numInputRows"],
        "input_rows_per_s": p.get("inputRowsPerSecond"),
        "processed_rows_per_s": p.get("processedRowsPerSecond"),
        "trigger_ms": p["durationMs"].get("triggerExecution"),
        "add_batch_ms": p["durationMs"].get("addBatch")})
```

| Spark field | Logged as | What it measures |
|---|---|---|
| `batchId` | `batch_id` | The micro-batch number (0, 1, 2 ...). Continues across restarts with the same checkpoint |
| `timestamp` | `timestamp` | When the trigger fired, in **UTC** (`2026-11-04T10:49:55.005Z`) |
| `numInputRows` | `num_input_rows` | Rows the source handed to the query in this batch |
| `inputRowsPerSecond` | `input_rows_per_s` | `numInputRows` ÷ seconds since the *previous* batch started. How fast data came in |
| `processedRowsPerSecond` | `processed_rows_per_s` | `numInputRows` ÷ this batch's duration. How fast Spark worked |
| `durationMs.triggerExecution` | `trigger_ms` | The whole batch: list files, plan, run, write, commit the checkpoint |
| `durationMs.addBatch` | `add_batch_ms` | Only the sink part: the time spent inside `write_batch()` |

A real P1 line:

```json
{"batch_id": 60, "timestamp": "2026-11-04T10:51:00.006Z", "num_input_rows": 100,
 "input_rows_per_s": 20.0, "processed_rows_per_s": 36.6, "trigger_ms": 2734, "add_batch_ms": 1359}
```

Read it as: 100 rows arrived in this batch. They came in at 20 rows/s
(one 100-row file per 5 s trigger). Spark could have handled 36.6 rows/s
at this batch size. The whole batch took 2.7 s; 1.4 s of that was the
database write. The other ~1.4 s is Spark's own work: listing the folder,
planning, the deduplication state, and writing the checkpoint.

**Gotcha: `processedRowsPerSecond` is not capacity.** It divides by the
batch duration, which is mostly fixed cost (section 4). With 100 rows a
batch it says 35 rows/s; with 10,000 rows it says 2,495 rows/s. Same
machine, same code.

**Gotcha: `numInputRows` is a little below rows written.** In P2 the
generator wrote 6,000 rows and Spark counted 5,984. Spark pushes the
"is not null" part of the filter down into the CSV reader, so rows with an
empty `event_id`, `event_time` or `user_id` are dropped before they are
counted. See [09 · Filter pushdown](09-hard-concepts-explained.md#h-15-filter-pushdown-and-why-numinputrows-is-low).

### The per-batch log (`batches.jsonl`)

`write_batch()` times its own two steps and appends one line per non-empty
batch:

```json
{"batch_id": 42, "rows": 83, "inserted": 0, "skipped_duplicates": 83,
 "jdbc_ms": 5875, "merge_ms": 125, "at": "2026-11-04T12:45:50"}
```

That is the forced replay of batch 42 (TC-05). The same batch appears
earlier in the file with `"inserted": 83` from its first run.

| Field | Meaning |
|---|---|
| `rows` | Valid rows in the batch, after cleaning and `dropDuplicates` (`batch_df.count()`) |
| `inserted` | `cur.rowcount` of the merge: rows that were new to `events` |
| `skipped_duplicates` | `rows − inserted`: rows whose `event_id` was already in `events` |
| `jdbc_ms` | From the start of `write_batch` to the end of the JDBC append. Includes the `count()` and the first `TRUNCATE` |
| `merge_ms` | The `INSERT ... ON CONFLICT` plus the second `TRUNCATE` |
| `at` | **Local** time (UTC+2 here), not UTC. `progress.jsonl` uses UTC. Do not mix them up |

Medians over all 145 lines: `jdbc_ms` 1,192 ms, `merge_ms` 57 ms. The JDBC
append is the slow step, not the SQL merge.

Empty batches are not in this file: `write_batch` returns early when
`rows == 0`. That is why the duplicated file in TC-07 left no line.

The console line `batch 60: 100 rows, 100 inserted, 1.3 s` is the same
timing (`done − started`). Some of these lines are missing in the saved
`reports/logs/*_job.log` files because Spark's `[Stage 1:> (0 + 4) / 4]` progress
bar overwrote them. `batches.jsonl` has every batch.

### The latency query in `perf_report.py`, line by line

```sql
SELECT count(*),
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at - event_time)),
       percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at - event_time)),
       max(extract(epoch FROM ingested_at - event_time)),
       count(*) / greatest(extract(epoch FROM max(ingested_at) - min(ingested_at)), 1)
FROM events WHERE ingested_at BETWEEN %s AND %s
```

| Piece | What it does |
|---|---|
| `ingested_at - event_time` | Subtracting two `TIMESTAMPTZ` values gives an `interval`, e.g. `00:00:05.4` |
| `extract(epoch FROM <interval>)` | Turns the interval into a number of seconds (`5.4`), so it can be sorted and averaged |
| `percentile_cont(0.5) WITHIN GROUP (ORDER BY x)` | Sorts all `x` values and returns the value halfway up: the **median**, or **p50**. "cont" (continuous) means it interpolates between the two middle values if needed |
| `percentile_cont(0.95) ...` | The value 95% of the way up: **p95**. Only 5% of rows waited longer |
| `max(...)` | The single worst row |
| `max(ingested_at) - min(ingested_at)` | How long rows kept landing, first to last |
| `greatest(..., 1)` | At least 1 second, so a run with one batch does not divide by zero |
| `count(*) / ...` | **Landed rows/s**: the real end-to-end throughput |
| `WHERE ingested_at BETWEEN %s AND %s` | Only rows that landed during this run. `%s` are psycopg2 parameters filled from `--since`/`--until` |

Why p50 and p95 and not the average? An average hides the tail. In P1 the
average would look "medium" while the last files waited almost 2 minutes.
p95 tells you what the unlucky users see.

Why `ingested_at` and not Spark's numbers? Spark only knows when a batch
started. It does not know how long a file sat in the folder before the
batch picked it up. `ingested_at − event_time` covers the whole path:
generator, folder, waiting, Spark, JDBC, merge.

## 3. The P1 / P2 / P3 story

| Run | Input | Trigger | `MAX_FILES` | Batches | Mean batch | Spark rows/s | Landed rows/s | Latency p50 / p95 / max |
|---|---|---|---|---|---|---|---|---|
| **P1** | 50 rows/s (100 every 2 s) | 5 s | 1 | 36 | 3,037 ms | 35 | **20.5** | 62.7 / 113.2 / 116.4 s |
| **P2** | 50 rows/s (100 every 2 s) | 5 s | 5 | 24 | 3,306 ms | 84 | 53.3 | **5.4 / 13.4** / 15.4 s |
| **P3** | 1,000 rows/s (1,000 every 1 s) | 10 s | 10 | 15 | 3,746 ms | 2,495 | 902.1 | 16.6 / 21.9 / 22.6 s |

### P1: the queue arithmetic

Think of files as people in a queue.

- **Arrivals:** the generator writes 1 file every 2 s, so **0.5 files/s**.
- **Service:** the job takes at most `MAX_FILES = 1` file per trigger, and
  a trigger fires every 5 s, so at most **0.2 files/s**.
- **Ratio:** 0.5 ÷ 0.2 = **2.5 files arrive for every file processed.**
- **Growth:** the queue grows by 0.5 − 0.2 = 0.3 files every second.

Check against the logs: the generator wrote 60 files
(`reports/logs/perf_P1_gen.log`: `done: 60 files, 6000 rows`). The job ran 36
batches of one file each (batches 48 to 83). **24 files were still
waiting** when it stopped. Landed rows/s was 20.5, almost exactly the
ceiling of 100 rows ÷ 5 s = 20 rows/s.

The important part: **the machine was not the limit.** Each batch took
about 3 s of its 5 s slot. The job was idle 40% of the time while files
piled up. The cap on files per batch was the limit, not the CPU.

Latency shows it too. Each new file joined the back of a longer queue, so
latency climbed steadily: p50 62.7 s, max 116.4 s.

**The warning that did not help.** Spark printed
`Current batch is falling behind. The trigger interval is 5000 milliseconds, but spent 11031 milliseconds`
once, for the slow first batch. It never printed it again, because every
later batch finished inside 5 s. The warning is about **one batch being
slower than the trigger**, not about the backlog. Watch latency or the
count of waiting files instead.

### P2: one setting fixes it

Same load, same trigger, but `MAX_FILES=5`. Service capacity is now
5 files per 5 s = **1 file/s**, twice the arrival rate. Each batch simply
takes whatever arrived since the last one: the batch sizes in
`progress.jsonl` alternate between ~200 and ~300 rows (2 or 3 files). The
queue never builds.

p50 latency fell from 62.7 s to **5.4 s**. That is close to the floor for
this design:

```
expected p50  ≈  half a trigger interval  +  one batch duration
              ≈  5 s / 2                  +  ~2.8 s (median trigger_ms)
              ≈  5.3 s                       (measured: 5.4 s)
```

A file waits on average half a trigger for the next batch to start, then
the batch takes ~3 s. Rows landed: 5,876. The generator spoiled 124 of
6,000 rows, and 6,000 − 124 = 5,876. Nothing lost.

### P3: 20 times the load

1,000 rows every second, `MAX_FILES=10`, trigger 10 s. The job kept up:
902 rows/s landed, batches of ~9,000 to 10,000 rows, mean batch 3.7 s.

Its p50 latency, 16.6 s, is higher than the formula predicts
(10 s / 2 + ~3.1 s ≈ 8 s). The checkpoint shows why. The file log
`checkpoint/perf_P3/sources/0/5` lists the files batch 5 read: exactly 10,
written between 11:07:00 and 11:07:09 UTC. Batch 5 started at 11:07:20, so
ten newer files (11:07:10 to 11:07:19) were already on disk and had to wait
for batch 6. The per-batch latency query below shows the same thing: the
batch that landed at 11:07:22.6 held events from 11:07:00.0 to 11:07:09.6.

```sql
SET timezone = 'UTC';
SELECT ingested_at, count(*),
       round(min(extract(epoch FROM ingested_at - event_time))::numeric, 1) AS min_s,
       round(max(extract(epoch FROM ingested_at - event_time))::numeric, 1) AS max_s
FROM events
WHERE ingested_at BETWEEN '2026-11-04 11:06:00+00' AND '2026-11-04 11:09:30+00'
GROUP BY 1 ORDER BY 1;
--  2026-11-04 11:07:22.604787+00 |  9785 | 13.0 | 22.6
```

The generator wrote one file about every 1.05 s (it sleeps 1 s, plus the
time to write 1,000 rows), so about 9.5 files per 10 s. The cap of 10 files
per batch leaves only ~5% spare capacity, and a whole trigger's worth of
files kept waiting one extra round. Spark's file source also keeps the
unread rest of a folder listing in memory and serves the next batch from
it instead of listing again. The file counts per batch in
`checkpoint/perf_P3/sources/0/` follow that pattern: 3, 10, 2, 10, 6, 10,
9, 10. Each "short" batch (2, 6, 9 files) is the leftover of the listing
before it, even though newer files were already on disk. This is a reading of the checkpoint files,
not a tested fix. The likely fix is more headroom, for example
`MAX_FILES=20`. A good follow-up run.

## 4. Fixed cost vs per-row cost

Every batch pays two kinds of cost:

- **Fixed cost**: paid once per batch, however many rows. Listing the
  folder, planning the query, opening two psycopg2 connections and one JDBC
  connection, two `TRUNCATE`s, one merge transaction, writing the
  checkpoint and state files.
- **Per-row cost**: parsing, casting, the dedup state lookup, sending the
  row over JDBC.

| Batch size | Mean batch duration | JDBC append (median) | Merge (median) |
|---|---|---|---|
| ~100 rows (P1) | 3,037 ms | 1.2 s | 56 ms |
| ~10,000 rows (P3) | 3,746 ms | 1.4 s | 145 ms |

100 times more rows cost only ~0.7 s more. Rough split:

```
per-row cost ≈ (3,746 − 3,037) ms ÷ (10,000 − 100) rows ≈ 0.07 ms per row
fixed cost   ≈ 3,037 ms − 100 × 0.07 ms                ≈ 3.0 s per batch
```

So at this scale **almost all of a batch is fixed cost**. Two lessons:

1. Bigger batches are much cheaper per row. That is why P3 moved 20 times
   P2's input with batches only ~0.4 s longer.
2. A very short trigger (say 1 s) would not help here. Each batch needs
   ~3 s of fixed work, so batches would just run back to back.

The first batch of every run is slower, 6 to 12 s (`trigger_ms` 10,979 in
P1, 12,561 in P2, 12,481 in P3), because Spark compiles code, loads the
JDBC driver and reads the old state from the checkpoint. Leave it out when
you judge the steady state.

## 5. The dial: trigger and `maxFilesPerTrigger`

The two settings work together:

| Setting | `.env` name | What it controls | Turn it up | Turn it down |
|---|---|---|---|---|
| Trigger interval | `TRIGGER` (`5 seconds`) | How often a batch starts | Bigger, cheaper batches; higher latency | Lower latency; more fixed cost per row |
| `maxFilesPerTrigger` | `MAX_FILES` (`1`) | The most files one batch may take | Can catch up after a backlog; bigger batches | Small, predictable batches; can fall behind |

The capacity of the job, in files per second, is at most
`MAX_FILES ÷ trigger seconds`. It must be **above** the arrival rate with
room to spare, so the job can also catch up after a restart or a slow
batch.

| Run | Capacity (files/s) | Arrivals (files/s) | Headroom | Result |
|---|---|---|---|---|
| P1 | 1 ÷ 5 = 0.2 | 0.5 | −60% | Fell behind |
| P2 | 5 ÷ 5 = 1.0 | 0.5 | +100% | Kept up, p50 5.4 s |
| P3 | 10 ÷ 10 = 1.0 | ~0.95 | ~+5% | Kept up, but files waited an extra round |

How to choose, in order:

1. **Pick the trigger from the latency the business needs.** If people
   want rows within ~10 s, a 5 s trigger gives p50 ≈ 5 s.
2. **Set `MAX_FILES` so capacity is at least 2× the arrival rate.**
3. **Check that one batch finishes inside the trigger** (`trigger_ms`
   below the interval). If not, batches run back to back and the trigger
   no longer sets the pace.

`MAX_FILES=1` stays the default in `.env.example` because it makes the
functional tests easy to follow: one file = one batch. The settings table
at the end of `reports/performance_metrics.md` gives the values for real loads.

## 6. Other knobs

These were not changed in the measured runs. Know what each does.

| Knob | Where | Current value | Effect |
|---|---|---|---|
| `spark.sql.shuffle.partitions` | `build_spark()` | `4` | How many pieces data is split into after a shuffle. `dropDuplicates` shuffles, so each batch runs as 4 tasks and the state store has 4 partitions (the `partitionId=0..3` in the logs). The default 200 would mean 200 tiny tasks per batch |
| JDBC `batchsize` | `.option("batchsize", ...)` on the write | default 1,000 | Rows sent per round trip to PostgreSQL |
| JDBC `numPartitions` | `.option("numPartitions", ...)` on the write | not set | Most parallel JDBC connections. Today the 4 partitions give up to 4 connections |
| `reWriteBatchedInserts=true` | in the JDBC URL | not set | A PostgreSQL driver setting that turns a batch of single-row inserts into multi-row inserts. Often faster for the staging append |
| More cores | `.master("local[*]")` | all laptop cores | `*` already uses every core. More power means a cluster (`spark://...` or YARN/Kubernetes) |
| `maxBytesPerTrigger` | — | — | Not available for the CSV file source. Only `maxFilesPerTrigger` |

**Gotcha: shuffle partitions are fixed by the checkpoint.** The number is
saved in the checkpoint (`"spark.sql.shuffle.partitions":"4"` in
`checkpoint/perf_P3/offsets/5`). A stateful query restarted with a new
value keeps the old one. To change it you need a new checkpoint.

## 7. How to reproduce a run

Each run needs its **own input folder and its own checkpoint**. If P2 had
reused P1's folder and checkpoint, it would first have to drain P1's 24
waiting files, and its latency would measure P1's backlog, not P2's
settings. `progress.jsonl` holds such a run: batches 84 to 110 on the
shared checkpoint (10:53 to 10:55 UTC) read ~500 rows at a time, which
looks like P1's leftovers being drained. P2 and P3 were run with
`data/perf_P2` + `checkpoint/perf_P2` and `data/perf_P3` +
`checkpoint/perf_P3`.

Both scripts read `INPUT_DIR` from the environment, and `load_dotenv()`
does not overwrite a variable that is already set. So exporting variables
for one run beats `.env`. A Git Bash helper in that pattern:

```bash
export HADOOP_HOME="$USERPROFILE/hadoop"

perf() {   # perf NAME ROWS EVERY FILES MAX_FILES "TRIGGER"
  local name=$1
  local since
  since=$(date -u +%Y-%m-%dT%H:%M:%SZ)          # UTC, for perf_report.py --since
  INPUT_DIR=data/perf_$name CHECKPOINT_DIR=checkpoint/perf_$name \
  MAX_FILES=$5 TRIGGER="$6" \
    python spark_streaming_to_postgres.py --run-seconds 150 > "reports/logs/perf_${name}_job.log" 2>&1 &
  sleep 30                                      # Spark needs ~25-30 s to start
  INPUT_DIR=data/perf_$name \
    python data_generator.py --rows "$2" --every "$3" --batches "$4" > "reports/logs/perf_${name}_gen.log"
  wait                                          # until the job stops by itself
  python -m src.perf_report --since "$since" --label "$name"
}

perf P2 100 2 60  5 "5 seconds"     # 50 rows/s, 2 minutes
perf P3 1000 1 120 10 "10 seconds"  # 1,000 rows/s, 2 minutes
```

Notes:

- `--batches 60` with `--every 2` is 2 minutes of files. `--bad-rate`
  stays at its default 0.02 (2% spoiled), as in the measured runs.
- `--run-seconds 150` = 30 s start-up + 2 minutes of data. The job then
  calls `query.stop()`, which finishes the current batch cleanly.
- `perf_report.py` takes UTC times. `--until` defaults to now.
- If you rerun with the same name, delete `data/perf_NAME` and
  `checkpoint/perf_NAME` first, or the old files count as already done.

Real output for P2:

```
run: P2
  micro-batches with data : 24
  rows read by Spark      : 5984
  batch duration (ms)     : mean 3306, max 12561
  processed rows/s (Spark): mean 84
  rows landed in Postgres : 5876  (53.3 rows/s)
  latency p50 / p95 / max : 5.4 s / 13.4 s / 15.4 s
```

(The max batch, 12,561 ms, is the slow first batch.)

## 8. Limits of these measurements

Say these before a reviewer does.

- **One laptop.** The generator, Spark and PostgreSQL shared the same CPU
  and disk. On a cluster with a separate database server, both the fixed
  cost and the per-row cost would change.
- **Short runs.** Each run had 2 minutes of data. That cannot show slow
  growth, for example the deduplication state getting bigger over hours.
  The 10-minute watermark should keep it flat, but that is not measured.
- **One run per setting.** No repeats, so no spread. Treat 5.4 s vs 5.3 s
  as "about the same".
- **Warm-up included.** The slow first batch is in the means.
- **The input rate is ideal.** Files arrive at a perfectly steady pace. Real
  traffic comes in bursts, which is where headroom (section 5) matters.
- **File source.** Listing a folder gets slower as the folder fills up. A
  message queue such as Kafka does not have that cost.

## Self-check

1. In P1, how many files arrive per file processed, and why? [2.5. One file
   every 2 s arrives; at most 1 file per 5 s trigger is processed.]
2. Why is `processedRowsPerSecond` 35 in P1 but 2,495 in P3 on the same
   laptop? [It divides rows by batch duration, and batch duration is
   mostly fixed cost. Bigger batches give a bigger number.]
3. What does `percentile_cont(0.95)` return? [The latency value 95% of the
   way up the sorted list: only 5% of rows waited longer.]
4. Why did each performance run get its own input folder and checkpoint?
   [So no run inherits another's backlog or "already done" file list.]
5. You need p50 under 10 s at 200 rows/s, files of 100 rows. Pick settings.
   [Trigger 5 s gives p50 ≈ 2.5 s + ~3 s. Arrivals are 2 files/s = 10 files
   per trigger, so `MAX_FILES` 20 or more for 2× headroom.]
