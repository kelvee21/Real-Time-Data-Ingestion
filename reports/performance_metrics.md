# Performance Metrics

Measured on 4 November 2026 on a Windows 11 laptop: Spark 4.3 in `local[*]`
mode, PostgreSQL 18 on the same machine. Every number below comes from a
real run. The raw data is in `metrics/progress.jsonl`,
`metrics/batches.jsonl` and `logs/perf_*`.

## What was measured, and how

| Metric | Meaning | Source |
|---|---|---|
| **Input rate** | Rows the generator wrote per second | generator settings (`--rows` ÷ `--every`) |
| **Batch duration** | Time Spark took for one micro-batch, read to merge | `triggerExecution` in Spark's progress report, logged by the `ProgressLogger` listener |
| **Processed rows/s** | Rows in a batch ÷ batch duration (Spark's own figure) | `processedRowsPerSecond` |
| **Landed rows/s** | Rows that reached `events`, per second of the run | `count(*) / (max(ingested_at) − min(ingested_at))` |
| **End-to-end latency** | Time from the event happening to the row being in PostgreSQL | `ingested_at − event_time`, as p50 / p95 / max |
| **JDBC / merge time** | Time of the two writes inside each batch | `metrics/batches.jsonl` |

`src/perf_report.py --since <UTC time>` prints all of these for one run.

Each run lasted about 2.5 minutes: 30 s to start Spark, then 2 minutes of
generated files. Runs P2 and P3 used their own input folder and checkpoint,
so no run inherited another's backlog. 2% of rows were spoiled on purpose.

## Results

| Run | Input | Trigger | Files per batch | Batches | Mean batch (ms) | Spark rows/s | Landed rows/s | Latency p50 / p95 / max |
|---|---|---|---|---|---|---|---|---|
| **P1** | 50 rows/s (100 every 2 s) | 5 s | 1 | 36 | 3,037 | 35 | **20.5** | 62.7 s / 113.2 s / 116.4 s |
| **P2** | 50 rows/s (100 every 2 s) | 5 s | 5 | 24 | 3,306 | 84 | 53.3 | **5.4 s / 13.4 s** / 15.4 s |
| **P3** | 1,000 rows/s (1,000 every 1 s) | 10 s | 10 | 15 | 3,746 | 2,495 | 902.1 | 16.6 s / 21.9 s / 22.6 s |

Inside each batch (medians from `metrics/batches.jsonl`), the JDBC append to
`events_staging` took 1.2 s and the SQL merge into `events` 56 ms for batches
of ~100 rows. For P3's batches of ~10,000 rows they took 1.4 s and 145 ms.
The first batch of every run is slower (6–12 s) because Spark compiles the
query and opens connections.

## What the numbers say

**1. P1 falls behind, and the cause is the settings, not the hardware.**
With `MAX_FILES=1` and a 5 s trigger, the job can take at most one file every
5 s. The generator writes one every 2 s, so 2.5 files arrive for each one
processed. The queue only grows: after 2 minutes, 24 of 60 files were still
waiting, and latency climbed steadily to almost 2 minutes. The machine was
not the limit: each batch took ~3 s of its 5 s slot. (Spark's
`Current batch is falling behind` warning appeared only once per run, for the
slow first batch, so it is not a reliable backlog alarm. Watch latency or the
number of waiting files instead.)

**2. P2 fixes it with one setting.** Same load, same trigger, but
`MAX_FILES=5`. Each batch now takes everything that arrived since the last
one (about 2–3 files), so the queue never builds. p50 latency drops from
62.7 s to 5.4 s. That is roughly half a trigger interval plus one batch
duration, which is the floor for a 5 s micro-batch design.

**3. Bigger batches are cheaper per row.** A batch of 100 rows took ~3 s. A
batch of ~10,000 rows (P3) took ~3.7 s. Most of a batch's time is fixed cost
(planning, the JDBC connection, the merge transaction), not per-row work.
So P3 handled 20 times the input of P2 with only slightly longer batches,
at 2,495 rows/s processed.

**4. Latency vs throughput is a dial.** P3's longer trigger (10 s) gives
bigger, more efficient batches, but every event waits longer for its batch
to start: p50 16.6 s vs 5.4 s. Pick the trigger from the latency the
business needs, then set `maxFilesPerTrigger` high enough that the job keeps up.

## Limits of these measurements

- Everything ran on one laptop, so the generator, Spark and PostgreSQL competed
  for the same CPU and disk. A cluster with a separate database would behave
  differently.
- Runs were 2 minutes long. A longer run would show whether state (the
  deduplication store) slowly grows. The 10-minute watermark should keep it flat.
- `numInputRows` is a little lower than the rows written (e.g. 5,984 of 6,000 in
  P2). Spark pushes the `IS NOT NULL` checks down into the CSV reader, so rows
  with an empty `event_time` or `user_id` are dropped before they are counted.

## Settings to use

| Situation | Trigger | `MAX_FILES` |
|---|---|---|
| Functional tests (one file = one batch, easy to follow) | 5 s | 1 |
| ~50 rows/s, low latency | 5 s | 5 |
| ~1,000 rows/s | 10 s | 10 or more |
