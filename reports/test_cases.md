# Test Cases: Real-Time Event Ingestion

Manual test plan for the pipeline, with the result of each test when it was
run on 4 November 2026.

**Environment:** Windows 11 laptop, Python 3.13, PySpark 4.3 (`local[*]`),
Java 17, PostgreSQL 18 on `localhost:5432`, database `ecommerce`, role
`spark_app`. Job settings: trigger 5 s, `MAX_FILES=1`, unless the test says
otherwise. The logs for each run are in `logs/`.

**How the expected row count is known exactly.** The generator prints how
many rows it spoiled in each file (`100 rows (23 spoiled)`). Every kind of
spoiling breaks a rule the job enforces, so the rows that reach PostgreSQL
must equal *rows generated − rows spoiled*, no more and no less.

## Summary

| ID | Test | Result |
|---|---|---|
| TC-01 | Generator writes complete CSV files | Pass |
| TC-02 | Spark detects and processes each new file | Pass |
| TC-03 | Columns have the right types in PostgreSQL | Pass |
| TC-04 | Dirty rows are rejected, clean rows all arrive | **Failed first, then Pass** after a fix |
| TC-05 | A replayed batch does not create duplicates | Pass |
| TC-06 | After a restart, old files are not read again | Pass |
| TC-07 | A duplicated input file adds no rows | Pass |
| TC-08 | Database unreachable: no data lost | Pass |
| TC-09 | Throughput keeps up with the input rate | Pass at 2 of 3 settings (see performance_metrics.md) |

---

## TC-01: Generator writes complete CSV files

| | |
|---|---|
| **Preconditions** | `data/incoming/` empty |
| **Steps** | `python -m src.data_generator --rows 100 --every 2 --batches 20 --bad-rate 0.2 --seed 42` |
| **Expected** | 20 files named `events_*.csv`, each with a header and 100 rows. No `_`-prefixed temporary files left behind. |
| **Actual** | 20 files, header + 100 rows each. `ls -a data/incoming \| grep -c '^_'` returned 0. |
| **Result** | Pass |

## TC-02: Spark detects and processes each new file

| | |
|---|---|
| **Preconditions** | Job running (`python -m src.spark_streaming_to_postgres`), empty checkpoint |
| **Steps** | Start the generator from TC-01 while the job runs. Watch the job's console and `metrics/batches.jsonl`. |
| **Expected** | One micro-batch per file (`MAX_FILES=1`), 20 batches with data. |
| **Actual** | Batches 0 to 19 each processed one file (75–89 valid rows each). `metrics/batches.jsonl` had 20 lines. |
| **Result** | Pass |

## TC-03: Columns have the right types in PostgreSQL

| | |
|---|---|
| **Steps** | `\d events` in psql. `SELECT event_time, price FROM events LIMIT 3;` |
| **Expected** | `event_time` is `timestamp with time zone`, `price` is `numeric(10,2)`, `user_id`/`product_id`/`quantity` are `integer`. Times match the generator's UTC times. |
| **Actual** | All types as expected. With `SET timezone='UTC'`, `event_time` values match the CSV strings exactly (no 2-hour shift). |
| **Result** | Pass |

## TC-04: Dirty rows are rejected, clean rows all arrive

| | |
|---|---|
| **Preconditions** | Empty `events` table, empty checkpoint |
| **Steps** | Run TC-01's generator (`--bad-rate 0.2`, seed 42) with the job running. Then: `SELECT count(*) FROM events;` and count rows breaking any rule. |
| **Expected** | 2,000 generated − 385 spoiled = **1,615 rows**, and 0 rows breaking a rule. |
| **Actual, first run** | **1,642 rows.** 27 too many. All 27 had `user_id = -5`. |
| **Cause** | The filter only checked `user_id IS NOT NULL`. The bad value `"-5"` casts to a valid integer, so it passed. |
| **Fix** | Filter changed to `user_id > 0` and `product_id > 0`. The same rule was added to the table as `CHECK (user_id > 0)` and `CHECK (product_id > 0)`, so the database also refuses such rows. |
| **Actual, after the fix** | **1,615 rows**, 1,615 distinct `event_id`s, 0 rule-breaking rows, `events_staging` empty. |
| **Result** | Pass (after fix). The run with the bug is kept in `logs/bug_run/`. |

## TC-05: A replayed batch does not create duplicates

Spark guarantees *at-least-once* delivery to `foreachBatch`: if the job stops
after writing a batch but before recording it as done, it runs that batch
again. This test forces exactly that situation.

| | |
|---|---|
| **Preconditions** | 3,190 rows in `events`. Batch 42 (83 rows) already written and committed. |
| **Steps** | 1. Stop the job. 2. Delete `checkpoint/events/commits/42` (and the empty batch 43's offset and commit files), so Spark believes batch 42 never finished. 3. Start the job. |
| **Expected** | Spark re-runs batch 42. The merge inserts 0 rows. The total stays 3,190. |
| **Actual** | `{"batch_id": 42, "rows": 83, "inserted": 0, "skipped_duplicates": 83}`. Total 3,190, distinct ids 3,190. |
| **Result** | Pass |

A hard kill was also tested: the job was killed with `taskkill /F` while the
generator was writing 15 files, then restarted. All 1,176 valid rows arrived,
with no duplicates (`count = count(DISTINCT event_id) = 3,190`).

## TC-06: After a restart, old files are not read again

| | |
|---|---|
| **Preconditions** | The job from TC-04 stopped after batch 20. 20 files in the folder. |
| **Steps** | Restart the job with the same checkpoint. Add 5 new files. |
| **Expected** | Batch numbers continue from 21. Only the 5 new files are read. |
| **Actual** | First new batch was 21. Batches 22–26 each held one new file. None of the 20 old files was read again. |
| **Result** | Pass |

## TC-07: A duplicated input file adds no rows

| | |
|---|---|
| **Steps** | While the job runs, copy an already-processed file to `events_zz_duplicate_copy.csv` (copy to a `_` name first, then rename). |
| **Expected** | Spark reads it as a new file, but no rows are added. |
| **Actual** | Batch 21 read 98 rows and passed 0 on: the in-stream `dropDuplicates` (watermark 10 minutes) recognised every `event_id`. The total matched the expected 2,014 exactly. |
| **Result** | Pass |

If the copy had arrived more than 10 minutes later, the watermark would have
let those ids out of Spark's memory. The primary key with
`ON CONFLICT DO NOTHING` is the second line of defence that would then skip them.

## TC-08: Database unreachable, no data lost

The database was made unreachable by pointing the job at a port with nothing
listening (`PGPORT=5499`). This has the same effect on the job as stopping
the server, without stopping the shared PostgreSQL service.

| | |
|---|---|
| **Steps** | 1. Generate 3 files. 2. `PGPORT=5499 python -m src.spark_streaming_to_postgres`. 3. Then start the job again with the normal settings. |
| **Expected** | Step 2: the batch fails, the query stops with an error, and the batch is not marked as done. Step 3: the batch is re-run and its rows arrive. |
| **Actual** | Step 2: `psycopg2.OperationalError: ... port 5499 failed: Connection refused`, exit code 1. The checkpoint had offsets for batch 44 but no commit. Step 3: batch 44 re-ran, then 45 and 46. The total reached the expected 3,428, with no duplicates. |
| **Result** | Pass |

## TC-09: Throughput keeps up with the input rate

| | |
|---|---|
| **Steps** | Three 2-minute runs at different loads and settings, each with its own input folder and checkpoint. See `performance_metrics.md`. |
| **Expected** | Rows landing per second ≈ rows generated per second, with latency that stays flat. |
| **Actual** | P1 (50 rows/s, 1 file per batch): **fell behind**. 24 of 60 files were still waiting at the end, and latency rose to 116 s. P2 (same load, 5 files per batch): kept up, p95 latency 13.4 s. P3 (1,000 rows/s, 10 files per batch, 10 s trigger): kept up, p95 21.9 s. |
| **Result** | Fail at P1's settings, Pass at P2 and P3. The default `.env` uses `MAX_FILES=1` for the functional tests. Use 5 or more for real loads. |
