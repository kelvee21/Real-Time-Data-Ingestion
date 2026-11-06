# 07 — Testing the Pipeline

How a streaming pipeline is tested by hand: deciding the exact expected
result before running, the commands used for each test case, the bug one test
found, the tricks for forcing rare failures, and what was not tested.

> **Help:** the official test plan with expected vs actual for each case is [../reports/test_cases.md](../reports/test_cases.md). This page explains *how* and *why* those tests were done. How to run the project is in [../user_guide.md](../reports/user_guide.md).

## What makes a pipeline testable

A pipeline test asks: "I put X in. Did exactly Y come out?" Three things make
that answerable:

| Need | How this lab provides it |
|---|---|
| **A known input** | the generator, with a fixed `--seed`, a fixed number of files and rows, and a printed count of spoiled rows |
| **An exact expected output** | every spoiled row breaks a rule, so expected = generated − spoiled ([03](03-generator-line-by-line.md#h-3-spoil-bad-data-on-purpose)) |
| **Observability**: you can see what happened inside | one `batch N: rows, inserted` line per batch, `metrics/batches.jsonl`, `metrics/progress.jsonl`, the checkpoint folder, and SQL counts |

Every run's console output was saved to a log file for evidence:

```bash
python spark_streaming_to_postgres.py --run-seconds 120 > reports/logs/runA_job.log 2>&1
python data_generator.py --rows 100 --every 2 --batches 20 --bad-rate 0.2 --seed 42 > reports/logs/runA_gen.log
```

`> file` sends normal output to the file; `2>&1` sends errors to the same
place. Spark writes its warnings to the error stream, so without `2>&1` the
log would miss them. `--run-seconds` makes the job stop by itself, so a test
has a clear end.

## Deriving exact expected counts

"Roughly 80% of rows arrived" is not a test. A test needs one number.

**Step 1: add up the spoiled rows the generator printed.**

```text
events_20261104_123540_223838.csv: 100 rows (13 spoiled)
events_20261104_123542_229546.csv: 100 rows (19 spoiled)
...
events_20261104_123618_410723.csv: 100 rows (26 spoiled)
done: 20 files, 2000 rows
```

```powershell
Select-String '\((\d+) spoiled\)' logs\runA_gen.log |
    ForEach-Object { [int]$_.Matches[0].Groups[1].Value } | Measure-Object -Sum
# Sum : 385
```

**Step 2: expected rows = 2,000 − 385 = 1,615.**

**Step 3: compare with three independent views.**

```sql
-- PostgreSQL: what landed
SELECT count(*), count(DISTINCT event_id) FROM events;      -- 1615 | 1615
```

```powershell
# The job's own log: sum of "rows" in batches.jsonl
python -c "import json; print(sum(json.loads(l)['rows'] for l in open('metrics/batches.jsonl')))"
```

If all three agree with the number you worked out **before** the run, the
test passes. If not, the difference itself is a clue (27 extra rows, below).

The same method gave every other expected total:

| Run | Generated | Spoiled | Expected new rows | Running total |
|---|---|---|---|---|
| A (TC-04) | 2,000 | 385 | 1,615 | 1,615 |
| B (TC-06/07) | 500 + a duplicate file | 101 | 399 | 2,014 |
| C/D (hard kill) | 1,500 | 324 | 1,176 | 3,190 |
| E (TC-05 replay) | 0 | 0 | 0 | 3,190 |
| F/G (TC-08) | 300 | 62 | 238 | 3,428 |

Do not use `numInputRows` from `progress.jsonl` for this check. It is
slightly lower than the rows in the files (98 for a 100-row file) because
Spark drops rows with an empty `event_time` or `user_id` inside the CSV reader,
before counting.

## The tests, one by one

All tests ran with trigger 5 s and `MAX_FILES=1` unless stated, starting from
the reset in [01](01-setup-postgres-and-spark.md#h-reset-to-a-clean-slate)
where the test says "empty".

### TC-01: the generator writes complete files

```powershell
python data_generator.py --rows 100 --every 2 --batches 20 --bad-rate 0.2 --seed 42
(Get-ChildItem data\incoming -Filter 'events_*.csv').Count          # 20
(Get-ChildItem data\incoming -Force -Filter '_*').Count             # 0: no temp files left
(Get-Content data\incoming\events_20261104_123540_223838.csv).Count  # 101 = header + 100
```

Result: 20 files, header + 100 rows each, 0 leftover `_` files.

### TC-02: Spark picks up each new file

Start the job first, then the generator. Watch the console. Expected: one
batch per file. Actual: batches 0–19, 74–87 valid rows each, 20 lines in
`batches.jsonl`.

### TC-03: types are right in PostgreSQL

```sql
\d events
SET timezone = 'UTC';
SELECT event_time, price FROM events LIMIT 3;
```

`event_time` is `timestamp with time zone`, `price` is `numeric(10,2)`, and
times match the CSV text exactly. This test exists because of the earlier
+2 hour bug ([05](05-postgres-jdbc-and-the-merge.md#h-6-timestamptz-vs-timestamp-the-2-hour-story)).

### TC-04: dirty rows rejected, clean rows all arrive

Expected 1,615 rows and 0 rule-breakers. Story below.

### TC-05: a replayed batch adds no duplicates

Forces a re-run of a finished batch (the replay trick, below). Result:
`{"batch_id": 42, "rows": 83, "inserted": 0, "skipped_duplicates": 83}`,
total unchanged at 3,190.

### TC-06: a restart does not re-read old files

```powershell
python spark_streaming_to_postgres.py --run-seconds 120     # same checkpoint as TC-04
python data_generator.py --rows 100 --every 2 --batches 5 --bad-rate 0.2
```

Batch numbers continued (21, then 22–26 for the 5 new files). None of the 20
old files was read again.

### TC-07: a duplicated input file adds no rows

While the job ran, an already-processed file was copied in, using the same
`_`-then-rename trick as the generator so Spark never saw a half-copied file:

```powershell
Copy-Item data\incoming\events_20261104_123540_223838.csv data\incoming\_events_zz_duplicate_copy.csv
Rename-Item data\incoming\_events_zz_duplicate_copy.csv events_zz_duplicate_copy.csv
```

Batch 21 read 98 rows and passed 0 on. Total exactly 2,014. The in-stream
`dropDuplicates` caught it; the primary key would have caught it otherwise.

### Hard kill (part of TC-05's evidence)

```powershell
# while the generator writes 15 files (100 rows every 1 s, bad-rate 0.2)
Get-Process java                          # find the Spark JVM's process id
taskkill /F /T /PID <pid>                 # /F = force, no clean shutdown
python spark_streaming_to_postgres.py --run-seconds 120   # restart
```

The restart began with the interrupted batch 28 and took all 15 files: 1,176
rows, exactly 1,500 − 324. No duplicates. Details in
[06](06-reliability-checkpoints-exactly-once.md#h-hard-kill-the-job-is-killed-mid-batch).

### TC-08: database unreachable

Described below under "Simulating a database outage".

### TC-09: throughput keeps up

Three 2-minute runs (P1, P2, P3) with their own input folder and checkpoint.
P1 (50 rows/s, `MAX_FILES=1`) fell behind; P2 and P3 kept up. The numbers and
why are in [08 · Performance and tuning](08-performance-and-tuning.md). A
"Fail at one setting" result is still a useful result: it shows where the
limit is and which knob moves it.

## TC-04: the test that found a real bug

**First run: 1,642 rows. Expected 1,615. 27 too many.**

The difference is the clue. 27 rows got through that should not have. Which
rule did they break?

```sql
SELECT count(*) FROM events
WHERE user_id <= 0 OR product_id <= 0 OR price < 0 OR quantity < 0
   OR event_type NOT IN ('view', 'add_to_cart', 'purchase');     -- 27

SELECT user_id, count(*) FROM events WHERE user_id <= 0 GROUP BY user_id;
-- -5 | 27
```

All 27 had `user_id = -5`. A quick sanity check: of 385 spoiled rows, about
1 in 4 hit `user_id` and about 1 in 4 of those got `"-5"`, so roughly
385 ÷ 16 ≈ 24 are expected. 27 fits.

**Cause.** The filter said:

```python
F.col("user_id").isNotNull()
```

The text `"-5"` passes `try_cast(user_id AS INT)` as the valid integer -5. It
is not null, so the rule passed it. The other bad values (`""`, `unknown`,
`NaN`) become null and were caught; that is why only 27 rows leaked and not
all spoiled `user_id`s. And the same `"-5"` in `price` *was* caught, because
`price >= 0` was already a range check.

**Fix, in two places.**

```python
& (F.col("user_id") > 0)        # "> 0" also rejects null; "-5" casts to a valid INT
& (F.col("product_id") > 0)
```

```sql
user_id    INT NOT NULL CHECK (user_id > 0),
product_id INT NOT NULL CHECK (product_id > 0),
```

The Spark filter keeps bad rows out; the database `CHECK` refuses them even
if a future code change breaks the filter.

**Re-run with the same seed: 1,615 rows**, 1,615 distinct ids, 0
rule-breakers, `events_staging` empty. The generator logs of the two runs show
identical spoiled counts file by file, so the only change was the fix. The
buggy run is kept in `reports/logs/bug_run/` (its `batches.jsonl` adds up to 1,642).

**Lessons.**

1. **"Not null" is not "valid".** A value can be present, of the right type,
   and still wrong. Write the rule the business means (`id > 0`), not the
   rule the type system gives you for free.
2. **Exact counts catch quiet bugs.** Nothing crashed. Every batch printed
   `N rows, N inserted`. Only the comparison with a number worked out in
   advance showed the problem. A test that checks "some rows arrived" would
   have passed.
3. **Keep the failed run.** A test plan that shows "Failed first, then Pass"
   with the evidence is more convincing than a page of passes.
4. **Fix the class, not the instance.** The bug was in `user_id`, but
   `product_id` had the same weakness (the generator never spoils it, so no
   test would show it). Both got the fix.

## The replay trick

The most important failure (dying after the merge but before Spark's commit)
lasts a few milliseconds. You cannot hit it by pressing Ctrl+C at the right
moment. So it was **created** by editing the checkpoint:

```powershell
# 1. stop the job cleanly; note the last batch with data (42, 83 rows)
# 2. make batch 42 look unfinished
Remove-Item checkpoint\events\commits\42
# batch 43 was an empty no-data batch after 42; remove it so 42 is the latest
Remove-Item checkpoint\events\offsets\43, checkpoint\events\commits\43
# 3. start the job again
python spark_streaming_to_postgres.py --run-seconds 60 > reports/logs/runE_replay_job.log 2>&1
```

On start, Spark found `offsets/42` without `commits/42` and re-ran batch 42,
exactly as after a real crash in that window. The log line
`batch 42: 83 rows, 0 inserted` is the proof that the merge is idempotent.

**Only in a test environment.** Hand-editing a production checkpoint can lose
or duplicate data. Here it was safe because the expected result was known and
the whole state could be reset.

## Simulating a database outage safely

The PostgreSQL service on this laptop is shared with other projects. Stopping
it needs admin rights and would cut off everything else using it. The job,
though, only sees "I could not connect". So the outage was simulated by
pointing **only this job** at a port where nothing listens:

```bash
# Git Bash: the variable applies to this one command
PGPORT=5499 python spark_streaming_to_postgres.py > reports/logs/runF_dbdown_job.log 2>&1
```

```powershell
# PowerShell equivalent
$env:PGPORT = "5499"; python spark_streaming_to_postgres.py; Remove-Item Env:PGPORT
```

`load_dotenv()` does not overwrite a variable that is already set, so 5499
wins over `.env`'s 5432.

Result: `psycopg2.OperationalError: connection to server at "localhost" ...
port 5499 failed: Connection refused`, the query stopped, exit code 1. The
checkpoint had `offsets/44` and no `commits/44`. Restarted with normal
settings, batches 44, 45, 46 wrote 80 + 82 + 76 = 238 rows = 300 − 62. Total
3,428, no duplicates.

What this does **not** reproduce: a server that goes away **during** a batch
(mid-JDBC write or mid-merge). The design should handle it (staging is
truncated on retry, the merge is one transaction), but it was not shown.

## What was not tested

Be ready to say this in review. Knowing the limits is part of testing.

| Not tested | Why it matters | How you would test it |
|---|---|---|
| **Long runs** (hours, days) | state and checkpoint size over time; a folder with thousands of files makes each listing slower; the `events` table and its indexes grow | run for a day at a steady rate; watch `state/` size, batch duration and latency for drift |
| **A real outage of the Windows service** | a stop during a write, dropped connections mid-transaction, the time the service takes to come back | in a private PostgreSQL (e.g. a Docker container), stop it mid-run with `pg_ctl stop -m immediate` |
| **Multiple writers** | two copies of the job share `events_staging`: one's `TRUNCATE` would wipe the other's staged rows. Two jobs on one checkpoint corrupt it | run two jobs with separate checkpoints and see it fail; fix with one staging table per job or a temp table per batch |
| **Late data beyond the watermark** | rows more than 10 minutes late are dropped by the stateful step | generate files with old `event_time`s |
| **Malformed files** | wrong column count, broken quotes, a different header | hand-made bad CSVs; check nothing crashes and what is dropped |
| **Automatic restart** | after TC-08 the job stays stopped until someone starts it | a service manager or scheduler with restart-on-failure |

## Self-check

1. How was the expected count 1,615 obtained before the run? (2,000 rows
   generated − 385 spoiled, summed from the generator's printed counts; every
   spoil breaks a rule.)
2. Why did 27 rows with `user_id = -5` pass the first version? (`"-5"` casts
   to a valid INT and the rule only checked `IS NOT NULL`.)
3. Why delete `commits/42` instead of killing the job at the right moment?
   (The after-merge, before-commit window is milliseconds long; editing the
   checkpoint creates it on demand.)
4. Why was the database "taken down" with a wrong port? (The service is shared;
   a wrong port gives the job the same connection error without affecting
   anyone else.)
5. Name one thing a two-copy deployment of this job would break. (The shared
   `events_staging`: one job's `TRUNCATE` deletes the other's staged rows.)
