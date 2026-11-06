# 13 — Review-Day Runbook

A live demo script: what to check before the review, the exact commands to
run, what you should see, what to say, and what to do if something fails
in front of the reviewer. Commands are given for PowerShell and Git Bash
where they differ. Run everything from the project folder
`C:\Workstation\Amalitech\Week-5\Real-Time-Data-Ingestion`.

> **Help:** every command's background is in [01 · Setup: PostgreSQL and Spark](01-setup-postgres-and-spark.md) and [../user_guide.md](../reports/user_guide.md). The test you show live is explained in [07 · Testing the pipeline](07-testing-the-pipeline.md). Answers to likely questions are in [10 · Code-review defense](10-code-review-defense.md).

## What state things are in

| Item | State (as of 4 November 2026) |
|---|---|
| PostgreSQL 18 | Windows service on `localhost:5432`, starts at boot |
| Database `ecommerce` | `events`: **138,629** rows, all distinct `event_id`s. `events_staging`: 0 rows |
| Role `spark_app` | Not a superuser. Password only in `.env` |
| Checkpoints | `checkpoint/events`, `checkpoint/perf_P2`, `checkpoint/perf_P3` |
| Evidence | `reports/logs/`, `metrics/*.jsonl`, `reports/test_cases.md`, `reports/performance_metrics.md` |

The demo below uses **its own folders** (`data/demo`, `checkpoint/demo`),
so it never disturbs the recorded runs. You do not need to empty `events`:
every check counts only rows that landed during the demo.

---

## 15 minutes before the review

Open three terminals in the project folder. Call them **JOB**, **GEN**
and **SQL**.

### 1. PostgreSQL is up and the data is there

PowerShell:

```powershell
Get-Service postgresql*
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -w -h localhost -U postgres -d ecommerce -c "SELECT count(*), count(DISTINCT event_id) FROM events;"
```

Git Bash:

```bash
PSQL="/c/Program Files/PostgreSQL/18/bin/psql.exe"
"$PSQL" -w -h localhost -U postgres -d ecommerce -c "SELECT count(*), count(DISTINCT event_id) FROM events;"
```

The service must say `Running`; the query must show two equal numbers
(138629 if nothing was added since). **Connection refused** means the
service is stopped: start it from an admin PowerShell with
`Start-Service postgresql*`, or from the Services app.

### 2. `.env` is present, without showing the password

```powershell
Select-String -Path .env -Pattern "PASSWORD" -NotMatch
```

```bash
grep -v PASSWORD .env
```

You should see `PGHOST=localhost`, `PGPORT=5432`, `PGDATABASE=ecommerce`,
`PGUSER=spark_app` and the four job settings. Check the password works
without printing it:

```powershell
python -c "import os, psycopg2, dotenv; dotenv.load_dotenv(); psycopg2.connect(host=os.getenv('PGHOST'), port=os.getenv('PGPORT'), dbname=os.getenv('PGDATABASE'), user=os.getenv('PGUSER'), password=os.getenv('PGPASSWORD')).close(); print('spark_app login OK')"
```

### 3. Java, Python and winutils

```powershell
java -version                      # 17.x
python --version                   # 3.13
$env:HADOOP_HOME = "C:\Users\User\hadoop"; $env:PATH += ";$env:HADOOP_HOME\bin"
Test-Path "$env:HADOOP_HOME\bin\winutils.exe"     # True
```

```bash
export HADOOP_HOME="C:\\Users\\User\\hadoop"; export PATH="$PATH:/c/Users/User/hadoop/bin"
ls /c/Users/User/hadoop/bin/winutils.exe
```

Set `HADOOP_HOME` in the **JOB** terminal especially. A terminal opened
before winutils was installed does not have it.

### 4. Reset the demo folders

PowerShell:

```powershell
Remove-Item -Recurse -Force data\demo, checkpoint\demo -ErrorAction SilentlyContinue
```

Git Bash:

```bash
rm -rf data/demo checkpoint/demo
```

Never delete `checkpoint/events`: it is the evidence for TC-05 to TC-08.

### 5. Open these files in VS Code, in this order

1. `Real-Time-Data-Ingestion-II.md` (the spec)
2. `reports/system_architecture.png`
3. `data_generator.py`
4. `spark_streaming_to_postgres.py`
5. `postgres_setup.sql`
6. `reports/test_cases.md`
7. `reports/performance_metrics.md`

---

## The demo, in order (about 15 minutes)

### Stage 1: The architecture (2 min)

Open `reports/system_architecture.png` and give the two-minute summary
(end of this page). Then point at the spec's deliverables table and say
that all nine files are in the folder.

### Stage 2: Start the job (1 min, plus ~30 s start-up)

**JOB** terminal, PowerShell:

```powershell
$env:HADOOP_HOME = "C:\Users\User\hadoop"; $env:PATH += ";$env:HADOOP_HOME\bin"
$env:INPUT_DIR = "data/demo"; $env:CHECKPOINT_DIR = "checkpoint/demo"; $env:MAX_FILES = "5"
python spark_streaming_to_postgres.py
```

Git Bash:

```bash
export HADOOP_HOME="C:\\Users\\User\\hadoop"
INPUT_DIR=data/demo CHECKPOINT_DIR=checkpoint/demo MAX_FILES=5 python spark_streaming_to_postgres.py
```

These variables beat `.env`, because `load_dotenv()` never overwrites a
variable that is already set. While Spark starts (25 to 30 s), say:
"Spark is starting the JVM and loading the PostgreSQL JDBC driver."

You should see:

```
query started: id=...
watching data/demo every 5 seconds, up to 5 file(s) per batch
```

Two warnings are normal: `spark.sql.adaptive.enabled is not supported in
streaming ...` and `Disabling AQE since AQE is not supported in stateful
workloads`. AQE (adaptive query execution) is a batch optimisation that
Spark switches off for streaming. Say so if asked.

Note the UTC start time in the **SQL** terminal for later:

```powershell
$since = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ"); $since
```

```bash
since=$(date -u +%Y-%m-%dT%H:%M:%SZ); echo $since
```

### Stage 3: Start the generator (2 min)

**GEN** terminal (`INPUT_DIR` must match the job's folder):

```powershell
$env:INPUT_DIR = "data/demo"
python data_generator.py --rows 100 --every 2 --batches 30 --bad-rate 0.2 --seed 42 | Tee-Object -Variable gen
```

```bash
INPUT_DIR=data/demo python data_generator.py --rows 100 --every 2 --batches 30 --bad-rate 0.2 --seed 42 | tee reports/logs/demo_gen.log
```

The `Tee-Object` / `tee` part shows the output and also keeps it, for
the exact-count check in Stage 4.

GEN prints one line per file: `events_..._.csv: 100 rows (23 spoiled)`.
JOB prints one line per batch: `batch 3: 162 rows, 162 inserted, 1.3 s`.
The first batch takes ~6 s; later ones ~1.3 s inside `write_batch`.

**Say:** "20% of rows are broken on purpose. The generator prints how many
per file, so I know exactly how many rows should arrive: rows written
minus rows spoiled."

Optional: open `http://localhost:4040` while the job runs. The
**Structured Streaming** tab shows input rate and batch duration live.

### Stage 4: Check the database (3 min)

**SQL** terminal. Open psql (`& "C:\Program Files\PostgreSQL\18\bin\psql.exe"
-w -h localhost -U postgres -d ecommerce`, or `"$PSQL" ...` in Git Bash),
then:

```sql
SET timezone = 'UTC';

-- rows from the demo so far (keep re-running while the generator writes)
SELECT count(*) FROM events WHERE ingested_at > now() - interval '10 minutes';

-- the newest rows: times match the CSV exactly (TIMESTAMPTZ)
SELECT event_id, event_time, user_id, event_type, price, ingested_at
FROM events ORDER BY ingested_at DESC LIMIT 5;

-- nothing broken got in
SELECT count(*) AS rule_breakers FROM events
WHERE user_id <= 0 OR product_id <= 0 OR price < 0 OR quantity < 0
   OR event_type NOT IN ('view', 'add_to_cart', 'purchase');

-- no duplicates, staging emptied
SELECT count(*) = count(DISTINCT event_id) AS no_dupes FROM events;
SELECT count(*) FROM events_staging;
```

If the demo has been running longer than 10 minutes, widen the interval.
Expected: the count grows every ~5 s; `rule_breakers` 0; `no_dupes` `t`;
staging 0.

**The exact-count check.** When GEN prints `done: 30 files, 3000 rows`,
add up the spoiled numbers in the **GEN** terminal:

```powershell
($gen | Select-String '\((\d+) spoiled' | ForEach-Object { [int]$_.Matches[0].Groups[1].Value } | Measure-Object -Sum).Sum
```

```bash
python -c "import re;print(sum(int(x) for x in re.findall(r'\((\d+) spoiled', open('reports/logs/demo_gen.log').read())))"
```

Rows in `events` from the demo should equal 3,000 minus that number.

**Say:** "This exact count is how I found the user_id -5 bug in TC-04:
27 rows too many."

### Stage 5: A live test, the duplicate file (2 min)

While the job runs, copy a processed file under a new name. Copy to an
underscore name first, then rename, so Spark never sees half a file.

PowerShell (**GEN** terminal, after the generator finished):

```powershell
$f = Get-ChildItem data\demo\events_*.csv | Select-Object -First 1
Copy-Item $f.FullName data\demo\_dup.csv
Rename-Item data\demo\_dup.csv events_zz_duplicate_copy.csv
```

Git Bash:

```bash
f=$(ls data/demo/events_*.csv | head -1)
cp "$f" data/demo/_dup.csv && mv data/demo/_dup.csv data/demo/events_zz_duplicate_copy.csv
```

What happens: Spark reads the file (it is a new name), `dropDuplicates`
recognises every `event_id`, and the batch has 0 rows to write. JOB
prints **no** `batch` line, because `write_batch` returns early on an
empty batch. Prove it in two places:

```sql
SELECT count(*) FROM events WHERE ingested_at > now() - interval '10 minutes';   -- unchanged
```

```powershell
Get-Content metrics\progress.jsonl -Tail 2
```

```bash
tail -2 metrics/progress.jsonl
```

The last progress line shows `num_input_rows` of about 100 (Spark read
the file), yet nothing was inserted.

**Say:** "In TC-07 the same test read 98 rows and passed 0. If the copy
came after the 10-minute watermark, the primary key with ON CONFLICT DO
NOTHING would skip the rows instead."

**Alternative test: forced replay** (if asked, 3 min). Stop the job with
Ctrl+C. Delete the highest-numbered file in `checkpoint\demo\commits`
(if `offsets` has a higher number than `commits`, delete that offset
too, as TC-05 did for batch 43). Start the job again with the Stage 2
command. JOB prints `batch N: X rows, 0 inserted`. The recorded version:
batch 42, 83 rows, 0 inserted.

### Stage 6: Metrics and the performance report (3 min)

Stop the job with Ctrl+C in **JOB** (it prints `query stopped:
exception=None`). Then:

```powershell
python -m src.perf_report --since $since --label "review demo"
```

```bash
python -m src.perf_report --since "$since" --label "review demo"
```

Point at: batches with data, Spark's rows read, mean batch duration,
landed rows, latency p50 / p95 / max. Then open `reports/performance_metrics.md`
and walk the P1 / P2 / P3 table:

- P1: `MAX_FILES=1` at 50 rows/s fell behind, p50 62.7 s.
- P2: `MAX_FILES=5`, same load: p50 5.4 s, p95 13.4 s.
- P3: 1,000 rows/s, 10 s trigger: 2,495 rows/s processed, p50 16.6 s.

**Say:** "2.5 files arrived for every file processed in P1. The batches
took 3 seconds of their 5, so the cap was the bottleneck, not the machine.
One setting fixed it."

The demo used `--bad-rate 0.2` and a 2 s file interval, so its numbers are
close to P2's but not identical. That is fine: say it.

### Stage 7: Questions (rest of the time)

Keep [10 · Code-review defense](10-code-review-defense.md) open. Lead
with the weak spots before you are asked.

---

## The 2-minute verbal summary

1. **What (20 s).** "A pipeline that moves fake shop events from CSV files
   into PostgreSQL within seconds, using Spark Structured Streaming."
2. **Flow (40 s).** "The generator writes a CSV every few seconds, under
   an underscore name, then renames it, so Spark never reads half a
   file. Every 5 seconds Spark reads up to N new files, reads every
   column as text, casts with try_cast, filters invalid rows, and drops
   duplicate event ids within a 10-minute watermark. foreachBatch
   appends the batch to a staging table by JDBC, then one SQL statement
   moves it into events with ON CONFLICT DO NOTHING and empties staging."
3. **Safety (30 s).** "The checkpoint records which files each batch read
   and whether it finished. A crash means the batch is re-run, and the
   primary key makes the re-run harmless. Tested: forced replay inserted
   0 of 83, a hard kill and a database outage lost nothing, and the exact
   row count caught a real bug."
4. **Performance (20 s).** "p50 latency 5.4 seconds at 50 rows a second;
   it kept up at 1,000 rows a second. The default of one file per batch
   falls behind under load, and I can show the arithmetic."
5. **Limits (10 s).** "One laptop, short runs, a file source not Kafka,
   rejected rows not stored yet."

---

## If something fails live

| Symptom | Likely cause | What to do |
|---|---|---|
| `Connection refused` from psql or the job | PostgreSQL service stopped | `Start-Service postgresql*` (admin), rerun |
| `UnsatisfiedLinkError ... NativeIO$Windows` or `HADOOP_HOME and hadoop.home.dir are unset` | `HADOOP_HOME` not set in this terminal | Set it inline (Stage 2 command), restart the job |
| `password authentication failed for user "spark_app"` | `.env` password differs from the role's | Check step 2. Do not type the password on screen |
| Spark hangs at `:: resolving dependencies` | No internet and the driver not cached | It is cached in `~/.ivy2.5.2` from earlier runs; check Wi-Fi, wait 30 s |
| Job runs, but no `batch` lines | GEN writes to another folder | Both terminals need `INPUT_DIR=data/demo`. Check `ls data/demo` |
| Batches read 0 files on restart | Files already recorded in `checkpoint/demo` | Expected after a restart. Generate new files, or reset (step 4) |
| Latency keeps growing | Falling behind | You started with `MAX_FILES=1`. Say "this is P1", then restart with `MAX_FILES=5` |
| `Current batch is falling behind` once at start | Slow first batch (~6 to 12 s) | Normal. Explain it is about one slow batch, not backlog |
| `Failed to delete ... postgresql-42.7.4.jar` at exit | Windows locks the jar during shutdown | Harmless. Say so and move on |
| `The state for version N doesn't exist in loadedMaps` | First batch after a restart reloads state from disk | Harmless; Spark's own message says it is normal |
| Spark UI on 4041 instead of 4040 | Another Spark session still running | Harmless. Close the other one when convenient |
| Ctrl+C does not stop the job | Shutdown stuck | Second Ctrl+C, or `taskkill /F /IM java.exe` in another terminal. The checkpoint makes this safe (the hard-kill test) |
| Something fails that you can't fix in a minute | — | Stop. Show the recorded evidence instead: `reports/logs/`, `metrics/batches.jsonl`, `reports/test_cases.md`. Every test has its log |

## Self-check

1. Why does the demo use `data/demo` and `checkpoint/demo`? [So it does
   not disturb the recorded runs or inherit their "already done" files.]
2. Why does the duplicate file print no `batch` line? [Its batch has 0
   rows after `dropDuplicates`, and `write_batch` returns early on 0.]
3. Which two warnings are harmless at start-up? [AQE disabled for
   streaming / stateful workloads.]
4. What do you show if the live demo breaks? [The recorded evidence:
   `reports/logs/`, `metrics/*.jsonl`, `reports/test_cases.md`, `reports/performance_metrics.md`.]
