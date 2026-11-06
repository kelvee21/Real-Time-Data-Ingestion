# User Guide

How to set up and run the pipeline, check that it works, and reset it.
Commands are for Windows PowerShell. Git Bash variants are given where they
differ.

## 1. Prerequisites

| Need | Check with | Notes |
|---|---|---|
| Java 17 | `java -version` | Spark runs on the JVM |
| Python 3.10+ | `python --version` | Tested with 3.13 |
| PostgreSQL 16+ | `& "C:\Program Files\PostgreSQL\18\bin\psql.exe" --version` | Tested with 18, running on port 5432 |
| winutils (Windows only) | `echo $env:HADOOP_HOME` | `winutils.exe` and `hadoop.dll` in `%HADOOP_HOME%\bin`. Spark's checkpoint writes fail without them |
| Internet, once | | The first run downloads the PostgreSQL JDBC driver from Maven |

## 2. Install

```powershell
pip install -r requirements.txt
```

## 3. Create the database

Run the setup script as the `postgres` superuser. Choose a password for the
pipeline's own login, `spark_app`:

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -p 5432 -U postgres `
    -v app_password='choose-a-password' -f postgres_setup.sql
```

This creates the `ecommerce` database, the `spark_app` role, and the
`events` and `events_staging` tables. Running it again is safe.

## 4. Configure

```powershell
copy .env.example .env
```

Open `.env` and set `PGPASSWORD` to the password from step 3. The other
values work as they are.

## 5. Run

Use two terminals, both in this folder.

**Terminal 1: the streaming job.** Start it first. It takes about 30 seconds
to start, then prints `watching data/incoming every 5 seconds`.

```powershell
$env:HADOOP_HOME = "C:\Users\User\hadoop"      # only if not set system-wide
python -m src.spark_streaming_to_postgres
```

**Terminal 2: the generator.**

```powershell
python -m src.data_generator                          # 100 rows every 2 s, until Ctrl+C
python -m src.data_generator --rows 100 --every 2 --batches 20 --bad-rate 0.2
```

| Generator flag | Default | Meaning |
|---|---|---|
| `--rows` | 100 | rows per file |
| `--every` | 2 | seconds between files |
| `--batches` | 0 | number of files; 0 = until Ctrl+C |
| `--bad-rate` | 0.02 | share of rows given one broken field |
| `--seed` | none | fixed random numbers, for repeatable tests |
| `--out` | `data/incoming` | target folder |

The job prints one line per micro-batch:

```
batch 3: 85 rows, 85 inserted, 1.3 s
```

Stop either program with **Ctrl+C**. To stop the job automatically, use
`python -m src.spark_streaming_to_postgres --run-seconds 120`.

## 6. Check the results

Unit tests first. They need Spark but not PostgreSQL:

```powershell
pytest
pyright
```

`analysis.ipynb` runs the generator, the cleaning steps and one
`--once` pass into PostgreSQL on its own folder (`data/notebook/`), top to bottom.

```powershell
$psql = "C:\Program Files\PostgreSQL\18\bin\psql.exe"
& $psql -h localhost -U spark_app -d ecommerce -c "SELECT event_type, count(*) FROM events GROUP BY 1;"
& $psql -h localhost -U spark_app -d ecommerce -c "SELECT count(*), count(DISTINCT event_id) FROM events;"
```

The two counts in the second query must be equal (no duplicates).

**Latency and throughput** of a run (pass the run's start time, in UTC):

```powershell
python -m src.perf_report --since 2026-11-04T10:50:00Z --label "my run"
```

Per-batch details are in `metrics/batches.jsonl` (rows, rows inserted, write
times) and `metrics/progress.jsonl` (Spark's own rates and durations).

## 7. Tuning

Set these in `.env`, or for one run: `$env:MAX_FILES = "5"`.

| Setting | Default | Effect |
|---|---|---|
| `TRIGGER` | `5 seconds` | How often a micro-batch starts. Longer = bigger, cheaper batches, higher latency |
| `MAX_FILES` | 1 | Most files per batch. Use 5 or more for real loads: with 1, the job falls behind at 50 rows/s |
| `INPUT_DIR` | `data/incoming` | Folder the job watches |
| `CHECKPOINT_DIR` | `checkpoint/events` | Where the job keeps its progress. One per query: never share it |

## 8. Reset

Stop the job first. Then:

```powershell
Remove-Item -Recurse -Force checkpoint
Remove-Item data\incoming\*.csv
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -U postgres -d ecommerce -c "TRUNCATE events, events_staging;"
```

If you delete the checkpoint but keep the files, the job reads every file
again on its next start. The primary key stops duplicate rows, so this is safe.

## 9. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `UnsatisfiedLinkError ... NativeIO$Windows` | winutils missing. Set `HADOOP_HOME` and add `%HADOOP_HOME%\bin` to PATH, then open a new terminal |
| `ClassNotFoundException: org.postgresql.Driver` | The JDBC download failed. Check internet access and run again |
| `password authentication failed for user "spark_app"` | `PGPASSWORD` in `.env` differs from the one used in step 3 |
| `Connection refused` | PostgreSQL is not running, or the port is wrong |
| The job runs but writes nothing | The generator writes to another folder; or every row breaks a rule; or the files were already processed (see the checkpoint) |
| Latency keeps growing | The job is falling behind. Raise `MAX_FILES` (section 7) |
| `Failed to delete ... postgresql-42.7.4.jar` at exit | Windows keeps the jar locked during shutdown. Harmless |
