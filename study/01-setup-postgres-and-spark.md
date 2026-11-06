# 01 — Setup: PostgreSQL, Spark and Everything Between

What has to be installed and running before the pipeline works, how to check
each piece, how to reset to a clean slate, and what the real errors from this
lab mean.

> **Help:** if you have never seen streaming before, read [02 · Batch vs streaming](02-batch-vs-streaming.md) next. This page is only about getting the machine ready.

## The pieces and what each one does

The pipeline has three programs and one folder between them:

```
data_generator.py ──CSV files──> data/incoming/ ──> spark_streaming_to_postgres.py ──> PostgreSQL (ecommerce)
                                                         │
                                                         └──> checkpoint/events/  (Spark's memory)
```

| Piece | What it is | Why this lab needs it |
|---|---|---|
| **Python 3.13** | the language both scripts are written in | runs the generator and drives Spark |
| **Java 17** | the runtime Spark itself is written for (Spark runs on the JVM, the Java Virtual Machine) | PySpark is only a Python remote control; the real engine is a Java process |
| **PySpark 4.3** | the Python package for Apache Spark, an engine that processes data in parallel | reads the CSV files as a stream, cleans them, hands each batch to our write code |
| **`local[*]`** | Spark's "run everything on this one machine, one worker thread per CPU core" mode | no cluster needed for the lab |
| **winutils + `HADOOP_HOME`** | two small Windows helper files from the Hadoop project | Spark writes its checkpoint through Hadoop's file-system code, which needs them on Windows |
| **PostgreSQL 18** | a relational database server, here a Windows service on `localhost:5432` | the place the cleaned events end up |
| **psql** | PostgreSQL's command-line client | runs `postgres_setup.sql` and lets you check results |
| **JDBC driver** `org.postgresql:postgresql:42.7.4` | a Java library that lets a Java program (Spark) talk to PostgreSQL | Spark's bulk write into the staging table |
| **psycopg2** | a Python library that talks to PostgreSQL | the small SQL steps after each batch (truncate, merge) |
| **python-dotenv** | reads a `.env` file into environment variables | keeps the password out of the code |

## 1. Java 17

Spark 4 needs Java 17 or newer. Check what you have:

```powershell
java -version
echo $env:JAVA_HOME
```

You want a line like `openjdk version "17..."`. If `java` is not found,
install a JDK 17 (for example Eclipse Temurin), set `JAVA_HOME` to its folder
and open a **new** terminal.

## 2. Python packages

```powershell
pip install -r requirements.txt
```

`requirements.txt` holds four packages, each with a comment saying why:

```text
pyspark>=3.5            # tested with 4.3 on Python 3.13; needs Java 17
psycopg2-binary>=2.9    # the merge step (staging -> events) after each micro-batch
python-dotenv>=1.0      # reads connection settings from .env
matplotlib>=3.8         # only for src/diagram.py
```

Check that PySpark imports and which version you got:

```powershell
python -c "import pyspark; print(pyspark.__version__)"
```

On PySpark 4 a FutureWarning about pandas 3 appears at start-up in every log
(`PySpark does not yet fully support pandas >= 3.0.0`). The job does not use
pandas, so it is harmless.

## 3. winutils and HADOOP_HOME (Windows only)

**Why.** Spark saves its progress (the *checkpoint*, see
[06](06-reliability-checkpoints-exactly-once.md)) through Hadoop's local
file-system code. On Windows that code calls two native helpers,
`winutils.exe` and `hadoop.dll`. Without them, writes fail with
`UnsatisfiedLinkError ... NativeIO$Windows`. With `spark.jars.packages` set,
the session may not even start.

**Steps** (done once):

1. Get `winutils.exe` and `hadoop.dll` (the `cdarlint/winutils` repo, folder
   `hadoop-3.3.6/bin`) and put them in `C:\Users\User\hadoop\bin`.
2. Set the variables for your user:

```powershell
[Environment]::SetEnvironmentVariable('HADOOP_HOME', "$env:USERPROFILE\hadoop", 'User')
[Environment]::SetEnvironmentVariable('Path', [Environment]::GetEnvironmentVariable('Path','User') + ";$env:USERPROFILE\hadoop\bin", 'User')
```

3. Open a **new** terminal (or restart VS Code) and check:

```powershell
echo $env:HADOOP_HOME          # C:\Users\User\hadoop
Test-Path "$env:HADOOP_HOME\bin\winutils.exe"   # True
```

## 4. PostgreSQL and psql

PostgreSQL 18 runs as a **Windows service**, so it starts with the machine.
Check it is up:

```powershell
Get-Service postgresql*                      # Status should be Running
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -p 5432 -U postgres -c "SELECT version();"
```

`psql` is usually not on the PATH, so this page always uses the full path.
In PowerShell the `&` in front is the *call operator*: it runs a program whose
path is in quotes. In Git Bash the same command is
`"/c/Program Files/PostgreSQL/18/bin/psql.exe" ...`.

## 5. Create the database, role and tables

Run `postgres_setup.sql` once, as the superuser `postgres`:

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -p 5432 -U postgres `
    -v app_password='pick-a-password' -f postgres_setup.sql
```

| Flag | Meaning |
|---|---|
| `-h localhost -p 5432` | which server: host and port |
| `-U postgres` | log in as the superuser (only a superuser may create a database and a role) |
| `-v app_password='...'` | define a **psql variable** named `app_password`. Inside the script, `:'app_password'` is replaced by that value, already wrapped in single quotes as a SQL string |
| `-f postgres_setup.sql` | run the file |

So the password is typed once on the command line and never stored in the
repository. The script uses it to create the login role `spark_app` (the
user Spark connects as). It creates database `ecommerce`, tables `events` and
`events_staging`, two indexes, and the grants. It is **safe to re-run**: every
step checks first. Every line is explained in
[05 · PostgreSQL, JDBC and the merge](05-postgres-jdbc-and-the-merge.md#h-2-creating-the-database-the-gexec-trick).

Check it worked:

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -U spark_app -d ecommerce -c "\dt"
```

You should see `events` and `events_staging`, owned by `postgres`.

## 6. Create .env from .env.example

The code reads its settings from environment variables. `python-dotenv` loads
them from a file called `.env`. The repository ships a template:

```text
# Copy to .env and fill in. .env is gitignored, never commit it.
PGHOST=localhost
PGPORT=5432
PGDATABASE=ecommerce
PGUSER=spark_app
PGPASSWORD=

# Streaming job knobs (see reports/performance_metrics.md)
INPUT_DIR=data/incoming
CHECKPOINT_DIR=checkpoint/events
TRIGGER=5 seconds
MAX_FILES=1
```

```powershell
Copy-Item .env.example .env
notepad .env        # put the same password you gave -v app_password after PGPASSWORD=
```

`.gitignore` lists `.env` on its first line, so git never picks it up. It also
ignores `data/`, `checkpoint/`, `metrics/*.jsonl` and `reports/logs/`: those are
produced by running the pipeline, not source code.

If `PGPASSWORD` is empty, the job prints
`note: PGPASSWORD is empty; this only works if the server trusts local logins`
and carries on. It will fail at the first batch unless the server allows
password-less local logins.

## 7. First run

Always run from the project folder: `INPUT_DIR` and `CHECKPOINT_DIR` are
relative paths. Two terminals:

```powershell
# terminal 1: the streaming job
python spark_streaming_to_postgres.py

# terminal 2: the generator (20 files of 100 rows, one every 2 s)
python data_generator.py --rows 100 --every 2 --batches 20
```

What to expect in terminal 1:

1. **The JDBC driver download.** The first time ever, `spark.jars.packages`
   fetches `org.postgresql:postgresql:42.7.4` (and its one dependency
   `checker-qual`) from Maven Central. This needs internet **once**. After
   that it comes from the local Ivy cache (on this machine
   `C:\Users\User\.ivy2.5.2\jars`) and the log says
   `0 artifacts copied, 2 already retrieved`.
2. **Start-up takes about 25–30 seconds.** The JVM starts, the driver jar
   loads, Spark plans the query.
3. Two harmless warnings:
   `spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled`
   and `Disabling AQE since AQE is not supported in stateful workloads`.
   AQE (Adaptive Query Execution) re-plans batch queries while they run;
   Spark turns it off for streaming queries that keep state, such as our
   de-duplication. Nothing to fix.
4. `query started: id=...`, then
   `watching data/incoming every 5 seconds, up to 1 file(s) per batch`.
5. One line per batch, for example `batch 0: 87 rows, 87 inserted, 5.9 s`.
   The first batch is slow (6–12 s) because Spark compiles the query and
   opens connections; later ones take about 1.2 s.

Stop with **Ctrl+C**. On Windows the shutdown always prints an ERROR like
`Exception while deleting Spark temp dir ... Failed to delete: ...\org.postgresql_postgresql-42.7.4.jar`.
Windows will not delete a jar file the JVM still has open. It is harmless:
the data is already saved.

## Reset to a clean slate

Use this before a test that needs empty starting conditions (most of
[07](07-testing-the-pipeline.md) does).

```powershell
# 1. Stop the job first (Ctrl+C) and wait for it to exit.
# 2. Forget everything Spark has processed:
Remove-Item -Recurse -Force checkpoint\events
# 3. Remove old input files, so they are not read again as "new":
Remove-Item data\incoming\*.csv
# 4. Empty the table. spark_app may not TRUNCATE events (least privilege), so use postgres:
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -U postgres -d ecommerce -c "TRUNCATE events;"
# Optional: start the metrics logs fresh
Remove-Item metrics\*.jsonl
```

The order matters. **Never delete the checkpoint while the job is running**,
and never delete it without also clearing the input files and the table.
A new checkpoint plus old files means every old file is "new" again. (The
primary key would still block duplicates in `events`, but the test counts
would be meaningless.)

## Troubleshooting

Every error below was met for real while building this lab.

| Symptom | Cause | Fix |
|---|---|---|
| `[PATH_NOT_FOUND] Path does not exist: file:/.../data/incoming` at start | the file source needs the input folder to exist before the query starts | the job now runs `os.makedirs(INPUT_DIR, exist_ok=True)` first; if you see it, you are running an old copy or from the wrong folder |
| `psycopg2.OperationalError: connection to server at "localhost" ... port 5432 failed: Connection refused` | nothing is listening on that host and port: the service is stopped, or `PGPORT` is wrong (TC-08 caused it on purpose with port 5499) | `Get-Service postgresql*`, start it; check `PGHOST`/`PGPORT` in `.env` |
| `FATAL: password authentication failed for user "spark_app"` | `PGPASSWORD` in `.env` differs from the one given to `-v app_password` | fix `.env`, or reset the password as `postgres`: `ALTER ROLE spark_app PASSWORD '...';` (re-running the setup script does **not** change an existing role) |
| `java.lang.ClassNotFoundException: org.postgresql.Driver` | the JDBC jar is not on Spark's classpath: the first-run download failed (no internet), or a Spark session already existed without the package | check internet, look for `found org.postgresql#postgresql;42.7.4` in the log; restart Python so `getOrCreate()` builds a new session with the setting |
| `UnsatisfiedLinkError: ... NativeIO$Windows.access0` (or `HADOOP_HOME and hadoop.home.dir are unset`) | winutils/`hadoop.dll` missing, or the terminal was opened before `HADOOP_HOME` was set | redo [step 3](#h-3-winutils-and-hadoop-home-windows-only) and open a new terminal |
| Stream runs, prints no `batch ...` lines, table stays empty | (a) the files are in a different folder than `INPUT_DIR`, e.g. you started from another directory; (b) the checkpoint already lists those files as done; (c) the files are still named `_...` or `....` (Spark skips those); (d) every row fails the cleaning rules (then `rows == 0` and `write_batch` returns without printing) | run from the project folder; check `metrics/progress.jsonl` for `num_input_rows`; for (b) do the full reset above |
| `Missing Python executable 'C:\Program Files\Python313\python.exe' ...` and `The system cannot find the path specified.` at the top of the log | Spark's launcher script guessing where Spark is installed | harmless here: the job sets `PYSPARK_PYTHON` to the running interpreter and starts normally |

> **Help:** what each log line of a normal run means is in [04 · The streaming job line by line](04-streaming-job-line-by-line.md#h-6-write-batch-the-sink).

## Self-check

1. Why does a Python program need Java installed? (PySpark only sends
   instructions; the Spark engine runs in a Java process.)
2. What does `-v app_password='x'` do, and where does `x` end up?
   (Defines a psql variable; the script uses it as the password of role
   `spark_app`. The same value goes in `.env` as `PGPASSWORD`.)
3. Why does the reset need the `postgres` user for `TRUNCATE events`?
   (`spark_app` only has `SELECT, INSERT` on `events`: least privilege.)
4. You deleted `checkpoint/events` but not the CSV files. What happens on
   the next start? (Spark sees every old file as new and re-reads it all.)
5. Is the `Failed to delete ... postgresql-42.7.4.jar` error a problem?
   (No. Windows keeps the jar locked during shutdown; data is already saved.)
