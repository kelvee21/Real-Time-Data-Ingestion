# real-time data ingestion: spark structured streaming -> postgres

DEM05 lab 2. a script writes fake e-commerce events (views, add to cart,
purchases) as csv files into a folder. spark structured streaming picks up
each new file, cleans and de-duplicates the rows and loads them into
postgres. each micro-batch is written so that running it twice never adds a
row twice. same repo layout as lab 1 (and my pandas tmdb lab): code in
`src/`, tests in `tests/`, write-ups in `reports/`.

- lab brief: [Real-Time-Data-Ingestion-II.md](Real-Time-Data-Ingestion-II.md)
- study notes: [study/](study/), open `study/index.html` in a browser

## run

needs java 17, python 3.10+ and postgres. on windows also winutils.exe with
HADOOP_HOME set, spark writes its checkpoint through hadoop code (see
[study/01-setup-postgres-and-spark.md](study/01-setup-postgres-and-spark.md)).

```
pip install -r requirements.txt
psql -U postgres -v app_password='choose-one' -f postgres_setup.sql
copy .env.example .env                 # PGPASSWORD = the same password
pytest
pyright
python spark_streaming_to_postgres.py  # terminal 1, same as python -m src.main
python data_generator.py               # terminal 2, same as python -m src.data_generator
```

`python -m src.main --once` takes every file waiting in the folder and stops,
`--run-seconds 120` stops after two minutes (that is how the test runs were
done). full steps, checks and reset: [reports/user_guide.md](reports/user_guide.md).

`pytest` needs spark but not postgres. 42 tests on fake rows: the generator,
every cleaning rule, the merge sql, the metrics and the perf report maths.
writing to the real db is tested by the end to end runs in
[reports/test_cases.md](reports/test_cases.md), because `spark_app` may not
delete, so a test row would stay in `events` for good.

`pyright` is the type check. every function has type hints and
`pyrightconfig.json` puts it in strict mode, so vscode shows real types
instead of Unknown. it should say 0 errors.

open analysis.ipynb and run it top to bottom. it calls the same functions as
`src.main`, one step per section, on its own folder (`data/notebook/`). if
postgres is down it says so and skips only the write section.

## deliverables

the brief names nine files. the two scripts it names are kept in the root
but only call into `src/`, so `python data_generator.py` still works.

| deliverable | file |
|---|---|
| data generator | [data_generator.py](data_generator.py) -> [src/data_generator.py](src/data_generator.py) |
| streaming job | [spark_streaming_to_postgres.py](spark_streaming_to_postgres.py) -> [src/main.py](src/main.py) |
| database setup | [postgres_setup.sql](postgres_setup.sql) |
| connection details | [postgres_connection_details.txt](postgres_connection_details.txt) |
| project overview | [reports/project_overview.md](reports/project_overview.md) |
| user guide | [reports/user_guide.md](reports/user_guide.md) |
| test cases | [reports/test_cases.md](reports/test_cases.md): 9 tests, all run, TC-04 found and fixed a real bug |
| performance metrics | [reports/performance_metrics.md](reports/performance_metrics.md): 3 measured load settings |
| architecture diagram | [reports/system_architecture.png](reports/system_architecture.png), drawn by `python -m src.diagram` |

## files

- src/ - all source lives here
- src/config.py - every setting from env vars / .env, folders resolved from the lab root
- src/spark_session.py - the one SparkSession everything uses (jdbc driver, utc, few shuffle partitions)
- src/data_generator.py - part 1: fake events, one csv every few seconds, some rows spoiled on purpose
- src/cleaning.py - part 2: raw schema, read_stream, cast_types, drop_bad_rows, drop_duplicates
- src/postgres_sink.py - part 3: jdbc into events_staging, then one merge into events
- src/metrics.py - per batch log + spark's progress listener, both to metrics/*.jsonl
- src/main.py - glue: starts the query, --once / --run-seconds
- src/perf_report.py - latency + throughput summary of one run (`python -m src.perf_report --since ...`)
- src/diagram.py - draws reports/system_architecture.png
- tests/ - 42 pytest tests on fake rows, no postgres needed
- data_generator.py, spark_streaming_to_postgres.py - the file names the brief asks for
- postgres_setup.sql - database, role, tables, grants (run once with psql)
- pyrightconfig.json - strict type check settings, vscode / pylance read it too
- data/ - incoming csv files (not committed, see data/README.md)
- checkpoint/ - spark's memory of which files are done (not committed)
- metrics/ - batches.jsonl and progress.jsonl, written by the job
- reports/ - the write-ups, the diagram, and logs/ with the console output of every test and performance run
- study/ - my study notes for this lab
- analysis.ipynb - all steps, runs top to bottom

## streaming choices

- **schema up front, everything as text.** a streaming source can not infer
  a schema, and reading text means a bad value becomes a null at the cast
  instead of failing the whole file.
- **try_cast / try_to_timestamp.** spark 4 runs in ansi mode, a plain cast
  of `"unknown"` would kill the batch.
- **the db rules checked in spark first.** `is_valid()` repeats the CHECK
  constraints of `events`, so a bad row is dropped in spark and the merge
  never fails on it.
- **watermark + dropDuplicates.** same event_id within 10 minutes is kept
  once, and the watermark lets spark forget old ids so the state stays small.
- **staging table + `ON CONFLICT DO NOTHING`.** jdbc can only append. if a
  batch is replayed after a crash the merge skips ids already there, so the
  sink is idempotent and the pipeline is effectively exactly once.
- **checkpoint.** which files are done + the dedup state survive a restart.
- **maxFilesPerTrigger.** caps how much one micro-batch takes, so a backlog
  is worked off in steady batches instead of one huge one.
- **atomic rename in the generator.** spark never sees half a file.
- **config from env.** password, folders, trigger and spark settings come
  from .env, nothing secret in code.
- **spark.stop() in finally.** the jvm is released even when a batch fails.
