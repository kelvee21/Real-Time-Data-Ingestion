"""Parts 2 and 3 glued together: watch the folder, clean each new file, write to postgres.

    python -m src.main                    # run until ctrl+c
    python -m src.main --run-seconds 120  # stop by itself (what the test runs used)
    python -m src.main --once             # take every file waiting now, then stop

(`python spark_streaming_to_postgres.py` does the same, the brief wants that name.)

settings come from .env through src.config. flow, per micro-batch:
  new csv files -> read as strings -> cast + validate -> drop duplicates
  -> foreachBatch: jdbc insert into events_staging -> sql merge into events
"""
import argparse
from functools import partial
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from src import cleaning, config
from src.metrics import ProgressLogger
from src.postgres_sink import write_batch
from src.spark_session import get_spark


def start_query(
    spark: SparkSession,
    input_dir: Path = config.INPUT_DIR,
    checkpoint_dir: Path = config.CHECKPOINT_DIR,
    metrics_dir: Path = config.METRICS_DIR,
    trigger: str = config.TRIGGER,
    max_files: int = config.MAX_FILES,
    once: bool = False,
) -> StreamingQuery:
    """Start the stream and hand back the query. the caller decides when to stop it.

    the folders are parameters so the notebook can run on its own folder and
    checkpoint without touching the real ones.
    """
    spark.streams.addListener(ProgressLogger(metrics_dir))
    writer = (cleaning.clean(cleaning.read_stream(spark, input_dir, max_files))
              .writeStream
              .queryName("events_to_postgres")
              .foreachBatch(partial(write_batch, metrics_dir=metrics_dir))
              # which files are done + the dedup state. delete it and every
              # file is read again (harmless, the merge skips known ids)
              .option("checkpointLocation", str(checkpoint_dir)))
    # availableNow: everything waiting right now, still split by
    # maxFilesPerTrigger, then the query stops by itself
    writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime=trigger)
    query = writer.start()
    mode = "once" if once else f"every {trigger}"
    print(f"watching {input_dir} {mode}, up to {max_files} file(s) per batch", flush=True)
    return query


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="csv folder -> spark structured streaming -> postgres")
    ap.add_argument("--run-seconds", type=int, default=0, help="stop after N seconds; 0 = run forever")
    ap.add_argument("--once", action="store_true", help="process the files waiting now, then stop")
    args = ap.parse_args(argv)
    run_seconds: int = args.run_seconds
    if not config.PG["password"]:
        print("note: PGPASSWORD is empty, this only works if the server trusts local logins")

    spark = get_spark()
    try:
        query = start_query(spark, once=args.once)
        try:
            if run_seconds:
                query.awaitTermination(run_seconds)
                query.stop()              # clean stop, finishes the batch it is on
            else:
                query.awaitTermination()
        except KeyboardInterrupt:
            query.stop()
    finally:
        spark.stop()                      # release the jvm even when a batch failed


if __name__ == "__main__":
    main()
