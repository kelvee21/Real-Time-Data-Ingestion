"""Part 2: read the csv folder as a stream, then cast, validate and de-duplicate.

one function per step so the tests (and the notebook) can check each rule on
its own. every function here also works on a normal batch DataFrame, that is
how the tests feed it rows without starting a stream.
"""
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

COLS = ["event_id", "event_time", "user_id", "session_id", "event_type",
        "product_id", "category", "price", "quantity"]
EVENT_TYPES = ["view", "add_to_cart", "purchase"]
TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"

# every column comes in as text. a bad value then turns into a null at the
# cast step, instead of failing the whole file at read time.
RAW_SCHEMA = StructType([StructField(c, StringType()) for c in COLS])


def read_stream(spark: SparkSession, input_dir: Path, max_files: int) -> DataFrame:
    """The source: every new csv in input_dir, at most max_files per micro-batch."""
    input_dir.mkdir(parents=True, exist_ok=True)   # the file source dies on a missing folder
    return (spark.readStream
            .schema(RAW_SCHEMA)                    # streaming sources need the schema up front
            .option("header", True)
            .option("maxFilesPerTrigger", max_files)
            .csv(str(input_dir)))


def cast_types(raw: DataFrame) -> DataFrame:
    """Text -> real types. try_ versions give null on bad input instead of
    killing the batch (spark 4 runs in ansi mode, a plain cast would throw)."""
    return (raw
        .withColumn("event_time", F.expr(f"try_to_timestamp(event_time, '{TIME_FORMAT}')"))
        .withColumn("user_id", F.expr("try_cast(user_id AS INT)"))
        .withColumn("product_id", F.expr("try_cast(product_id AS INT)"))
        .withColumn("price", F.expr("try_cast(price AS DECIMAL(10,2))"))
        .withColumn("quantity", F.expr("try_cast(quantity AS INT)"))
        .withColumn("event_type", F.lower(F.trim("event_type")))
        .withColumn("category", F.lower(F.trim("category"))))


def is_valid() -> Column:
    """The same rules the events table has as CHECK constraints, checked in spark
    first so a bad row is dropped here and never fails the merge."""
    return (F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
        & (F.col("user_id") > 0)            # "> 0" also drops null, and "-5" casts fine so it needs this
        & (F.col("product_id") > 0)
        & F.col("event_type").isin(EVENT_TYPES)
        & (F.col("price") >= 0)             # null price fails this too
        & (F.col("quantity").isNull() | (F.col("quantity") >= 0)))


def drop_bad_rows(typed: DataFrame) -> DataFrame:
    return typed.filter(is_valid())


def drop_duplicates(valid: DataFrame) -> DataFrame:
    """Same event_id seen twice within 10 minutes of event time -> keep one.

    the watermark tells spark when it may forget old ids, so the dedup state
    does not grow forever. event_time is in the key so spark can drop old state.
    a copy that comes later than that is still caught by the primary key in
    postgres (ON CONFLICT DO NOTHING), so this is the first line, not the only one.
    """
    return (valid
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["event_id", "event_time"]))


def clean(raw: DataFrame) -> DataFrame:
    """Cast, validate and de-duplicate. rows that fail a rule are dropped."""
    return drop_duplicates(drop_bad_rows(cast_types(raw))).select(*COLS)
