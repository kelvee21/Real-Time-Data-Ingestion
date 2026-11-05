"""Fake csv rows so the tests never need the generator folder or postgres.

same shape as one line of a data/incoming/events_*.csv file: every value is
text, the way spark reads it with RAW_SCHEMA.
"""
from pyspark.sql import DataFrame, Row, SparkSession

from src import cleaning

RawRow = dict[str, str | None]


def raw_row(**changes: str | None) -> RawRow:
    r: RawRow = {
        "event_id": "e-1", "event_time": "2026-11-04 09:42:17.123", "user_id": "42",
        "session_id": "abc123def456", "event_type": "purchase", "product_id": "1500",
        "category": "books", "price": "19.99", "quantity": "2",
    }
    r.update(changes)
    return r


def raw_df(spark: SparkSession, *rows: RawRow) -> DataFrame:
    """Rows -> DataFrame with the same all-string schema the stream uses."""
    tuples = [tuple(r[c] for c in cleaning.COLS) for r in rows]
    return spark.createDataFrame(tuples, cleaning.RAW_SCHEMA)


def cleaned(spark: SparkSession, *rows: RawRow) -> DataFrame:
    return cleaning.clean(raw_df(spark, *rows))


def first(df: DataFrame) -> Row:
    """df.first() is typed Row | None. every test table here has rows, so
    this saves an "is not None" check in each test."""
    row = df.first()
    assert row is not None, "expected at least one row"
    return row
