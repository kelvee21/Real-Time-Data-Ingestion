from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from src import cleaning
from tests.helpers import cleaned, first, raw_df, raw_row


def test_good_row_survives_with_real_types(spark: SparkSession) -> None:
    df = cleaned(spark, raw_row())
    r = first(df)
    assert isinstance(r["event_time"], datetime)
    # collect() turns a timestamp into my laptop's local time, so check the
    # value in spark, where the session time zone is utc like the generator
    shown = first(df.selectExpr("date_format(event_time, 'yyyy-MM-dd HH:mm:ss.SSS') AS t"))
    assert shown["t"] == "2026-11-04 09:42:17.123"
    assert r["user_id"] == 42
    assert r["product_id"] == 1500
    assert r["price"] == Decimal("19.99")
    assert r["quantity"] == 2


def test_output_columns_match_the_table(spark: SparkSession) -> None:
    assert cleaned(spark, raw_row()).columns == cleaning.COLS


def test_event_type_and_category_trimmed_and_lowercased(spark: SparkSession) -> None:
    r = first(cleaned(spark, raw_row(event_type="  PURCHASE ", category=" Books")))
    assert r["event_type"] == "purchase"
    assert r["category"] == "books"


# every bad value the generator can write, plus a few more
@pytest.mark.parametrize("field,value", [
    ("price", ""), ("price", "unknown"), ("price", "-5"),
    ("user_id", "-5"), ("user_id", "unknown"), ("user_id", "NaN"), ("user_id", None),
    ("event_type", "unknown"), ("event_type", ""),
    ("event_time", "unknown"), ("event_time", "NaN"), ("event_time", ""),
    ("product_id", "0"), ("event_id", None), ("quantity", "-1"),
])
def test_bad_value_drops_the_row(spark: SparkSession, field: str, value: str | None) -> None:
    assert cleaned(spark, raw_row(**{field: value})).count() == 0


def test_cast_keeps_the_row_but_nulls_the_bad_value(spark: SparkSession) -> None:
    # cast_types alone must not throw on junk, that is the point of try_cast
    r = first(cleaning.cast_types(raw_df(spark, raw_row(price="unknown"))))
    assert r["price"] is None


def test_missing_quantity_is_allowed(spark: SparkSession) -> None:
    assert cleaned(spark, raw_row(quantity=None)).count() == 1


def test_duplicate_event_dropped(spark: SparkSession) -> None:
    df = cleaned(spark, raw_row(), raw_row(), raw_row(event_id="e-2"))
    assert sorted(r["event_id"] for r in df.collect()) == ["e-1", "e-2"]


def test_mixed_batch_keeps_only_good_rows(spark: SparkSession) -> None:
    rows = [raw_row(event_id=f"ok-{i}") for i in range(5)]
    rows += [raw_row(event_id="bad-1", price="NaN"), raw_row(event_id="bad-2", event_type="click")]
    assert cleaned(spark, *rows).count() == 5
