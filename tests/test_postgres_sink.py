"""The merge sql and the settings. writing to the real db is covered by the
end to end runs in reports/test_cases.md, not here: spark_app may not delete,
so a test row would stay in events for good."""
from src import config
from src.cleaning import COLS
from src.postgres_sink import MERGE_SQL


def test_merge_is_idempotent_by_primary_key() -> None:
    sql = " ".join(MERGE_SQL.split())
    assert "INSERT INTO events (" in sql
    assert "FROM events_staging" in sql
    assert sql.endswith("ON CONFLICT (event_id) DO NOTHING")


def test_merge_lists_columns_in_table_order() -> None:
    assert MERGE_SQL.count(", ".join(COLS)) == 2   # insert list and select list are the same


def test_jdbc_url_points_at_the_configured_db() -> None:
    pg = config.PG
    assert config.JDBC_URL == f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['dbname']}"


def test_relative_folders_are_under_the_lab() -> None:
    assert config.lab_path("data/x") == config.ROOT / "data" / "x"
    assert config.INPUT_DIR.is_absolute()
