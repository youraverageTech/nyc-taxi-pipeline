"""
Tests for EtlControl._upsert and the mark_* helper methods.

Mocking strategy:
- `_get_connection` is patched so no real Snowflake connection is made.
- Note: `_upsert` uses `with self._get_connection() as conn:` then plain
  `conn.cursor()` (NOT a `with` block on the cursor) — unlike
  `get_pending_months`, which wraps the cursor in its own `with` block too.
  The mock must match this exact usage pattern, otherwise assertions on
  call counts / call args will be wrong.
- We inspect `cur.execute.call_args` to verify the generated SQL and the
  parameters passed, without needing a real database.
"""

import pytest
from unittest.mock import MagicMock, ANY
from source.etl_control import EtlControl


@pytest.fixture
def etl():
    return EtlControl(connection_params={"account": "dummy", "user": "dummy", "password": "dummy"})


@pytest.fixture
def mock_cursor(mocker, etl):
    """
    Patches _get_connection so that `with self._get_connection() as conn:`
    yields a mock conn, and `conn.cursor()` (called directly, not via `with`)
    returns a mock cursor. Returns the mock cursor for assertions.
    """
    mock_cur = MagicMock()

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_conn.__enter__.return_value = mock_conn

    mocker.patch.object(EtlControl, "_get_connection", return_value=mock_conn)
    return mock_cur


class TestUpsertSQLGeneration:
    """Verify the SQL string and parameter list built by _upsert."""

    def test_includes_year_month_as_a_real_column(self, mock_cursor, etl):
        """year_month must appear as a literal column name in the SQL
        (not interpolated as a value), and as the first bound parameter."""
        etl._upsert("2024-01", status="pending")

        sql = mock_cursor.execute.call_args[0][0]
        params = mock_cursor.execute.call_args[0][1]

        assert "target.year_month = source.year_month" in sql
        assert params[0] == "2024-01"

    def test_columns_str_used_in_insert_clause(self, mock_cursor, etl):
        """INSERT clause must use the comma-joined column string, not a
        raw Python list repr (this was the bug in the earlier draft)."""
        etl._upsert("2024-01", status="pending", source_url="http://x")

        sql = mock_cursor.execute.call_args[0][0]

        assert "INSERT (year_month, status, source_url, updated_at)" in sql
        assert "[" not in sql  # guards against list literal leaking into SQL

    def test_update_clause_excludes_year_month(self, mock_cursor, etl):
        """The UPDATE SET clause should not attempt to update the
        matching key (year_month) against itself."""
        etl._upsert("2024-01", status="pending")

        sql = mock_cursor.execute.call_args[0][0]

        assert "target.year_month = source.year_month" not in sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "target.status = source.status" in sql

    def test_value_placeholder_count_matches_field_count(self, mock_cursor, etl):
        """Number of %s placeholders must equal number of values passed,
        including year_month and the auto-added updated_at."""
        etl._upsert("2024-01", status="pending", source_url="http://x")

        sql = mock_cursor.execute.call_args[0][0]
        params = mock_cursor.execute.call_args[0][1]

        placeholder_count = sql.count("%s")
        assert placeholder_count == len(params) == 4  # year_month, status, source_url, updated_at

    def test_updated_at_is_always_added(self, mock_cursor, etl):
        """Every upsert call should automatically stamp updated_at,
        even if the caller didn't pass it explicitly."""
        etl._upsert("2024-01", status="pending")

        sql = mock_cursor.execute.call_args[0][0]
        params = mock_cursor.execute.call_args[0][1]

        assert "updated_at" in sql
        assert len(params) == 3  # year_month, status, updated_at

    def test_merge_targets_correct_table(self, mock_cursor, etl):
        etl._upsert("2024-01", status="pending")

        sql = mock_cursor.execute.call_args[0][0]
        assert "MERGE INTO staging.etl_control" in sql

    def test_execute_called_exactly_once(self, mock_cursor, etl):
        etl._upsert("2024-01", status="pending")
        mock_cursor.execute.assert_called_once()


class TestMarkPending:

    def test_calls_upsert_with_pending_status_and_source_url(self, mocker, etl):
        mock_upsert = mocker.patch.object(etl, "_upsert")

        etl.mark_pending("2024-01", source_url="https://example.com/2024-01.parquet")

        mock_upsert.assert_called_once_with(
            "2024-01", status="pending", source_url="https://example.com/2024-01.parquet"
        )


class TestMarkLoaded:

    def test_calls_upsert_with_loaded_status_and_timestamp(self, mocker, etl):
        mock_upsert = mocker.patch.object(etl, "_upsert")

        etl.mark_loaded("2024-01")

        mock_upsert.assert_called_once()
        call_args = mock_upsert.call_args
        assert call_args[0][0] == "2024-01"
        assert call_args[1]["status"] == "loaded"
        assert "loaded_at" in call_args[1]


class TestMarkDownloaded:

    def test_calls_upsert_with_s3_path_and_timestamp(self, mocker, etl):
        mock_upsert = mocker.patch.object(etl, "_upsert")

        etl.mark_downloaded("2024-01", s3_raw_path="s3://bucket/raw/2024-01.parquet")

        mock_upsert.assert_called_once()
        call_args = mock_upsert.call_args
        assert call_args[0][0] == "2024-01"
        assert call_args[1]["s3_raw_path"] == "s3://bucket/raw/2024-01.parquet"
        assert call_args[1]["status"] == "downloaded"
        assert "downloaded_at" in call_args[1]
        

class TestMarkFailed:

    def test_calls_upsert_with_error_message(self, mocker, etl):
        mock_upsert = mocker.patch.object(etl, "_upsert")

        etl.mark_failed("2024-01", error_message="connection timeout")

        mock_upsert.assert_called_once_with(
            "2024-01", status="failed", error_message="connection timeout"
        )
        call_args = mock_upsert.call_args
        assert call_args[1]["status"] == "failed"



        
        