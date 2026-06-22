"""
Tests for loading.load_to_snowflake.

Mocking strategy: connection_provider is a plain callable, so we just pass
a lambda/MagicMock factory directly — no need to patch anything at module
level, since the function receives its dependency explicitly (dependency
injection), unlike extract_v2 which instantiates EtlControl internally.

We patch EtlControl at the point load_to_snowflake.py looks it up, since
that's still constructed inside the function.
"""

import pytest
from unittest.mock import MagicMock
import source.loading as lts



@pytest.fixture
def mock_etl_control_class(mocker):
    mock_instance = MagicMock()
    mocker.patch("source.loading.EtlControl", return_value=mock_instance)
    return mock_instance


@pytest.fixture
def mock_connection_provider():
    """
    Returns (provider, conn, cur) where provider is a callable that, when
    called, yields a mock connection supporting `with ... as conn:` and
    `with conn.cursor() as cur:`.
    """
    mock_cur = MagicMock()
    mock_cur.__enter__.return_value = mock_cur

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_conn.__enter__.return_value = mock_conn

    provider = MagicMock(return_value=mock_conn)
    return provider, mock_conn, mock_cur


class TestLoadToSnowflake:

    def test_no_months_needing_load_does_nothing(self, mock_etl_control_class, mock_connection_provider):
        provider, conn, cur = mock_connection_provider
        mock_etl_control_class.get_months_needing_load.return_value = []

        lts.load_to_snowflake(provider)

        cur.execute.assert_not_called()
        mock_etl_control_class.mark_loaded.assert_not_called()
        mock_etl_control_class.mark_failed.assert_not_called()

    def test_successful_copy_into_marks_loaded(self, mock_etl_control_class, mock_connection_provider):
        provider, conn, cur = mock_connection_provider
        mock_etl_control_class.get_months_needing_load.return_value = [
            ("2024-01", "raw/yellow/2024/yellow_tripdata_2024-01.parquet")
        ]

        lts.load_to_snowflake(provider)

        cur.execute.assert_called_once()
        executed_sql = cur.execute.call_args[0][0]
        assert "COPY INTO" in executed_sql
        assert "raw/yellow/2024/yellow_tripdata_2024-01.parquet" in executed_sql

        mock_etl_control_class.mark_loaded.assert_called_once_with("2024-01")
        mock_etl_control_class.mark_failed.assert_not_called()

    def test_copy_into_uses_correct_s3_path_per_month(self, mock_etl_control_class, mock_connection_provider):
        """Each month's COPY INTO must reference its own s3_raw_path, not
        a shared/incorrect one — important since paths differ per month."""
        provider, conn, cur = mock_connection_provider
        mock_etl_control_class.get_months_needing_load.return_value = [
            ("2024-01", "raw/yellow/2024/yellow_tripdata_2024-01.parquet"),
            ("2024-02", "raw/yellow/2024/yellow_tripdata_2024-02.parquet"),
        ]

        lts.load_to_snowflake(provider)

        executed_sqls = [call.args[0] for call in cur.execute.call_args_list]
        assert any("2024-01" in sql for sql in executed_sqls)
        assert any("2024-02" in sql for sql in executed_sqls)

    def test_copy_into_failure_marks_failed_not_loaded(self, mock_etl_control_class, mock_connection_provider):
        provider, conn, cur = mock_connection_provider
        mock_etl_control_class.get_months_needing_load.return_value = [
            ("2024-03", "raw/yellow/2024/yellow_tripdata_2024-03.parquet")
        ]
        cur.execute.side_effect = Exception("COPY INTO failed: file format mismatch")

        lts.load_to_snowflake(provider)

        mock_etl_control_class.mark_loaded.assert_not_called()
        mock_etl_control_class.mark_failed.assert_called_once_with(
            year_month="2024-03", error_message="COPY INTO failed: file format mismatch"
        )

    def test_one_failure_does_not_block_subsequent_months(self, mock_etl_control_class, mock_connection_provider):
        provider, conn, cur = mock_connection_provider
        mock_etl_control_class.get_months_needing_load.return_value = [
            ("2024-01", "raw/yellow/2024/yellow_tripdata_2024-01.parquet"),
            ("2024-02", "raw/yellow/2024/yellow_tripdata_2024-02.parquet"),
        ]
        # First COPY INTO call fails, second succeeds.
        cur.execute.side_effect = [Exception("network blip"), None]

        lts.load_to_snowflake(provider)

        assert mock_etl_control_class.mark_failed.call_count == 1
        assert mock_etl_control_class.mark_loaded.call_count == 1
        mock_etl_control_class.mark_failed.assert_called_once_with(
            year_month="2024-01", error_message="network blip"
        )
        mock_etl_control_class.mark_loaded.assert_called_once_with("2024-02")

    def test_uses_custom_stage_and_table_when_provided(self, mock_etl_control_class, mock_connection_provider):
        provider, conn, cur = mock_connection_provider
        mock_etl_control_class.get_months_needing_load.return_value = [
            ("2024-01", "raw/yellow/2024/yellow_tripdata_2024-01.parquet")
        ]

        lts.load_to_snowflake(provider, stage_name="custom.my_stage", target_table="custom.my_table")

        executed_sql = cur.execute.call_args[0][0]
        assert "custom.my_stage" in executed_sql
        assert "custom.my_table" in executed_sql

    def test_default_stage_and_table_used_when_not_specified(self, mock_etl_control_class, mock_connection_provider):
        provider, conn, cur = mock_connection_provider
        mock_etl_control_class.get_months_needing_load.return_value = [
            ("2024-01", "raw/yellow/2024/yellow_tripdata_2024-01.parquet")
        ]

        lts.load_to_snowflake(provider)

        executed_sql = cur.execute.call_args[0][0]
        assert "nyc_taxi_stage" in executed_sql
        assert "staging.raw_nyc_taxi_tripdata" in executed_sql