"""
Tests for EtlControl.get_pending_months

Mocking strategy:
- _get_connection() is mocked so it does not actually connect to Snowflake.
- conn and cur are mocked as context managers (because the original code uses
  `with self._get_connection() as conn:` and `with conn.cursor() as cur:`).
- cur.fetchall() is mocked to return fake data according to each test scenario.
"""

import pytest
from unittest.mock import MagicMock
from source.etl_control import EtlControl


@pytest.fixture
def etl():
    """EtlControl instance with dummy connection_params (not used because it's mocked)."""
    return EtlControl(connection_params={"account": "dummy", "user": "dummy", "password": "dummy"})


def _mock_connection(mocker, fetchall_return):
    """
    Helper to create a mocked connection + cursor that supports context managers
    (`with ... as conn:` and `with conn.cursor() as cur:`), and returns
    fetchall_return when cur.fetchall() is called.
    """
    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = fetchall_return
    # cursor as context manager: __enter__ returns itself
    mock_cur.__enter__.return_value = mock_cur

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_conn.__enter__.return_value = mock_conn

    mocker.patch.object(EtlControl, "_get_connection", return_value=mock_conn)
    return mock_cur, mock_conn


class TestGetPendingMonths:

    def test_all_months_not_loaded(self, mocker, etl):
        """If the etl_control table is empty (no 'loaded' data),
        all expected_months should be considered pending."""
        _mock_connection(mocker, fetchall_return=[])

        expected = ["2024-01", "2024-02", "2024-03"]
        result = etl.get_pending_months(expected)

        assert result == ["2024-01", "2024-02", "2024-03"]

    def test_some_months_already_loaded(self, mocker, etl):
        """Months with status 'loaded' should be excluded from the result,
        while the rest are considered pending."""
        _mock_connection(mocker, fetchall_return=[("2024-01",)])

        expected = ["2024-01", "2024-02", "2024-03"]
        result = etl.get_pending_months(expected)

        assert result == ["2024-02", "2024-03"]

    def test_all_months_loaded(self, mocker, etl):
        """If all expected_months are already loaded, the result should be empty."""
        _mock_connection(mocker, fetchall_return=[("2024-01",), ("2024-02",)])

        expected = ["2024-01", "2024-02"]
        result = etl.get_pending_months(expected)

        assert result == []

    def test_empty_expected_month(self, mocker, etl):
        """If expected_month is empty, the result should also be empty,
        regardless of the etl_control table content."""
        _mock_connection(mocker, fetchall_return=[("2024-01",)])

        result = etl.get_pending_months([])

        assert result == []

    def test_non_loaded_status_still_treated_as_pending(self, mocker, etl):
        """Months with status 'failed' or 'pending' will not appear in the query result
        (because the query filters status = 'loaded'), so they are automatically
        treated as pending by this function. This test verifies this behavior
        through the final output, not the query itself."""
        # Simulate: only 2024-01 is loaded; 2024-02 (failed) and 2024-03
        # (never exists in table) are not returned by query.
        _mock_connection(mocker, fetchall_return=[("2024-01",)])

        expected = ["2024-01", "2024-02", "2024-03"]
        result = etl.get_pending_months(expected)

        assert "2024-02" in result
        assert "2024-03" in result
        assert "2024-01" not in result

    def test_expected_month_order_is_preserved(self, mocker, etl):
        """The result order should follow expected_month order, not database order,
        because the output is generated using a list comprehension over expected_month."""
        _mock_connection(mocker, fetchall_return=[("2024-02",)])

        expected = ["2024-03", "2024-01", "2024-02"]
        result = etl.get_pending_months(expected)

        assert result == ["2024-03", "2024-01"]

    def test_query_is_executed_correctly(self, mocker, etl):
        """Ensures the SQL executed is correct and targets etl_control
        with status = 'loaded', and is called exactly once."""
        mock_cur, mock_conn = _mock_connection(mocker, fetchall_return=[])

        etl.get_pending_months(["2024-01"])

        mock_cur.execute.assert_called_once()
        executed_sql = mock_cur.execute.call_args[0][0]
        assert "etl_control" in executed_sql
        assert "status = 'loaded'" in executed_sql

    def test_connection_is_handled_via_context_manager(self, mocker, etl):
        """Ensures _get_connection is called exactly once per execution of
        get_pending_months (no excessive connections are opened)."""
        _mock_connection(mocker, fetchall_return=[])

        etl.get_pending_months(["2024-01"])

        etl._get_connection.assert_called_once()