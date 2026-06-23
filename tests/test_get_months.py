"""
Tests for EtlControl.get_months_needing_download and get_months_needing_load.

These two functions replace the original single get_pending_months function,
splitting "what needs downloading" from "what needs loading" so a load
failure doesn't trigger a redundant re-download.

Mocking strategy: same pattern as before — _get_connection is patched, and
the mock cursor/connection support the `with ... as ...:` usage seen in
the source code.
"""

import pytest
from unittest.mock import MagicMock
from source.etl_control import EtlControl


@pytest.fixture
def etl():
    return EtlControl(connection_provider=lambda: MagicMock())


def _mock_connection(mocker, fetchall_return):
    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = fetchall_return
    mock_cur.__enter__.return_value = mock_cur
 
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_conn.__enter__.return_value = mock_conn
 
    mocker.patch.object(EtlControl, "_get_connection", return_value=mock_conn)
    return mock_cur, mock_conn


class TestGetMonthsNeedingDownload:

    def test_no_months_processed_yet(self, mocker, etl):
        """If etl_control is empty, every expected month needs downloading."""
        _mock_connection(mocker, fetchall_return=[])

        result = etl.get_months_needing_download(["2024-01", "2024-02"])

        assert result == ["2024-01", "2024-02"]


    def test_downloaded_month_is_excluded(self, mocker, etl):
        """A month already at 'downloaded' status should NOT be re-downloaded,
        even though it hasn't reached 'loaded' yet."""
        _mock_connection(mocker, fetchall_return=[("2024-01",)])

        result = etl.get_months_needing_download(["2024-01", "2024-02"])

        assert result == ["2024-02"]


    def test_loaded_month_is_excluded(self, mocker, etl):
        """A month already 'loaded' should also not be re-downloaded."""
        _mock_connection(mocker, fetchall_return=[("2024-01",)])

        result = etl.get_months_needing_download(["2024-01", "2024-02"])

        assert result == ["2024-02"]


    def test_failed_month_still_needs_download(self, mocker, etl):
        """A month with status 'failed' (failed before a file ever reached S3)
        is NOT in the downloaded/loaded set, so it correctly needs downloading again."""
        # Simulate: only 2024-01 is downloaded/loaded; 2024-02 ('failed') is
        # not returned by the query, so it falls through to needing download.
        _mock_connection(mocker, fetchall_return=[("2024-01",)])

        result = etl.get_months_needing_download(["2024-01", "2024-02"])

        assert "2024-02" in result


    def test_query_filters_on_downloaded_and_loaded(self, mocker, etl):
        mock_cur, _ = _mock_connection(mocker, fetchall_return=[])

        etl.get_months_needing_download(["2024-01"])

        sql = mock_cur.execute.call_args[0][0]
        assert "status IN ('downloaded', 'loaded')" in sql


    def test_empty_expected_months_returns_empty(self, mocker, etl):
        _mock_connection(mocker, fetchall_return=[("2024-01",)])

        result = etl.get_months_needing_download([])

        assert result == []


class TestGetMonthsNeedingLoad:

    def test_no_downloaded_months_returns_empty(self, mocker, etl):
        """If nothing is at 'downloaded' status, nothing needs loading."""
        _mock_connection(mocker, fetchall_return=[])

        result = etl.get_months_needing_load()

        assert result == []


    def test_returns_months_with_downloaded_status(self, mocker, etl):
        """Months sitting at 'downloaded' (file in S3, not yet in Snowflake)
        should be returned for the load step."""
        _mock_connection(mocker, fetchall_return=[
            ("2024-01", "raw/yellow/2024/yellow_tripdata_2024-01.parquet"),
            ("2024-03", "raw/yellow/2024/yellow_tripdata_2024-03.parquet"),])

        result = etl.get_months_needing_load()

        assert result == [
            ("2024-01", "raw/yellow/2024/yellow_tripdata_2024-01.parquet"),
            ("2024-03", "raw/yellow/2024/yellow_tripdata_2024-03.parquet"),]


    def test_does_not_take_expected_months_argument(self, mocker, etl):
        """Unlike get_months_needing_download, this function doesn't need an
        expected_month list — it only cares about what's already in S3
        according to etl_control, not what 'should' exist overall."""
        _mock_connection(mocker, fetchall_return=[("2024-01", "raw/yellow/2024/file.parquet")])

        # Should work with no arguments at all.
        result = etl.get_months_needing_load()

        assert result == [("2024-01", "raw/yellow/2024/file.parquet")]


    def test_query_filters_on_downloaded_only(self, mocker, etl):
        mock_cur, _ = _mock_connection(mocker, fetchall_return=[])

        etl.get_months_needing_load()

        sql = mock_cur.execute.call_args[0][0]
        assert "status = 'downloaded'" in sql
        assert "loaded'" not in sql.replace("status = 'downloaded'", "")
    

    def test_query_selects_s3_raw_path_column(self, mocker, etl):
        """The SQL must select s3_raw_path alongside year_month, since the
        load step needs it to construct the COPY INTO source."""
        mock_cur, _ = _mock_connection(mocker, fetchall_return=[])
 
        etl.get_months_needing_load()
 
        sql = mock_cur.execute.call_args[0][0]
        assert "s3_raw_path" in sql


class TestDownloadAndLoadAreComplementary:
    """Integration-style tests (still mocked) verifying the two functions
    behave consistently with each other for the same underlying data,
    which matters since they're meant to be used together in sequence."""


    def test_month_failed_at_load_is_not_redownloaded_but_is_returned_for_load(self, mocker, etl):
        """
        Scenario: 2024-03 was downloaded successfully (file in S3) but the
        Snowflake load failed, so it's stuck at 'downloaded'.
        - get_months_needing_download should NOT include it (file already exists).
        - get_months_needing_load SHOULD include it (still needs loading).
        """
        # First call: download check — 2024-03 already downloaded.
        _mock_connection(mocker, fetchall_return=[("2024-03",)])
        download_result = etl.get_months_needing_download(["2024-03"])
        assert download_result == []  # correctly skips re-download

        # Second call: load check — 2024-03 is still pending load.
        _mock_connection(mocker, fetchall_return=[("2024-03", "raw/yellow/2024/yellow_tripdata_2024-03.parquet")])
        load_result = etl.get_months_needing_load()
        assert load_result == [("2024-03", "raw/yellow/2024/yellow_tripdata_2024-03.parquet")]  # correctly flagged for loading