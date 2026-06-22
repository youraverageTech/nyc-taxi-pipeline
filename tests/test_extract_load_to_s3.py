import pytest
import source.extract
import io
from unittest.mock import MagicMock


@pytest.fixture
def mock_etl_control_class(mocker):
    mock_instance = MagicMock()
    mock_class = mocker.patch("source.extract.EtlControl", return_value=mock_instance)

    return mock_instance


@pytest.fixture
def mock_s3_client(mocker):
    mock_client = MagicMock()
    mocker.patch("source.extract.boto3.client", return_value=mock_client)
    return mock_client


class TestExtractAndLoadToS3:
 
    def test_successful_download_is_uploaded_and_marked_downloaded(
        self, mocker, mock_etl_control_class, mock_s3_client
    ):
        mock_etl_control_class.get_months_needing_download.return_value = ["2024-01"]
        mock_get = mocker.patch("source.extract.requests.get")
        mock_get.return_value.status_code = 200
        mock_get.return_value.iter_content.return_value = [b"chunk1", b"chunk2"]
 
        source.extract.extract_and_load_to_s3(["2024-01"], "yellow", 2024)
 
        mock_s3_client.upload_fileobj.assert_called_once()
        call_kwargs = mock_s3_client.upload_fileobj.call_args
        assert call_kwargs.kwargs["Bucket"] == source.extract.BUCKET_NAME
        assert call_kwargs.kwargs["Key"] == "raw/yellow/2024/yellow_tripdata_2024-01.parquet"
 
        mock_etl_control_class.mark_downloaded.assert_called_once_with(
            year_month="2024-01", s3_raw_path="raw/yellow/2024/yellow_tripdata_2024-01.parquet"
        )
        mock_etl_control_class.mark_failed.assert_not_called()
 
    def test_uploaded_buffer_contains_concatenated_chunks(
        self, mocker, mock_etl_control_class, mock_s3_client
    ):
        """Verifies file_buffer actually contains the downloaded bytes,
        and that the stream position is reset to 0 before upload."""
        mock_etl_control_class.get_months_needing_download.return_value = ["2024-01"]
        mock_get = mocker.patch("source.extract.requests.get")
        mock_get.return_value.status_code = 200
        mock_get.return_value.iter_content.return_value = [b"hello-", b"world"]
 
        source.extract.extract_and_load_to_s3(["2024-01"], "yellow", 2024)
 
        uploaded_buffer = mock_s3_client.upload_fileobj.call_args.args[0]
        assert isinstance(uploaded_buffer, io.BytesIO)
        assert uploaded_buffer.read() == b"hello-world"
 
    def test_non_200_status_marks_failed_and_skips_upload(
        self, mocker, mock_etl_control_class, mock_s3_client
    ):
        mock_etl_control_class.get_months_needing_download.return_value = ["2024-02"]
        mock_get = mocker.patch("source.extract.requests.get")
        mock_get.return_value.status_code = 404
 
        source.extract.extract_and_load_to_s3(["2024-02"], "yellow", 2024)
 
        mock_s3_client.upload_fileobj.assert_not_called()
        mock_etl_control_class.mark_downloaded.assert_not_called()
        mock_etl_control_class.mark_failed.assert_called_once()
        error_message = mock_etl_control_class.mark_failed.call_args.kwargs["error_message"]
        assert "404" in error_message
 
    def test_network_exception_during_get_marks_failed(
        self, mocker, mock_etl_control_class, mock_s3_client
    ):
        mock_etl_control_class.get_months_needing_download.return_value = ["2024-03"]
        mocker.patch("source.extract.requests.get", side_effect=ConnectionError("timeout"))
 
        source.extract.extract_and_load_to_s3(["2024-03"], "yellow", 2024)
 
        mock_etl_control_class.mark_failed.assert_called_once_with(
            year_month="2024-03", error_message="timeout"
        )
 
    def test_s3_upload_failure_marks_failed(
        self, mocker, mock_etl_control_class, mock_s3_client
    ):
        """If S3 upload itself raises (e.g. credentials error, network
        issue), the month should be marked failed, not silently lost,
        and mark_downloaded should never be called."""
        mock_etl_control_class.get_months_needing_download.return_value = ["2024-04"]
        mock_get = mocker.patch("source.extract.requests.get")
        mock_get.return_value.status_code = 200
        mock_get.return_value.iter_content.return_value = [b"data"]
        mock_s3_client.upload_fileobj.side_effect = Exception("S3 access denied")
 
        source.extract.extract_and_load_to_s3(["2024-04"], "yellow", 2024)
 
        mock_etl_control_class.mark_downloaded.assert_not_called()
        mock_etl_control_class.mark_failed.assert_called_once_with(
            year_month="2024-04", error_message="S3 access denied"
        )
 
    def test_one_failed_month_does_not_block_the_next(
        self, mocker, mock_etl_control_class, mock_s3_client
    ):
        """A failure on one month should not prevent subsequent months
        in the same call from being processed."""
        mock_etl_control_class.get_months_needing_download.return_value = ["2024-01", "2024-02"]
        mock_get = mocker.patch("source.extract.requests.get")
 
        resp_fail = MagicMock()
        resp_fail.status_code = 404
        resp_ok = MagicMock()
        resp_ok.status_code = 200
        resp_ok.iter_content.return_value = [b"data"]
        mock_get.side_effect = [resp_fail, resp_ok]
 
        source.extract.extract_and_load_to_s3(["2024-01", "2024-02"], "yellow", 2024)
 
        assert mock_etl_control_class.mark_failed.call_count == 1
        assert mock_etl_control_class.mark_downloaded.call_count == 1
        mock_etl_control_class.mark_downloaded.assert_called_once_with(
            year_month="2024-02", s3_raw_path="raw/yellow/2024/yellow_tripdata_2024-02.parquet"
        )
 
    def test_empty_months_needing_download_does_nothing(
        self, mocker, mock_etl_control_class, mock_s3_client
    ):
        mock_etl_control_class.get_months_needing_download.return_value = []
        mock_get = mocker.patch("source.extract.requests.get")
 
        source.extract.extract_and_load_to_s3([], "yellow", 2024)
 
        mock_get.assert_not_called()
        mock_s3_client.upload_fileobj.assert_not_called()
 
    def test_get_request_uses_stream_and_timeout(
        self, mocker, mock_etl_control_class, mock_s3_client
    ):
        mock_etl_control_class.get_months_needing_download.return_value = ["2024-01"]
        mock_get = mocker.patch("source.extract.requests.get")
        mock_get.return_value.status_code = 200
        mock_get.return_value.iter_content.return_value = [b"data"]
 
        source.extract.extract_and_load_to_s3(["2024-01"], "yellow", 2024)
 
        call_kwargs = mock_get.call_args.kwargs
        assert call_kwargs.get("stream") is True
        assert call_kwargs.get("timeout") == 30
