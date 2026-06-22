import pytest
from source.extract import get_available_months
from unittest.mock import MagicMock



@pytest.fixture
def mock_etl_control_class(mocker):
    mock_instance = MagicMock()
    mock_class = mocker.patch("source.extract.EtlControl", return_value=mock_instance)

    return mock_instance


class TestGetAvailableMonths:
 

    def test_loaded_months_are_skipped_from_head_check(self, mocker, mock_etl_control_class):
        mock_etl_control_class.get_loaded_months.return_value = ["2024-01", "2024-02"]
        mock_head = mocker.patch("source.extract.requests.head")
        mock_head.return_value.status_code = 200
 
        get_available_months(2024, "yellow")
 
        checked_urls = [call.args[0] for call in mock_head.call_args_list]

        assert not any("2024-01" in url for url in checked_urls)
        assert not any("2024-02" in url for url in checked_urls)
        assert len(checked_urls) == 10

    def test_loaded_all_months(self, mocker, mock_etl_control_class):
        mock_etl_control_class.get_loaded_months.return_value = []
        mock_head = mocker.patch("source.extract.requests.head")
        mock_head.return_value.status_code = 200
 
        get_available_months(2024, "yellow")
 
        checked_urls = [call.args[0] for call in mock_head.call_args_list]

        assert len(checked_urls) == 12

    
    def test_returns_only_months_with_200_status(self, mocker, mock_etl_control_class):
        mock_etl_control_class.get_loaded_months.return_value = []
        mock_head = mocker.patch("source.extract.requests.head")

        responses = []
        for month in range(1, 13):
            resp = MagicMock()
            resp.status_code = 200 if month <= 3 else 404
            responses.append(resp)
        mock_head.side_effect = responses

        result = get_available_months(2024, "yellow")

        assert result == ["2024-01", "2024-02", "2024-03"]
    

    def test_network_error_excludes_month_without_crashing(self, mocker, mock_etl_control_class, capsys):
        mock_etl_control_class.get_loaded_months.return_value = []
        mock_head = mocker.patch("source.extract.requests.head")
        mock_head.side_effect = ConnectionError("network unreachable")
 
        result = get_available_months(2024, "yellow")
 
        assert result == []
        assert "Error checking" in capsys.readouterr().out
 
 
    def test_timeout_passed_to_head_request(self, mocker, mock_etl_control_class):
        mock_etl_control_class.get_loaded_months.return_value = []
        mock_head = mocker.patch("source.extract.requests.head")
        mock_head.return_value.status_code = 404
 
        get_available_months(2024, "yellow")
 
        for call in mock_head.call_args_list:
            assert call.kwargs.get("timeout") == 10


    

