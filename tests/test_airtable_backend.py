from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from openclaw_crm.backends.airtable_backend import AirtableBackend


@pytest.fixture
def backend():
    return AirtableBackend(api_token="fake_token", base_id="appFAKEBASEID")


@pytest.fixture
def mock_records():
    return [
        {"id": "rec1", "fields": {"Client": "Acme", "Stage": "lead", "Budget": "5000"}},
        {"id": "rec2", "fields": {"Client": "Globex", "Stage": "proposal", "Budget": "12000"}},
    ]


class TestAirtableBackendInit:
    def test_init_with_params(self):
        backend = AirtableBackend(api_token="tok", base_id="app123")
        assert backend.api_token == "tok"
        assert backend.base_id == "app123"

    def test_init_from_env(self):
        with patch.dict("os.environ", {"AIRTABLE_API_TOKEN": "env_token", "AIRTABLE_BASE_ID": "env_base"}):
            backend = AirtableBackend()
            assert backend.api_token == "env_token"
            assert backend.base_id == "env_base"

    def test_init_missing_token(self):
        with pytest.raises(ValueError, match="AIRTABLE_API_TOKEN"):
            AirtableBackend(base_id="app123")

    def test_init_missing_base_id(self):
        with pytest.raises(ValueError, match="AIRTABLE_BASE_ID"):
            AirtableBackend(api_token="tok")


class TestAirtableBackendRead:
    @patch("pyairtable.Api")
    def test_read_success(self, mock_api, backend, mock_records):
        mock_table = MagicMock()
        mock_table.all.return_value = mock_records
        mock_api.return_value.table.return_value = mock_table

        result = backend.read("ignored", "Pipeline")

        assert result.success is True
        assert "values" in result.data

    @patch("pyairtable.Api")
    def test_read_empty(self, mock_api, backend):
        mock_table = MagicMock()
        mock_table.all.return_value = []
        mock_api.return_value.table.return_value = mock_table

        result = backend.read("ignored", "Pipeline")

        assert result.success is True
        assert result.data["values"] == []

    @patch("pyairtable.Api")
    def test_read_error(self, mock_api, backend):
        mock_table = MagicMock()
        mock_table.all.side_effect = Exception("API Error")
        mock_api.return_value.table.return_value = mock_table

        result = backend.read("ignored", "Pipeline")

        assert result.success is False
        assert "API Error" in result.error


class TestAirtableBackendAppend:
    @patch("pyairtable.Api")
    def test_append_success(self, mock_api, backend, mock_records):
        mock_table = MagicMock()
        mock_table.all.return_value = mock_records
        mock_api.return_value.table.return_value = mock_table

        result = backend.append("ignored", "Pipeline", [["Client", "Stage", "Budget"], ["Test", "lead", "1000"]])

        assert result.success is True
        assert result.data["updatedRows"] == 2  # 2 data rows (excluding header)

    @patch("pyairtable.Api")
    def test_append_error(self, mock_api, backend):
        mock_table = MagicMock()
        mock_table.all.side_effect = Exception("API Error")
        mock_api.return_value.table.return_value = mock_table

        result = backend.append("ignored", "Pipeline", [])

        assert result.success is False


class TestAirtableBackendUpdate:
    @patch("pyairtable.Api")
    def test_update_success(self, mock_api, backend, mock_records):
        mock_table = MagicMock()
        mock_table.all.return_value = mock_records
        mock_api.return_value.table.return_value = mock_table

        result = backend.update("ignored", "Pipeline", [["Acme", "proposal", "8000"]])

        assert result.success is True

    @patch("pyairtable.Api")
    def test_update_no_records(self, mock_api, backend):
        mock_table = MagicMock()
        mock_table.all.return_value = []
        mock_api.return_value.table.return_value = mock_table

        result = backend.update("ignored", "Pipeline", [])

        assert result.success is False
        assert "No existing records" in result.error
