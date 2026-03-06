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

    def test_init_from_env(self, monkeypatch):
        monkeypatch.setenv("AIRTABLE_API_TOKEN", "env_tok")
        monkeypatch.setenv("AIRTABLE_BASE_ID", "appENV")
        backend = AirtableBackend()
        assert backend.api_token == "env_tok"
        assert backend.base_id == "appENV"

    def test_init_missing_token(self):
        with pytest.raises(ValueError, match="AIRTABLE_API_TOKEN"):
            AirtableBackend(api_token="", base_id="app123")

    def test_init_missing_base_id(self):
        with pytest.raises(ValueError, match="AIRTABLE_BASE_ID"):
            AirtableBackend(api_token="tok", base_id="")


class TestRead:
    def test_read_returns_values(self, backend, mock_records):
        mock_table = MagicMock()
        mock_table.all.return_value = mock_records

        with patch.object(backend, "_get_table", return_value=mock_table):
            result = backend.read("ignored", "'Pipeline'!A:U")

        assert result.success is True
        values = result.data["values"]
        assert values[0] == ["Client", "Stage", "Budget"]
        assert values[1] == ["Acme", "lead", "5000"]
        assert values[2] == ["Globex", "proposal", "12000"]

    def test_read_empty_table(self, backend):
        mock_table = MagicMock()
        mock_table.all.return_value = []

        with patch.object(backend, "_get_table", return_value=mock_table):
            result = backend.read("ignored", "'Pipeline'!A:U")

        assert result.success is True
        assert result.data["values"] == []

    def test_read_error(self, backend):
        with patch.object(backend, "_get_table", side_effect=Exception("API error")):
            result = backend.read("ignored", "'Pipeline'!A:U")

        assert result.success is False
        assert "API error" in result.error


class TestAppend:
    def test_append_rows(self, backend, mock_records):
        mock_table = MagicMock()
        mock_table.all.return_value = mock_records

        with patch.object(backend, "_get_table", return_value=mock_table):
            result = backend.append("ignored", "'Pipeline'!A:U", [["NewCo", "lead", "8000"]])

        assert result.success is True
        mock_table.create.assert_called_once_with({"Client": "NewCo", "Stage": "lead", "Budget": "8000"})

    def test_append_to_empty_table_uses_first_row_as_headers(self, backend):
        mock_table = MagicMock()
        mock_table.all.return_value = []

        with patch.object(backend, "_get_table", return_value=mock_table):
            result = backend.append("ignored", "'Pipeline'!A:U", [["Name", "Status"], ["Acme", "active"]])

        assert result.success is True
        mock_table.create.assert_called_once_with({"Name": "Acme", "Status": "active"})

    def test_append_error(self, backend):
        with patch.object(backend, "_get_table", side_effect=Exception("Network error")):
            result = backend.append("ignored", "'Pipeline'!A:U", [["x"]])

        assert result.success is False


class TestUpdate:
    def test_update_matching_record(self, backend, mock_records):
        mock_table = MagicMock()
        mock_table.all.return_value = mock_records

        with patch.object(backend, "_get_table", return_value=mock_table):
            result = backend.update("ignored", "'Pipeline'!A:U", [["Acme", "won", "5500"]])

        assert result.success is True
        mock_table.update.assert_called_once_with("rec1", {"Stage": "won", "Budget": "5500"})

    def test_update_no_match(self, backend, mock_records):
        mock_table = MagicMock()
        mock_table.all.return_value = mock_records

        with patch.object(backend, "_get_table", return_value=mock_table):
            result = backend.update("ignored", "'Pipeline'!A:U", [["NonExistent", "won"]])

        assert result.success is True
        assert result.data["updatedRows"] == 0
        mock_table.update.assert_not_called()

    def test_update_empty_table(self, backend):
        mock_table = MagicMock()
        mock_table.all.return_value = []

        with patch.object(backend, "_get_table", return_value=mock_table):
            result = backend.update("ignored", "'Pipeline'!A:U", [["Acme", "won"]])

        assert result.success is False

    def test_update_error(self, backend):
        with patch.object(backend, "_get_table", side_effect=Exception("fail")):
            result = backend.update("ignored", "'Pipeline'!A:U", [["x"]])

        assert result.success is False
