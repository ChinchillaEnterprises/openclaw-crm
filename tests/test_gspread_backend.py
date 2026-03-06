from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from openclaw_crm.backends.gspread_backend import GspreadBackend


@pytest.fixture
def backend():
    with patch("gspread.service_account") as mock_sa:
        mock_sa.return_value = MagicMock()
        b = GspreadBackend("fake_creds.json")
    return b


@pytest.fixture
def mock_worksheet():
    ws = MagicMock()
    ws.get_all_values.return_value = [
        ["Client", "Stage", "Budget"],
        ["Acme", "lead", "5000"],
        ["Globex", "proposal", "12000"],
    ]
    return ws


class TestRead:
    def test_read_returns_values(self, backend, mock_worksheet):
        backend.gc.open_by_key.return_value.worksheet.return_value = mock_worksheet
        result = backend.read("sheet_id", "'Pipeline'!A:U")

        assert result.success is True
        assert len(result.data["values"]) == 3
        assert result.data["values"][0] == ["Client", "Stage", "Budget"]

    def test_read_empty(self, backend):
        ws = MagicMock()
        ws.get_all_values.return_value = []
        backend.gc.open_by_key.return_value.worksheet.return_value = ws

        result = backend.read("sheet_id", "'Pipeline'!A:U")
        assert result.success is True
        assert result.data["values"] == []

    def test_read_error(self, backend):
        backend.gc.open_by_key.side_effect = Exception("Auth failed")
        result = backend.read("sheet_id", "'Pipeline'!A:U")

        assert result.success is False
        assert "Auth failed" in result.error


class TestAppend:
    def test_append_rows(self, backend, mock_worksheet):
        backend.gc.open_by_key.return_value.worksheet.return_value = mock_worksheet
        result = backend.append("sheet_id", "'Pipeline'!A:U", [["NewCo", "lead", "8000"]])

        assert result.success is True
        mock_worksheet.append_row.assert_called_once_with(["NewCo", "lead", "8000"], value_input_option="USER_ENTERED")

    def test_append_error(self, backend):
        backend.gc.open_by_key.side_effect = Exception("fail")
        result = backend.append("sheet_id", "'Pipeline'!A:U", [["x"]])

        assert result.success is False


class TestUpdate:
    def test_update_cells(self, backend, mock_worksheet):
        backend.gc.open_by_key.return_value.worksheet.return_value = mock_worksheet
        result = backend.update("sheet_id", "'Pipeline'!A2:U2", [["Acme", "won", "5500"]])

        assert result.success is True
        mock_worksheet.update.assert_called_once_with("A2:U2", [["Acme", "won", "5500"]], value_input_option="USER_ENTERED")

    def test_update_error(self, backend):
        backend.gc.open_by_key.side_effect = Exception("fail")
        result = backend.update("sheet_id", "'Pipeline'!A2:U2", [["x"]])

        assert result.success is False
