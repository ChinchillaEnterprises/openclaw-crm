from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from openclaw_crm.backends.gspread_backend import GspreadBackend
from openclaw_crm.sheets import SheetResult


@pytest.fixture
def mock_gspread_config():
    """Mock gspread configuration."""
    return {
        "spreadsheet_id": "test_spreadsheet_id",
        "credentials_path": "/path/to/credentials.json",
    }


@pytest.fixture
def mock_worksheet():
    """Mock gspread worksheet."""
    return MagicMock()


@pytest.fixture
def mock_sheet():
    """Mock gspread spreadsheet."""
    return MagicMock()


@pytest.fixture
def mock_client():
    """Mock gspread client."""
    return MagicMock()


@pytest.fixture
def sample_sheet_data():
    """Sample spreadsheet data."""
    return [
        ["Client", "Contact", "Stage", "Budget"],
        ["Acme", "John Doe", "lead", "15000"],
        ["TechCorp", "Jane Smith", "proposal", "25000"],
    ]


@pytest.fixture
def sample_append_data():
    """Sample data to append."""
    return [
        ["NewClient", "Bob Wilson", "qualifying", "18000"],
    ]


@pytest.fixture
def sample_update_data():
    """Sample data to update."""
    return [
        ["Acme", "John Smith", "negotiation", "15000"],
    ]


class TestGspreadBackendInit:
    """Test GspreadBackend initialization."""

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_init_with_params(self, mock_gs, mock_gspread_config):
        """Test initialization with explicit parameters."""
        mock_client = MagicMock()
        mock_gs.service_account.return_value = mock_client
        mock_client.open_by_key.return_value = MagicMock()

        backend = GspreadBackend(
            spreadsheet_id=mock_gspread_config["spreadsheet_id"],
            credentials_path=mock_gspread_config["credentials_path"],
        )

        assert backend.spreadsheet_id == mock_gspread_config["spreadsheet_id"]
        assert backend.credentials_path == mock_gspread_config["credentials_path"]
        mock_gs.service_account.assert_called_once_with(
            filename=mock_gspread_config["credentials_path"]
        )

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_init_with_json_params(self, mock_gs):
        """Test initialization with JSON parameter."""
        mock_client = MagicMock()
        mock_gs.service_account_from_dict.return_value = mock_client
        mock_client.open_by_key.return_value = MagicMock()

        credentials_json = '{"type": "service_account"}'
        backend = GspreadBackend(spreadsheet_id="test_id", credentials_json=credentials_json)

        assert backend.spreadsheet_id == "test_id"
        assert backend.credentials_json == credentials_json
        mock_gs.service_account_from_dict.assert_called_once()

    def test_init_missing_spreadsheet_id(self):
        """Test initialization with missing spreadsheet ID."""
        with pytest.raises(ValueError, match="SPREADSHEET_ID not configured"):
            GspreadBackend(credentials_path="/path/to/creds.json")

    def test_init_missing_credentials(self):
        """Test initialization with missing credentials."""
        with pytest.raises(ValueError, match="GOOGLE_CREDENTIALS_PATH or GOOGLE_CREDENTIALS_JSON"):
            GspreadBackend(spreadsheet_id="test_id")


class TestExtractWorksheetAndRange:
    """Test worksheet and range extraction."""

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_extract_with_range(self, mock_gs, mock_sheet, mock_worksheet):
        """Test extraction with range notation."""
        mock_gs.service_account.return_value = MagicMock()
        mock_gs.service_account.return_value.open_by_key.return_value = mock_sheet
        mock_sheet.worksheet.return_value = mock_worksheet

        backend = GspreadBackend(spreadsheet_id="test_id", credentials_path="/path/creds.json")

        ws, cell_range, orig = backend._extract_worksheet_and_range("Sheet1!A1:Z")
        assert ws == mock_worksheet
        assert cell_range == "A1:Z"
        assert orig == "Sheet1!A1:Z"
        mock_sheet.worksheet.assert_called_once_with("Sheet1")

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_extract_quoted(self, mock_gs, mock_sheet, mock_worksheet):
        """Test extraction with quoted sheet name."""
        mock_gs.service_account.return_value = MagicMock()
        mock_gs.service_account.return_value.open_by_key.return_value = mock_sheet
        mock_sheet.worksheet.return_value = mock_worksheet

        backend = GspreadBackend(spreadsheet_id="test_id", credentials_path="/path/creds.json")

        ws, cell_range, orig = backend._extract_worksheet_and_range("'Sheet 1'!A1")
        assert ws == mock_worksheet
        assert cell_range == "A1"
        mock_sheet.worksheet.assert_called_once_with("Sheet 1")

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_extract_simple(self, mock_gs, mock_sheet, mock_worksheet):
        """Test extraction without range."""
        mock_gs.service_account.return_value = MagicMock()
        mock_gs.service_account.return_value.open_by_key.return_value = mock_sheet
        mock_sheet.worksheet.return_value = mock_worksheet

        backend = GspreadBackend(spreadsheet_id="test_id", credentials_path="/path/creds.json")

        ws, cell_range, orig = backend._extract_worksheet_and_range("Sheet1")
        assert ws == mock_worksheet
        assert cell_range is None
        mock_sheet.worksheet.assert_called_once_with("Sheet1")


class TestCellRangeToRowCol:
    """Test cell range to row/col conversion."""

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_valid_range(self, mock_gs):
        """Test valid cell range."""
        mock_gs.service_account.return_value = MagicMock()
        mock_gs.service_account.return_value.open_by_key.return_value = MagicMock()
        mock_gs.utils.a1_to_column.return_value = 26

        backend = GspreadBackend(spreadsheet_id="test_id", credentials_path="/path/creds.json")

        result = backend._cell_range_to_row_col("A1:Z10")
        assert result == (1, 1, 10, 26)

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_single_cell(self, mock_gs):
        """Test single cell."""
        mock_gs.service_account.return_value = MagicMock()
        mock_gs.service_account.return_value.open_by_key.return_value = MagicMock()

        backend = GspreadBackend(spreadsheet_id="test_id", credentials_path="/path/creds.json")

        result = backend._cell_range_to_row_col("A1")
        assert result is None

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_invalid_range(self, mock_gs):
        """Test invalid cell range."""
        mock_gs.service_account.return_value = MagicMock()
        mock_gs.service_account.return_value.open_by_key.return_value = MagicMock()

        backend = GspreadBackend(spreadsheet_id="test_id", credentials_path="/path/creds.json")

        result = backend._cell_range_to_row_col("invalid")
        assert result is None


class TestRead:
    """Test read method."""

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_read_success(self, mock_gs, mock_sheet, mock_worksheet, sample_sheet_data):
        """Test successful read operation."""
        mock_gs.service_account.return_value = MagicMock()
        mock_gs.service_account.return_value.open_by_key.return_value = mock_sheet
        mock_sheet.worksheet.return_value = mock_worksheet
        mock_worksheet.get_values.return_value = sample_sheet_data

        backend = GspreadBackend(spreadsheet_id="test_id", credentials_path="/path/creds.json")
        result = backend.read("test_id", "Sheet1!A1:D")

        assert result.ok is True
        assert result.data == {"values": sample_sheet_data}
        mock_worksheet.get_values.assert_called_once()

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_read_all_data(self, mock_gs, mock_sheet, mock_worksheet, sample_sheet_data):
        """Test reading all data from sheet."""
        mock_gs.service_account.return_value = MagicMock()
        mock_gs.service_account.return_value.open_by_key.return_value = mock_sheet
        mock_sheet.worksheet.return_value = mock_worksheet
        mock_worksheet.get_values.return_value = sample_sheet_data

        backend = GspreadBackend(spreadsheet_id="test_id", credentials_path="/path/creds.json")
        result = backend.read("test_id", "Sheet1")

        assert result.ok is True
        assert result.data == {"values": sample_sheet_data}

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_read_error(self, mock_gs, mock_sheet, mock_worksheet):
        """Test read operation with error."""
        mock_gs.service_account.return_value = MagicMock()
        mock_gs.service_account.return_value.open_by_key.return_value = mock_sheet
        mock_sheet.worksheet.return_value = mock_worksheet
        mock_worksheet.get_values.side_effect = Exception("API error")

        backend = GspreadBackend(spreadsheet_id="test_id", credentials_path="/path/creds.json")
        result = backend.read("test_id", "Sheet1")

        assert result.ok is False
        assert "Gspread read error" in result.error


class TestAppend:
    """Test append method."""

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_append_success(
        self, mock_gs, mock_sheet, mock_worksheet, sample_append_data
    ):
        """Test successful append operation."""
        mock_gs.service_account.return_value = MagicMock()
        mock_gs.service_account.return_value.open_by_key.return_value = mock_sheet
        mock_sheet.worksheet.return_value = mock_worksheet

        backend = GspreadBackend(spreadsheet_id="test_id", credentials_path="/path/creds.json")
        result = backend.append("test_id", "Sheet1", sample_append_data)

        assert result.ok is True
        assert result.data == {"updatedRows": 1}
        mock_worksheet.append_rows.assert_called_once_with(sample_append_data, table_range="A1")

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_append_with_range(
        self, mock_gs, mock_sheet, mock_worksheet, sample_append_data
    ):
        """Test append with specified range."""
        mock_gs.service_account.return_value = MagicMock()
        mock_gs.service_account.return_value.open_by_key.return_value = mock_sheet
        mock_sheet.worksheet.return_value = mock_worksheet

        backend = GspreadBackend(spreadsheet_id="test_id", credentials_path="/path/creds.json")
        result = backend.append("test_id", "Sheet1!C1", sample_append_data)

        assert result.ok is True
        mock_worksheet.append_rows.assert_called_once_with(sample_append_data, table_range="C1")

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_append_error(self, mock_gs, mock_sheet, mock_worksheet, sample_append_data):
        """Test append operation with error."""
        mock_gs.service_account.return_value = MagicMock()
        mock_gs.service_account.return_value.open_by_key.return_value = mock_sheet
        mock_sheet.worksheet.return_value = mock_worksheet
        mock_worksheet.append_rows.side_effect = Exception("API error")

        backend = GspreadBackend(spreadsheet_id="test_id", credentials_path="/path/creds.json")
        result = backend.append("test_id", "Sheet1", sample_append_data)

        assert result.ok is False
        assert "Gspread append error" in result.error


class TestUpdate:
    """Test update method."""

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_update_success(
        self, mock_gs, mock_sheet, mock_worksheet, sample_update_data
    ):
        """Test successful update operation."""
        mock_gs.service_account.return_value = MagicMock()
        mock_gs.service_account.return_value.open_by_key.return_value = mock_sheet
        mock_sheet.worksheet.return_value = mock_worksheet

        backend = GspreadBackend(spreadsheet_id="test_id", credentials_path="/path/creds.json")
        result = backend.update("test_id", "Sheet1!A2:D", sample_update_data)

        assert result.ok is True
        assert result.data == {"updatedRows": 1}
        mock_worksheet.update.assert_called_once_with(sample_update_data, "A2")

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_update_full_sheet(
        self, mock_gs, mock_sheet, mock_worksheet, sample_update_data
    ):
        """Test updating full sheet (no range specified)."""
        mock_gs.service_account.return_value = MagicMock()
        mock_gs.service_account.return_value.open_by_key.return_value = mock_sheet
        mock_sheet.worksheet.return_value = mock_worksheet

        backend = GspreadBackend(spreadsheet_id="test_id", credentials_path="/path/creds.json")
        result = backend.update("test_id", "Sheet1", sample_update_data)

        assert result.ok is True
        mock_worksheet.update.assert_called_once_with(sample_update_data, "A1")

    @patch("openclaw_crm.backends.gspread_backend.gspread", create=True)
    def test_update_error(self, mock_gs, mock_sheet, mock_worksheet, sample_update_data):
        """Test update operation with error."""
        mock_gs.service_account.return_value = MagicMock()
        mock_gs.service_account.return_value.open_by_key.return_value = mock_sheet
        mock_sheet.worksheet.return_value = mock_worksheet
        mock_worksheet.update.side_effect = Exception("API error")

        backend = GspreadBackend(spreadsheet_id="test_id", credentials_path="/path/creds.json")
        result = backend.update("test_id", "Sheet1", sample_update_data)

        assert result.ok is False
        assert "Gspread update error" in result.error
