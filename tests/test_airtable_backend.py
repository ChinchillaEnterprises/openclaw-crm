from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from openclaw_crm.backends.airtable_backend import AirtableBackend
from openclaw_crm.sheets import SheetResult


@pytest.fixture
def mock_airtable_config():
    """Mock Airtable configuration."""
    return {
        "base_id": "app1234567890",
        "api_token": "pat1234567890",
    }


@pytest.fixture
def mock_airtable_api(mock_airtable_config):
    """Mock pyairtable Api and Base."""
    with patch("openclaw_crm.backends.airtable_backend.Api") as mock_api_class, patch.dict(
        "os.environ",
        {
            "AIRTABLE_BASE_ID": mock_airtable_config["base_id"],
            "AIRTABLE_API_TOKEN": mock_airtable_config["api_token"],
        },
    ):
        mock_api = MagicMock()
        mock_base = MagicMock()
        mock_table = MagicMock()

        mock_api_class.return_value = mock_api
        mock_api.base.return_value = mock_base
        mock_base.table.return_value = mock_table

        yield mock_table


@pytest.fixture
def backend(mock_airtable_config):
    """Create AirtableBackend instance with mocked configuration."""
    with patch.dict(
        "os.environ",
        {
            "AIRTABLE_BASE_ID": mock_airtable_config["base_id"],
            "AIRTABLE_API_TOKEN": mock_airtable_config["api_token"],
        },
    ):
        return AirtableBackend()


class TestAirtableBackendInit:
    """Test AirtableBackend initialization."""

    def test_init_with_env_vars(self):
        """Test initialization with environment variables."""
        with patch.dict(
            "os.environ",
            {"AIRTABLE_BASE_ID": "app123", "AIRTABLE_API_TOKEN": "pat123"},
        ):
            with patch("openclaw_crm.backends.airtable_backend.Api"):
                backend = AirtableBackend()
                assert backend.base_id == "app123"
                assert backend.api_token == "pat123"

    def test_init_without_base_id(self):
        """Test initialization fails without base_id."""
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ValueError, match="AIRTABLE_BASE_ID not configured"):
                AirtableBackend()

    def test_init_without_api_token(self):
        """Test initialization fails without api_token."""
        with patch.dict("os.environ", {"AIRTABLE_BASE_ID": "app123"}, clear=True):
            with pytest.raises(ValueError, match="AIRTABLE_API_TOKEN not configured"):
                AirtableBackend()


class TestExtractTableName:
    """Test table name extraction from range strings."""

    def test_extract_table_name_simple(self):
        """Test extraction of simple table name."""
        backend = AirtableBackend.__new__(AirtableBackend)
        result = backend._extract_table_name("Pipeline")
        assert result == "Pipeline"

    def test_extract_table_name_with_range(self):
        """Test extraction with range notation."""
        backend = AirtableBackend.__new__(AirtableBackend)
        result = backend._extract_table_name("Pipeline!A1:Z")
        assert result == "Pipeline"

    def test_extract_table_name_with_single_cell(self):
        """Test extraction with single cell notation."""
        backend = AirtableBackend.__new__(AirtableBackend)
        result = backend._extract_table_name("Pipeline!A1")
        assert result == "Pipeline"

    def test_extract_table_name_with_quotes(self):
        """Test extraction with quoted table name."""
        backend = AirtableBackend.__new__(AirtableBackend)
        result = backend._extract_table_name("'Pipeline'!A1:Z")
        assert result == "Pipeline"


class TestConvertSheetsToAirtable:
    """Test conversion from Google Sheets format to Airtable format."""

    def test_convert_with_data(self):
        """Test conversion with actual data."""
        backend = AirtableBackend.__new__(AirtableBackend)
        values = [
            ["Client", "Budget", "Stage"],
            ["Acme", "15000", "lead"],
            ["TechCorp", "25000", "proposal"],
        ]
        result = backend._convert_sheets_values_to_airtable_records(values, "Pipeline")
        assert len(result) == 2
        assert result[0] == {"Client": "Acme", "Budget": "15000", "Stage": "lead"}
        assert result[1] == {"Client": "TechCorp", "Budget": "25000", "Stage": "proposal"}

    def test_convert_empty(self):
        """Test conversion with empty data."""
        backend = AirtableBackend.__new__(AirtableBackend)
        result = backend._convert_sheets_values_to_airtable_records([], "Pipeline")
        assert result == []

    def test_convert_with_missing_values(self):
        """Test conversion with missing values in rows."""
        backend = AirtableBackend.__new__(AirtableBackend)
        values = [
            ["Client", "Budget", "Stage"],
            ["Acme", "", "lead"],
        ]
        result = backend._convert_sheets_values_to_airtable_records(values, "Pipeline")
        assert len(result) == 1
        assert "Budget" not in result[0]


class TestConvertAirtableToSheets:
    """Test conversion from Airtable format to Google Sheets format."""

    def test_convert_with_data(self):
        """Test conversion with actual data."""
        backend = AirtableBackend.__new__(AirtableBackend)
        records = [
            {"id": "rec1", "fields": {"Client": "Acme", "Budget": "15000", "Stage": "lead"}},
            {"id": "rec2", "fields": {"Client": "TechCorp", "Budget": "25000", "Stage": "proposal"}},
        ]
        result = backend._convert_airtable_records_to_sheets_values(records)
        assert len(result) == 3  # Header + 2 rows
        assert result[0] == ["Budget", "Client", "Stage"]  # Sorted headers
        assert result[1] == ["15000", "Acme", "lead"]
        assert result[2] == ["25000", "TechCorp", "proposal"]

    def test_convert_empty(self):
        """Test conversion with empty data."""
        backend = AirtableBackend.__new__(AirtableBackend)
        result = backend._convert_airtable_records_to_sheets_values([])
        assert result == []

    def test_convert_with_null_values(self):
        """Test conversion with null/None values."""
        backend = AirtableBackend.__new__(AirtableBackend)
        records = [{"id": "rec1", "fields": {"Client": "Acme", "Budget": None}}]
        result = backend._convert_airtable_records_to_sheets_values(records)
        assert len(result) == 2
        assert result[1] == ["", "Acme"]


class TestRead:
    """Test read method."""

    def test_read_success(self, backend, mock_airtable_api):
        """Test successful read."""
        mock_airtable_api.all.return_value = [
            {"id": "rec1", "fields": {"Client": "Acme", "Budget": "15000"}},
            {"id": "rec2", "fields": {"Client": "TechCorp", "Budget": "25000"}},
        ]

        result = backend.read("spreadsheet_id", "Pipeline")

        assert result.success is True
        assert "values" in result.data
        assert len(result.data["values"]) == 3  # Header + 2 rows
        mock_airtable_api.all.assert_called_once()

    def test_read_empty_table(self, backend, mock_airtable_api):
        """Test reading empty table."""
        mock_airtable_api.all.return_value = []

        result = backend.read("spreadsheet_id", "Pipeline")

        assert result.success is True
        assert result.data["values"] == []

    def test_read_error(self, backend, mock_airtable_api):
        """Test read with error."""
        mock_airtable_api.all.side_effect = Exception("API Error")

        result = backend.read("spreadsheet_id", "Pipeline")

        assert result.success is False
        assert result.data is None
        assert "Airtable read error" in result.error


class TestAppend:
    """Test append method."""

    def test_append_success(self, backend, mock_airtable_api):
        """Test successful append."""
        mock_airtable_api.batch_create.return_value = [
            {"id": "rec1"},
            {"id": "rec2"},
        ]

        values = [
            ["Client", "Budget"],
            ["Acme", "15000"],
            ["TechCorp", "25000"],
        ]
        result = backend.append("spreadsheet_id", "Pipeline", values)

        assert result.success is True
        assert result.data["updatedRows"] == 2
        mock_airtable_api.batch_create.assert_called_once()

    def test_append_empty(self, backend, mock_airtable_api):
        """Test append with empty data."""
        values = [["Client", "Budget"]]
        result = backend.append("spreadsheet_id", "Pipeline", values)

        assert result.success is True
        assert result.data["updatedRows"] == 0

    def test_append_error(self, backend, mock_airtable_api):
        """Test append with error."""
        mock_airtable_api.batch_create.side_effect = Exception("API Error")

        values = [["Client"], ["Acme"]]
        result = backend.append("spreadsheet_id", "Pipeline", values)

        assert result.success is False
        assert "Airtable append error" in result.error


class TestUpdate:
    """Test update method."""

    def test_update_success(self, backend, mock_airtable_api):
        """Test successful update."""
        existing_records = [
            {"id": "rec1", "fields": {"Client": "Acme", "Budget": "15000"}},
            {"id": "rec2", "fields": {"Client": "TechCorp", "Budget": "25000"}},
        ]
        mock_airtable_api.all.return_value = existing_records
        mock_airtable_api.update.return_value = {"id": "rec1", "fields": {"Client": "Acme Updated"}}

        values = [
            ["Client", "Budget"],
            ["Acme Updated", "15000"],
            ["TechCorp", "25000"],
        ]
        result = backend.update("spreadsheet_id", "Pipeline", values)

        assert result.success is True
        assert result.data["updatedRows"] == 2

    def test_update_with_new_records(self, backend, mock_airtable_api):
        """Test update with additional new records."""
        existing_records = [{"id": "rec1", "fields": {"Client": "Acme", "Budget": "15000"}}]
        mock_airtable_api.all.return_value = existing_records
        mock_airtable_api.batch_create.return_value = [{"id": "rec2"}]

        values = [
            ["Client", "Budget"],
            ["Acme", "15000"],
            ["TechCorp", "25000"],  # New record
        ]
        result = backend.update("spreadsheet_id", "Pipeline", values)

        assert result.success is True
        assert result.data["updatedRows"] == 2
        mock_airtable_api.batch_create.assert_called_once()

    def test_update_error(self, backend, mock_airtable_api):
        """Test update with error."""
        mock_airtable_api.all.side_effect = Exception("API Error")

        values = [["Client"], ["Acme"]]
        result = backend.update("spreadsheet_id", "Pipeline", values)

        assert result.success is False
        assert "Airtable update error" in result.error
