"""Unit tests for Airtable backend."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from openclaw_crm.backends.airtable_backend import AirtableBackend, PIPELINE_FIELDS
from openclaw_crm.sheets import SheetResult


class TestAirtableBackend:
    """Tests for AirtableBackend class."""

    @pytest.fixture
    def mock_api(self):
        """Create a mock Airtable API."""
        with patch("openclaw_crm.backends.airtable_backend.Api") as mock:
            yield mock.return_value

    def test_init_with_credentials(self, mock_api):
        """Test initialization with explicit credentials."""
        backend = AirtableBackend(base_id="app123", api_token="token123")
        assert backend._base_id == "app123"
        assert backend._api_token == "token123"

    def test_init_from_env(self, mock_api):
        """Test initialization from environment variables."""
        with patch.dict("os.environ", {"AIRTABLE_BASE_ID": "appEnv", "AIRTABLE_API_TOKEN": "tokenEnv"}):
            backend = AirtableBackend()
            assert backend._base_id == "appEnv"
            assert backend._api_token == "tokenEnv"

    def test_init_missing_credentials(self, mock_api):
        """Test initialization fails without credentials."""
        with patch.dict("os.environ", {}):
            with pytest.raises(ValueError, match="credentials required"):
                AirtableBackend()

    def test_read_returns_sheet_result(self, mock_api):
        """Test read returns SheetResult in correct format."""
        mock_table = MagicMock()
        mock_api.table.return_value = mock_table
        mock_table.all.return_value = [
            {"id": "rec1", "fields": {"Client": "Acme", "Stage": "lead"}},
            {"id": "rec2", "fields": {"Client": "Tech", "Stage": "proposal"}},
        ]

        backend = AirtableBackend(base_id="app123", api_token="token123")
        result = backend.read("Pipeline", "")

        assert result.success is True
        assert "values" in result.data

    def test_append_creates_record(self, mock_api):
        """Test append creates a new record."""
        mock_table = MagicMock()
        mock_api.table.return_value = mock_table
        mock_table.create.return_value = MagicMock(id="recNew", fields={"Client": "New"})

        backend = AirtableBackend(base_id="app123", api_token="token123")
        result = backend.append("Pipeline", "", [["New Corp", "John", "upwork", "lead"]])

        assert result.success is True
        mock_table.create.assert_called_once()

    def test_update_modifies_record(self, mock_api):
        """Test update modifies an existing record."""
        mock_table = MagicMock()
        mock_api.table.return_value = mock_table
        mock_table.update.return_value = MagicMock(id="rec1", fields={"Client": "Updated"})

        backend = AirtableBackend(base_id="app123", api_token="token123")
        result = backend.update("Pipeline", "rec1", [["Updated Corp", "Jane", "network", "proposal"]])

        assert result.success is True
        mock_table.update.assert_called_once()

    def test_read_handles_empty_table(self, mock_api):
        """Test read handles empty table."""
        mock_table = MagicMock()
        mock_api.table.return_value = mock_table
        mock_table.all.return_value = []

        backend = AirtableBackend(base_id="app123", api_token="token123")
        result = backend.read("Pipeline", "")

        assert result.success is True
        # Should return headers at minimum

    def test_append_handles_error(self, mock_api):
        """Test append handles API errors."""
        mock_table = MagicMock()
        mock_api.table.return_value = mock_table
        mock_table.create.side_effect = Exception("API Error")

        backend = AirtableBackend(base_id="app123", api_token="token123")
        result = backend.append("Pipeline", "", [["Test"]])

        assert result.success is False
        assert "API Error" in result.error
