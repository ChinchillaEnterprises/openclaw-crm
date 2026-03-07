"""Tests for AirtableBackend."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest


class TestAirtableBackend:
    """Test cases for AirtableBackend class."""

    def test_init_with_env_vars(self):
        """Test initialization with environment variables."""
        with patch.dict(os.environ, {"AIRTABLE_BASE_ID": "appTest123", "AIRTABLE_API_TOKEN": "patTest456"}):
            from openclaw_crm.backends.airtable_backend import AirtableBackend
            
            backend = AirtableBackend()
            assert backend._base_id == "appTest123"
            assert backend._api_token == "patTest456"

    def test_init_with_params(self):
        """Test initialization with parameters."""
        from openclaw_crm.backends.airtable_backend import AirtableBackend
        
        backend = AirtableBackend(base_id="appXXX", api_token="patYYY")
        assert backend._base_id == "appXXX"
        assert backend._api_token == "patYYY"

    def test_init_with_custom_column_map(self):
        """Test initialization with custom column mapping."""
        from openclaw_crm.backends.airtable_backend import AirtableBackend
        
        custom_map = {"Client": "Client Name", "Stage": "Deal Stage"}
        backend = AirtableBackend(column_map=custom_map)
        assert backend._column_map == custom_map

    def test_parse_range_simple(self):
        """Test parsing simple table name."""
        from openclaw_crm.backends.airtable_backend import AirtableBackend
        
        backend = AirtableBackend()
        table_name, row_num = backend._parse_range("Pipeline")
        assert table_name == "Pipeline"
        assert row_num is None

    def test_parse_range_with_sheet(self):
        """Test parsing range with sheet name."""
        from openclaw_crm.backends.airtable_backend import AirtableBackend
        
        backend = AirtableBackend()
        table_name, row_num = backend._parse_range("Pipeline!A:U")
        assert table_name == "Pipeline"
        assert row_num is None

    def test_parse_range_with_row(self):
        """Test parsing range with row number."""
        from openclaw_crm.backends.airtable_backend import AirtableBackend
        
        backend = AirtableBackend()
        table_name, row_num = backend._parse_range("Pipeline!A2:U2")
        assert table_name == "Pipeline"
        assert row_num == 2

    def test_parse_range_with_quoted_sheet(self):
        """Test parsing range with quoted sheet name."""
        from openclaw_crm.backends.airtable_backend import AirtableBackend
        
        backend = AirtableBackend()
        table_name, row_num = backend._parse_range("'Revenue Log'!A:F")
        assert table_name == "Revenue Log"
        assert row_num is None

    @patch("openclaw_crm.backends.airtable_backend.AirtableBackend._get_client")
    def test_read_success(self, mock_get_client):
        """Test successful read operation."""
        from openclaw_crm.backends.airtable_backend import AirtableBackend
        
        # Mock the Airtable API response
        mock_table = MagicMock()
        mock_table.all.return_value = [
            {"id": "rec123", "fields": {"Client": "Acme Corp", "Stage": "lead"}},
            {"id": "rec456", "fields": {"Client": "Beta Inc", "Stage": "qualifying"}},
        ]
        
        mock_api = MagicMock()
        mock_api.table.return_value = mock_table
        mock_get_client.return_value = mock_api
        
        backend = AirtableBackend(base_id="appTest", api_token="patTest")
        result = backend.read("appTest", "Pipeline")
        
        assert result.success is True
        assert result.data is not None
        assert "values" in result.data

    @patch("openclaw_crm.backends.airtable_backend.AirtableBackend._get_client")
    def test_read_empty_table(self, mock_get_client):
        """Test reading empty table."""
        from openclaw_crm.backends.airtable_backend import AirtableBackend
        
        mock_table = MagicMock()
        mock_table.all.return_value = []
        
        mock_api = MagicMock()
        mock_api.table.return_value = mock_table
        mock_get_client.return_value = mock_api
        
        backend = AirtableBackend(base_id="appTest", api_token="patTest")
        result = backend.read("appTest", "Pipeline")
        
        assert result.success is True
        assert result.data == {"values": []}

    @patch("openclaw_crm.backends.airtable_backend.AirtableBackend._get_client")
    def test_read_error(self, mock_get_client):
        """Test read operation error."""
        from openclaw_crm.backends.airtable_backend import AirtableBackend
        
        mock_get_client.return_value = None
        
        backend = AirtableBackend(base_id="appTest", api_token="patTest")
        result = backend.read("appTest", "Pipeline")
        
        assert result.success is False
        assert "pyairtable not installed" in result.error

    @patch("openclaw_crm.backends.airtable_backend.AirtableBackend._get_client")
    def test_append_success(self, mock_get_client):
        """Test successful append operation."""
        from openclaw_crm.backends.airtable_backend import AirtableBackend
        
        mock_table = MagicMock()
        mock_table.create.return_value = {"id": "rec789"}
        
        mock_api = MagicMock()
        mock_api.table.return_value = mock_table
        mock_get_client.return_value = mock_api
        
        backend = AirtableBackend(base_id="appTest", api_token="patTest")
        values = [
            ["Client", "Stage"],
            ["New Corp", "lead"],
        ]
        result = backend.append("appTest", "Pipeline", values)
        
        assert result.success is True

    @patch("openclaw_crm.backends.airtable_backend.AirtableBackend._get_client")
    def test_update_success(self, mock_get_client):
        """Test successful update operation."""
        from openclaw_crm.backends.airtable_backend import AirtableBackend
        
        # Setup mock with cached record ID
        mock_table = MagicMock()
        mock_table.update.return_value = {"id": "rec123", "fields": {"Client": "Updated Corp"}}
        
        mock_api = MagicMock()
        mock_api.table.return_value = mock_table
        mock_get_client.return_value = mock_api
        
        backend = AirtableBackend(base_id="appTest", api_token="patTest")
        
        # Pre-cache the record ID
        backend._record_id_cache["Pipeline"] = {2: "rec123"}
        
        values = [
            ["Client", "Stage"],
            ["Updated Corp", "proposal"],
        ]
        result = backend.update("appTest", "Pipeline!A2:U2", values)
        
        assert result.success is True

    @patch("openclaw_crm.backends.airtable_backend.AirtableBackend._get_client")
    def test_update_missing_row_number(self, mock_get_client):
        """Test update without row number fails."""
        from openclaw_crm.backends.airtable_backend import AirtableBackend
        
        backend = AirtableBackend(base_id="appTest", api_token="patTest")
        values = [["Client"], ["Test"]]
        result = backend.update("appTest", "Pipeline", values)
        
        assert result.success is False
        assert "Row number required" in result.error

    @patch("openclaw_crm.backends.airtable_backend.AirtableBackend._get_client")
    def test_update_no_record_id(self, mock_get_client):
        """Test update with uncached record ID fails."""
        from openclaw_crm.backends.airtable_backend import AirtableBackend
        
        backend = AirtableBackend(base_id="appTest", api_token="patTest")
        
        # No cached record ID
        values = [["Client"], ["Test"]]
        result = backend.update("appTest", "Pipeline!A2:U2", values)
        
        assert result.success is False
        assert "Record not found" in result.error


class TestDefaultColumnMap:
    """Test the default column mapping."""

    def test_default_map_has_all_headers(self):
        """Test that default map includes all standard headers."""
        from openclaw_crm.backends.airtable_backend import DEFAULT_COLUMN_MAP
        from openclaw_crm.pipeline import HEADERS
        
        for header in HEADERS:
            assert header in DEFAULT_COLUMN_MAP, f"Missing mapping for {header}"
