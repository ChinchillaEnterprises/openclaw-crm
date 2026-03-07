"""Tests for Airtable backend."""

import pytest
from unittest.mock import Mock, patch

from openclaw_crm.backends.airtable_backend import AirtableBackend
from openclaw_crm.sheets import SheetResult


class TestAirtableBackend:
    """Test cases for AirtableBackend."""
    
    @patch("openclaw_crm.backends.airtable_backend.Api")
    def test_read(self, mock_api):
        """Test reading from Airtable."""
        mock_table = Mock()
        mock_table.all.return_value = [
            {"id": "rec1", "fields": {"Client": "Acme", "Budget": "10000"}},
            {"id": "rec2", "fields": {"Client": "Beta", "Budget": "5000"}},
        ]
        mock_api.return_value.table.return_value = mock_table
        
        backend = AirtableBackend("fake_key", "fake_base")
        result = backend.read()
        
        assert result.success is True
        assert len(result.data["values"]) == 2
    
    @patch("openclaw_crm.backends.airtable_backend.Api")
    def test_append(self, mock_api):
        """Test appending to Airtable."""
        mock_table = Mock()
        mock_table.create.return_value = {"id": "rec_new"}
        mock_api.return_value.table.return_value = mock_table
        
        backend = AirtableBackend("fake_key", "fake_base")
        result = backend.append(None, None, [["Acme", "John", "john@acme.com", "", "10000", "lead"]])
        
        assert result.success is True
        assert "id" in result.data
    
    @patch("openclaw_crm.backends.airtable_backend.Api")
    def test_update(self, mock_api):
        """Test updating Airtable record."""
        mock_table = Mock()
        mock_table.update.return_value = {"id": "rec1"}
        mock_api.return_value.table.return_value = mock_table
        
        backend = AirtableBackend("fake_key", "fake_base")
        result = backend.update(None, None, [["rec1", "Acme Corp", "John", "john@acme.com"]])
        
        assert result.success is True
