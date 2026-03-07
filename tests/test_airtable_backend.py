import os
import unittest
from unittest.mock import MagicMock, patch

from openclaw_crm.backends.airtable_backend import AirtableBackend
from openclaw_crm.sheets import SheetResult

class TestAirtableBackend(unittest.TestCase):
    def setUp(self):
        self.api_key = "test_key"
        self.base_id = "test_base"
        self.patcher = patch('openclaw_crm.backends.airtable_backend.Api')
        self.mock_api_cls = self.patcher.start()
        self.mock_api = self.mock_api_cls.return_value
        self.backend = AirtableBackend(api_key=self.api_key, base_id=self.base_id)

    def tearDown(self):
        self.patcher.stop()

    def test_init_raises_if_no_api_key(self):
        with self.assertRaises(ValueError):
            AirtableBackend(api_key="", base_id="base")

    def test_init_raises_if_no_base_id(self):
        with self.assertRaises(ValueError):
            AirtableBackend(api_key="key", base_id="")

    def test_get_table_name(self):
        self.assertEqual(self.backend._get_table_name("Pipeline!A:U"), "Pipeline")
        self.assertEqual(self.backend._get_table_name("'Revenue Log'!A:F"), "Revenue Log")
        self.assertEqual(self.backend._get_table_name("Pipeline"), "Pipeline")

    def test_read_pipeline(self):
        table_mock = MagicMock()
        self.mock_api.table.return_value = table_mock
        
        # Mock records
        records = [
            {"fields": {"Client": "Client1", "Stage": "lead", "Budget": "100"}},
            {"fields": {"Client": "Client2", "Stage": "won", "Budget": "200"}},
        ]
        table_mock.all.return_value = records

        result = self.backend.read("ignored", "Pipeline!A:U")
        
        self.assertTrue(result.success)
        values = result.data["values"]
        self.assertEqual(len(values), 3) # Headers + 2 records
        
        headers = values[0]
        self.assertIn("Client", headers)
        self.assertIn("Stage", headers)
        
        client_idx = headers.index("Client")
        stage_idx = headers.index("Stage")
        
        self.assertEqual(values[1][client_idx], "Client1")
        self.assertEqual(values[1][stage_idx], "lead")
        self.assertEqual(values[2][client_idx], "Client2")
        self.assertEqual(values[2][stage_idx], "won")

        self.mock_api.table.assert_called_with(self.base_id, "Pipeline")

    def test_append_pipeline(self):
        table_mock = MagicMock()
        self.mock_api.table.return_value = table_mock
        table_mock.create.return_value = {"id": "rec123", "fields": {}}

        # Headers are needed to map columns
        headers = self.backend._get_headers("Pipeline")
        row = ["New Client"] + [""] * (len(headers) - 1)
        row[headers.index("Stage")] = "lead"
        
        result = self.backend.append("ignored", "Pipeline!A:U", [row])
        
        self.assertTrue(result.success)
        self.assertEqual(result.data["updates"]["updatedRows"], 1)
        
        expected_fields = {"Client": "New Client", "Stage": "lead"}
        # Other empty fields might be present depending on implementation
        # Our implementation includes empty strings if they are in row
        
        call_args = table_mock.create.call_args[0][0]
        self.assertEqual(call_args["Client"], "New Client")
        self.assertEqual(call_args["Stage"], "lead")

    def test_update_pipeline(self):
        table_mock = MagicMock()
        self.mock_api.table.return_value = table_mock
        
        # Mock records to find ID by index
        records = [
            {"id": "rec1", "fields": {}},
            {"id": "rec2", "fields": {}}, # Row 3 (index 1)
            {"id": "rec3", "fields": {}}, # Row 4 (index 2)
        ]
        table_mock.all.return_value = records

        # Update row 4 (index 2)
        headers = self.backend._get_headers("Pipeline")
        row = ["Updated Client"] + [""] * (len(headers) - 1)
        
        result = self.backend.update("ignored", "Pipeline!A4:U4", [row])
        
        self.assertTrue(result.success)
        self.assertEqual(result.data["updatedRows"], 1)
        
        table_mock.update.assert_called_with("rec3", unittest.mock.ANY)
        call_args = table_mock.update.call_args[0][1]
        self.assertEqual(call_args["Client"], "Updated Client")

    def test_update_invalid_range(self):
        result = self.backend.update("ignored", "InvalidRange", [[]])
        self.assertFalse(result.success)
        self.assertIn("Invalid range format", result.error)

    def test_read_failure(self):
        self.mock_api.table.side_effect = Exception("API Error")
        result = self.backend.read("ignored", "Pipeline!A:U")
        self.assertFalse(result.success)
        self.assertEqual(result.error, "API Error")

if __name__ == "__main__":
    unittest.main()
