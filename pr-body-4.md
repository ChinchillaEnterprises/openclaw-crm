## Summary

Implement an `AirtableBackend` class as an alternative storage backend using the Airtable API.

## Changes

- Add `src/openclaw_crm/backends/airtable_backend.py` with `AirtableBackend(SheetsBackend)` class
- Add `pyairtable` as optional dependency in `pyproject.toml`
- Add README documentation for Airtable setup
- Add `tests/test_airtable_backend.py` with 8 unit tests

## Acceptance Criteria

- [x] New file `src/openclaw_crm/backends/airtable_backend.py` with `AirtableBackend(SheetsBackend)` class
- [x] Uses `pyairtable` library (add as optional dependency)
- [x] Maps Pipeline columns to Airtable fields
- [x] Implements `read()`, `append()`, `update()` matching `SheetsBackend` interface
- [x] Config accepts Airtable base ID + API token via env vars or config file
- [x] Include setup instructions in README
- [x] Tests with mock Airtable responses

## Test Results

```
tests/test_airtable_backend.py::TestAirtableBackend::test_init_with_credentials PASSED
tests/test_airtable_backend.py::TestAirtableBackend::test_init_from_env PASSED
tests/test_airtable_backend.py::TestAirtableBackend::test_init_missing_credentials PASSED
tests/test_airtable_backend.py::TestAirtableBackend::test_read_returns_sheet_result PASSED
tests/test_airtable_backend.py::TestAirtableBackend::test_append_creates_record PASSED
tests/test_airtable_backend.py::TestAirtableBackend::test_update_modifies_record PASSED
tests/test_airtable_backend.py::TestAirtableBackend::test_read_handles_empty_table PASSED
tests/test_airtable_backend.py::TestAirtableBackend::test_append_handles_error PASSED

8 passed
```

---

Bounty Claim: Issue #4 - $5