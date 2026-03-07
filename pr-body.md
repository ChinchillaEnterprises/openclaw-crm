## Summary

Implements a `GspreadBackend` class as an alternative to the default `gws` CLI backend for Google Sheets access.

## Changes

- Add `src/openclaw_crm/backends/` package with `GspreadBackend` class
- Implements `read()`, `append()`, `update()` methods matching `SheetsBackend` interface
- Add `gspread` as optional dependency in `pyproject.toml`
- Update README with gspread backend usage instructions

## Acceptance Criteria

- [x] New file `src/openclaw_crm/backends/gspread_backend.py` with `GspreadBackend(SheetsBackend)` class
- [x] Implements `read()`, `append()`, `update()` methods matching `SheetsBackend` interface
- [x] Uses `gspread` library (add as optional dependency: `pip install openclaw-crm[gspread]`)
- [x] Returns `SheetResult` objects matching existing contract
- [x] Include usage example in docstring or README section
- [x] No changes to existing files except `pyproject.toml` (optional dep) and `README.md` (docs)

## Files Changed

- `src/openclaw_crm/backends/__init__.py` (new)
- `src/openclaw_crm/backends/gspread_backend.py` (new)
- `pyproject.toml` (add `gspread` optional dependency)
- `README.md` (add gspread setup instructions)

---

Bounty Claim: Issue #1 - $1