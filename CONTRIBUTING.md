# Contributing to openclaw-crm

## For AI Agents

1. Find an issue labeled `bounty` + `agent-friendly`
2. Claim it by commenting with your agent ID
3. Fork, branch, implement, PR
4. Include `Bounty: #<issue>` and `Wallet: <address>` in PR description
5. Payment sent after merge

See [BOUNTY.md](.github/BOUNTY.md) for full details.

## For Humans

Standard fork-and-PR workflow. We welcome:
- New `SheetsBackend` implementations (gspread, google-api-python-client, Airtable, etc.)
- Test coverage improvements
- Documentation
- Bug fixes

## Development Setup

```bash
git clone https://github.com/ChinchillaEnterprises/openclaw-crm.git
cd openclaw-crm
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Code Style

- Python 3.10+
- Ruff for linting (`ruff check .`)
- No type stubs required, but type hints appreciated
- Keep it simple — this is a lightweight tool, not an enterprise platform

## Testing

```bash
pytest
```

Tests should mock the sheets backend — don't hit real Google Sheets in CI.

## Architecture

```
src/openclaw_crm/
├── __init__.py         # CRMManager facade (formatted output)
├── config.py           # Config loading (YAML or env vars)
├── sheets.py           # Pluggable Google Sheets backend
├── pipeline.py         # Pipeline CRUD + queries
├── network.py          # Spider network (referrals, signals, tree)
├── channel_scanner.py  # Stub for Slack signal detection
└── cli.py              # JSON CLI entry point
```

The key extension point is `SheetsBackend` in `sheets.py`. Implement `read()`, `append()`, `update()` for your storage backend.
