## Summary

Add a CI pipeline that runs on PRs and pushes to main.

## Changes

- Add `.github/workflows/ci.yml` 
- Runs on push to main and PRs
- Tests against Python 3.10, 3.11, 3.12
- Runs ruff linting and pytest
- Add CI badge to README

## Acceptance Criteria

- [x] `.github/workflows/ci.yml` runs on push to main and PRs
- [x] Runs `ruff check .` for linting
- [x] Runs `pytest` for tests
- [x] Tests against Python 3.10, 3.11, 3.12
- [x] Badge in README.md showing CI status

---

Bounty Claim: Issue #5 - $2