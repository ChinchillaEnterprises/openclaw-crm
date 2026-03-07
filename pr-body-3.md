## Summary

Add unit tests for the network module (spider network referral tracking).

## Changes

- Add `tests/test_network.py` with 10 unit tests

## Test Coverage

- `add_signal()` - basic signal creation
- `get_pending_signals()` - retrieving pending signals
- `promote_signal()` - basic promote, already promoted guard
- `dismiss_signal()` - signal dismissal
- `get_network_tree()` - full tree and root-specific tree
- `get_network_value()` - calculating network value
- `check_competitor_guard()` - competitor guard checks

## Acceptance Criteria

- [x] New file `tests/test_network.py`
- [x] Tests cover: `add_signal()`, `promote_signal()`, `dismiss_signal()`, `get_network_tree()`, `get_network_value()`, `check_competitor_guard()`
- [x] Uses MockBackend from conftest
- [x] Tests verify: atomic promote (deal created before signal marked), re-promote guard, competitor guard checks both Pipeline
- [x] All tests pass with `pytest tests/test_network.py`

## Test Results

```
tests/test_network.py::TestAddSignal::test_add_signal_basic PASSED
tests/test_network.py::TestGetPendingSignals::test_get_pending_signals PASSED
tests/test_network.py::TestPromoteSignal::test_promote_signal_basic PASSED
tests/test_network.py::TestPromoteSignal::test_promote_already_promoted PASSED
tests/test_network.py::TestDismissSignal::test_dismiss_signal PASSED
tests/test_network.py::TestGetNetworkTree::test_get_network_tree_all PASSED
tests/test_network.py::TestGetNetworkTree::test_get_network_tree_root PASSED
tests/test_network.py::TestGetNetworkValue::test_get_network_value PASSED
tests/test_network.py::TestCompetitorGuard::test_competitor_guard_allowed PASSED
tests/test_network.py::TestCompetitorGuard::test_competitor_guard_blocked PASSED

10 passed
```

---

Bounty Claim: Issue #3 - $0.50