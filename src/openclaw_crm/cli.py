from __future__ import annotations

import json
import sys

from openclaw_crm import CRMManager


def _out(text: str, data: dict | None = None) -> None:
    print(json.dumps({"ok": True, "text": text, "data": data or {}}))


def _err(msg: str) -> None:
    print(json.dumps({"ok": False, "error": msg}))
    sys.exit(1)


def main() -> None:
    if len(sys.argv) < 2:
        _err("Usage: openclaw-crm <command> [json_args]")

    cmd = sys.argv[1]
    args = json.loads(sys.argv[2]) if len(sys.argv) > 2 else {}
    mgr = CRMManager()

    if cmd == "summary":
        _out(mgr.pipeline_summary())
    elif cmd == "stale":
        _out(mgr.stale_deals())
    elif cmd == "overdue":
        _out(mgr.overdue_invoices())
    elif cmd == "add":
        _out(mgr.add_deal(**args))
    elif cmd == "move":
        _out(mgr.move_deal(args["client"], args["stage"]))
    elif cmd == "network":
        _out(mgr.network_tree(args.get("root")))
    elif cmd == "signals":
        _out(mgr.pending_signals())
    elif cmd == "promote":
        row = args.pop("row")
        _out(mgr.promote_signal(row, **args))
    elif cmd == "dismiss":
        _out(mgr.dismiss_signal(args["row"]))
    elif cmd == "record-signal":
        _out(mgr.record_signal(**args))
    else:
        _err(f"Unknown command: {cmd}")


if __name__ == "__main__":
    main()
