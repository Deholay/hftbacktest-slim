"""CLI entrypoint for the daily opportunity report and Lark notification."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent
for path in (PROJECT_ROOT, WORKSPACE_ROOT):
    text = str(path)
    if text not in sys.path:
        sys.path.insert(0, text)

from arbitrage.daily_lark import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
