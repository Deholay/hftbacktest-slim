#!/usr/bin/env python3
"""Compare compact selected-depth reconstruction with direct source conversion."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))
from hftbacktest_slim import (
    CompactBuildConfig,
    CompactCacheStore,
)
from scripts.compact_hbt_adapter import compact_to_reference_events
from scripts.tw_stock_data_to_npz import (
    build_events_from_parquet_frame,
    symbol_filter_values,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--raw-file", type=Path, required=True)
    parser.add_argument("--depth-levels", type=int, choices=range(1, 6), default=1)
    parser.add_argument("--volume-scale", type=float, default=1.0)
    parser.add_argument("--price-only-depth-qty", type=float)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    store = CompactCacheStore(
        CompactBuildConfig(
            cache_root=args.cache_root,
            depth_levels=args.depth_levels,
            max_cache_bytes=2**63 - 1,
            min_free_bytes=0,
        )
    )
    compact = store.read_symbol(args.date, args.source, args.symbol)
    compact_events, _ = compact_to_reference_events(compact, trade_date=args.date)

    schema = pl.scan_parquet(args.raw_file).collect_schema().names()
    symbol_column = "symbol" if "symbol" in schema else "symbol_id"
    raw = (
        pl.scan_parquet(args.raw_file)
        .filter(pl.col(symbol_column).cast(pl.Utf8).is_in(symbol_filter_values(args.symbol)))
        .collect()
    )
    converter_args = argparse.Namespace(
        levels=args.depth_levels,
        timestamp_unit="auto",
        timezone="Asia/Taipei",
        date=args.date,
        base_latency_ns=0,
        volume_scale=args.volume_scale,
        price_only_depth_qty=args.price_only_depth_qty,
        trade_side="infer",
        no_trades=False,
        no_depth=False,
        qa_sample_rows=1000,
        source_kind=args.source,
    )
    direct_events, _ = build_events_from_parquet_frame(raw, converter_args)
    equal = np.array_equal(compact_events, direct_events)
    overlap = min(len(direct_events), len(compact_events))
    row_mismatches = 0
    field_mismatches: dict[str, int] = {}
    if overlap:
        row_different = np.zeros(overlap, dtype=bool)
        for name in direct_events.dtype.names or ():
            left = direct_events[name][:overlap]
            right = compact_events[name][:overlap]
            if left.dtype.kind == "f":
                different = ~((left == right) | (np.isnan(left) & np.isnan(right)))
            else:
                different = left != right
            count = int(np.count_nonzero(different))
            if count:
                field_mismatches[name] = count
                row_different |= different
        row_mismatches = int(np.count_nonzero(row_different))
    event_mismatch_count = row_mismatches + abs(len(direct_events) - len(compact_events))
    payload = {
        "date": args.date,
        "source": args.source,
        "symbol": args.symbol,
        "depth_levels": args.depth_levels,
        "compact_profile": "bbo" if args.depth_levels == 1 else "top5",
        "compact_schema_version": "bbo_v2" if args.depth_levels == 1 else "top5_v1",
        "source_rows": raw.height,
        "compact_rows": compact.num_rows,
        "direct_event_rows": len(direct_events),
        "compact_event_rows": len(compact_events),
        "events_equal": equal,
        "event_mismatch_count": event_mismatch_count,
        "field_mismatch_counts": field_mismatches,
        "direct_sha256": hashlib.sha256(direct_events.tobytes()).hexdigest(),
        "compact_sha256": hashlib.sha256(compact_events.tobytes()).hexdigest(),
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if equal else 1


if __name__ == "__main__":
    raise SystemExit(main())
