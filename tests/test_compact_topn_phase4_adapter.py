from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hftbacktest_slim import CompactBuildConfig, CompactCacheStore, CompactSource
from scripts.compact_hbt_adapter import (
    ADAPTER_VERSION,
    compact_to_reference_events,
    reference_npz_is_reusable,
    write_reference_npz_from_compact,
)
from scripts.tw_stock_data_to_npz import (
    BUY_EVENT,
    DEPTH_CLEAR_EVENT,
    DEPTH_SNAPSHOT_EVENT,
    SELL_EVENT,
    build_events_from_parquet_frame,
)


def _raw() -> pa.Table:
    values: dict[str, list] = {
        "symbol": ["0050", "0050", "0050"],
        "exchtime": [100, 100, 200],
        "localtime": [90, 90, 220],
        "status": [0, 1 << 23, 0],
        "last_price": [77.95, 77.95, 78.05],
        "total_volume": [10, 11, 13],
    }
    bid_prices = (
        [77.80, 77.90, 77.90, 77.85, None],
        [77.90, 77.85, None, None, None],
        [77.95, 78.00, 77.95, None, None],
    )
    ask_prices = (
        [78.10, 78.00, 78.00, 78.05, None],
        [78.00, 78.05, None, None, None],
        [78.15, 78.10, 78.05, None, None],
    )
    bid_qty = (
        [1.0, 2.0, 3.0, 4.0, None],
        [1.0, 2.0, None, None, None],
        [1.5, 2.0, 2.5, None, None],
    )
    ask_qty = (
        [1.0, 2.0, 3.0, 4.0, None],
        [1.0, 2.0, None, None, None],
        [1.5, 2.0, 2.5, None, None],
    )
    for level in range(1, 6):
        values[f"bid_price{level}"] = [row[level - 1] for row in bid_prices]
        values[f"ask_price{level}"] = [row[level - 1] for row in ask_prices]
        values[f"bid_volume{level}"] = [row[level - 1] for row in bid_qty]
        values[f"ask_volume{level}"] = [row[level - 1] for row in ask_qty]
    return pa.table(values)


def _build(tmp_path: Path, depth: int, *, volume_scale: float = 1.0):
    raw_path = tmp_path / f"raw-{depth}.parquet"
    pq.write_table(_raw(), raw_path)
    store = CompactCacheStore(
        CompactBuildConfig(
            cache_root=tmp_path / "cache",
            depth_levels=depth,
            batch_rows=2,
            max_cache_bytes=1024**3,
            min_free_bytes=0,
        )
    )
    manifest = store.build_date(
        "2026-03-02",
        [CompactSource("stock", (raw_path,), ("0050",), volume_scale=volume_scale)],
    )
    return store.read_symbol("2026-03-02", "stock", "0050"), manifest


@pytest.mark.parametrize("depth", [2, 3, 5])
def test_topn_reconstruction_matches_direct_converter_at_same_depth(
    tmp_path: Path, depth: int
) -> None:
    compact, _ = _build(tmp_path, depth)
    reconstructed, stats = compact_to_reference_events(
        compact, trade_date="2026-03-02"
    )
    direct_args = type(
        "Args",
        (),
        {
            "levels": depth,
            "timestamp_unit": "ns",
            "timezone": "Asia/Taipei",
            "date": "2026-03-02",
            "base_latency_ns": 0,
            "volume_scale": 1.0,
            "price_only_depth_qty": None,
            "trade_side": "infer",
            "no_trades": False,
            "no_depth": False,
            "qa_sample_rows": 1000,
            "source_kind": "stock",
        },
    )()
    direct, direct_stats = build_events_from_parquet_frame(
        pl.from_arrow(_raw()), direct_args
    )
    np.testing.assert_array_equal(reconstructed, direct)
    assert stats.depth_events == direct_stats.depth_events
    assert stats.trade_events == direct_stats.trade_events
    assert stats.non_tradable_rows == 1
    assert reconstructed.dtype == direct.dtype


def test_enabled_levels_map_exactly_and_disabled_levels_are_absent(tmp_path: Path) -> None:
    compact, _ = _build(tmp_path, 3)
    events, stats = compact_to_reference_events(
        compact.slice(0, 1), trade_date="2026-03-02", no_trades=True
    )
    snapshots = events[(events["ev"] & 0xFF) == DEPTH_SNAPSHOT_EVENT]
    assert stats.depth_events == 8
    bid = snapshots[(snapshots["ev"] & BUY_EVENT) != 0]
    ask = snapshots[(snapshots["ev"] & SELL_EVENT) != 0]
    np.testing.assert_allclose(bid["px"], [77.90, 77.85, 77.80])
    np.testing.assert_allclose(bid["qty"], [5.0, 4.0, 1.0])
    np.testing.assert_allclose(ask["px"], [78.00, 78.05, 78.10])
    np.testing.assert_allclose(ask["qty"], [5.0, 4.0, 1.0])
    assert np.count_nonzero((events["ev"] & 0xFF) == DEPTH_CLEAR_EVENT) == 2


def test_topn_quantities_are_not_scaled_twice(tmp_path: Path) -> None:
    compact, _ = _build(tmp_path, 3, volume_scale=10.0)
    events, _ = compact_to_reference_events(
        compact.slice(0, 1), trade_date="2026-03-02", no_trades=True
    )
    snapshots = events[(events["ev"] & 0xFF) == DEPTH_SNAPSHOT_EVENT]
    np.testing.assert_allclose(snapshots["qty"][:3], [50.0, 40.0, 10.0])
    with pytest.raises(ValueError, match="volume_scale must be 1.0"):
        compact_to_reference_events(
            compact, trade_date="2026-03-02", volume_scale=10.0
        )


def test_reference_npz_identity_reuse_and_interruption_safety(tmp_path: Path) -> None:
    compact, manifest = _build(tmp_path, 3)
    output = tmp_path / "profile=top5_v1" / "depth_levels=3" / "0050.npz"
    result = write_reference_npz_from_compact(
        compact,
        output,
        trade_date="2026-03-02",
        compact_identity_sha256=manifest["identity_sha256"],
        npz_compression="uncompressed",
    )
    assert result["adapter_version"] == ADAPTER_VERSION
    assert result["compact_schema_version"] == "top5_v1"
    assert result["compact_profile"] == "top5"
    assert result["depth_levels"] == 3
    reusable = dict(
        compact_schema_version="top5_v1",
        compact_profile="top5",
        depth_levels=3,
        compact_identity_sha256=manifest["identity_sha256"],
        trade_date="2026-03-02",
        npz_compression="uncompressed",
    )
    assert reference_npz_is_reusable(output, **reusable)
    assert not reference_npz_is_reusable(output, **(reusable | {"depth_levels": 2}))
    sidecar = output.with_suffix(".npz.compact.json")
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    for key in (
        "adapter_version",
        "compact_schema_version",
        "compact_profile",
        "depth_levels",
        "compact_identity_sha256",
        "trade_date",
        "base_latency_ns",
        "effective_quantity_scale",
        "trade_side_inference_policy",
        "no_trades",
        "npz_compression",
        "event_rows",
    ):
        assert key in payload
    sidecar.unlink()
    assert not reference_npz_is_reusable(output, **reusable)


def test_empty_topn_partition_reconstructs_empty_events(tmp_path: Path) -> None:
    compact, _ = _build(tmp_path, 5)
    empty = compact.slice(0, 0)
    events, stats = compact_to_reference_events(empty, trade_date="2026-03-02")
    assert len(events) == 0
    assert stats.input_rows == 0
