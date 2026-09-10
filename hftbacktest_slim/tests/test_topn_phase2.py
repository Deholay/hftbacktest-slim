from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hftbacktest_slim import (
    BBO_SCHEMA,
    TOP5_SCHEMA,
    CompactBuildConfig,
    CompactCacheError,
    CompactCacheStore,
    CompactSource,
    normalized_bbo_from_depth_columns,
    normalized_depth_from_depth_columns,
    validate_compact_schema,
)
from hftbacktest_slim.cache import builder
from hftbacktest_slim.cli import build_cache
from hftbacktest_slim.market_data.status import TWSE_TRIAL_STATUS_MASK


def _raw_table(*, price_only: bool = False, trial: bool = False) -> pa.Table:
    values: dict[str, list] = {
        "symbol": ["0050", "0050"],
        "exchtime": [100, 200],
        "localtime": [90, 210],
        "status": [TWSE_TRIAL_STATUS_MASK if trial else 0, 0],
        "last_price": [77.95, 78.05],
        "total_volume": [10, 20],
    }
    bid_prices = (
        [77.90, 77.95, 77.90, 77.85, None],
        [78.00, 78.05, 77.95, None, None],
    )
    ask_prices = (
        [78.05, 78.00, 78.05, None, None],
        [78.10, 78.05, None, None, None],
    )
    bid_qty = ([3.0, 2.0, 4.0, 1.0, None], [1.0, 2.0, 3.0, None, None])
    ask_qty = ([4.0, 2.0, 3.0, None, None], [1.0, 2.0, None, None, None])
    for level in range(1, 6):
        values[f"bid_price{level}"] = [row[level - 1] for row in bid_prices]
        values[f"ask_price{level}"] = [row[level - 1] for row in ask_prices]
        values[f"bid_volume{level}"] = (
            [None, None] if price_only else [row[level - 1] for row in bid_qty]
        )
        values[f"ask_volume{level}"] = (
            [None, None] if price_only else [row[level - 1] for row in ask_qty]
        )
    return pa.table(values)


def _store(tmp_path: Path, depth: int) -> CompactCacheStore:
    return CompactCacheStore(
        CompactBuildConfig(
            cache_root=tmp_path / "cache",
            depth_levels=depth,
            batch_rows=1,
            max_cache_bytes=1024**3,
            min_free_bytes=0,
        )
    )


@pytest.mark.parametrize("depth", [1, 2, 3, 4, 5])
def test_canonical_topn_normalization_all_depths_without_input_mutation(
    depth: int,
) -> None:
    prices = np.array(
        [
            [100.0, 101.0, 100.0, np.nan, 99.0],
            [77.90, 77.95, 78.00, 78.05, 0.0],
        ],
        dtype=np.float64,
    )
    quantities = np.array(
        [[3.0, 2.0, 4.0, 8.0, 1.0], [1.0, 2.0, 3.0, 4.0, -1.0]],
        dtype=np.float64,
    )
    original_prices = prices.copy()
    original_quantities = quantities.copy()
    bid_px, bid_qty = normalized_depth_from_depth_columns(
        prices, quantities, 1.0, 0.0, False, True, depth
    )
    ask_px, ask_qty = normalized_depth_from_depth_columns(
        prices, quantities, 1.0, 0.0, False, False, depth
    )

    assert bid_px.shape == bid_qty.shape == (2, depth)
    assert ask_px.shape == ask_qty.shape == (2, depth)
    assert bid_px.dtype == bid_qty.dtype == np.float64
    expected_bid = [101.0, 100.0, 99.0, np.nan, np.nan][:depth]
    expected_bid_qty = [2.0, 7.0, 1.0, np.nan, np.nan][:depth]
    np.testing.assert_equal(bid_px[0], expected_bid)
    np.testing.assert_equal(bid_qty[0], expected_bid_qty)
    np.testing.assert_equal(ask_px[0], [99.0, 100.0, 101.0, np.nan, np.nan][:depth])
    np.testing.assert_equal(ask_qty[0], [1.0, 7.0, 2.0, np.nan, np.nan][:depth])
    np.testing.assert_equal(bid_px[1], [78.05, 78.00, 77.95, 77.90, np.nan][:depth])
    np.testing.assert_equal(ask_px[1], [77.90, 77.95, 78.00, 78.05, np.nan][:depth])
    np.testing.assert_equal(prices, original_prices)
    np.testing.assert_equal(quantities, original_quantities)

    best_px, best_qty = normalized_bbo_from_depth_columns(
        prices, quantities, 1.0, 0.0, False, True
    )
    np.testing.assert_equal(best_px, bid_px[:, 0])
    np.testing.assert_equal(best_qty, bid_qty[:, 0])


def test_normalization_filters_invalid_quantities_and_scales_once_with_price_only() -> None:
    prices = np.array([[10.0, 11.0, 12.0, 13.0, 10.0]])
    quantities = np.array([[np.nan, 0.0, -1.0, np.inf, np.nan]])
    selected_px, selected_qty = normalized_depth_from_depth_columns(
        prices, quantities, 3.0, 1.5, True, False, 5
    )
    np.testing.assert_equal(selected_px, [[10.0, 13.0, np.nan, np.nan, np.nan]])
    np.testing.assert_equal(selected_qty, [[9.0, 4.5, np.nan, np.nan, np.nan]])


def test_normalization_empty_inputs_and_invalid_depths() -> None:
    empty = np.empty((0, 5), dtype=np.float64)
    prices, quantities = normalized_depth_from_depth_columns(
        empty, empty, 1.0, 0.0, False, True, 3
    )
    assert prices.shape == quantities.shape == (0, 3)
    for invalid in (True, 0, 6, 2.0, "2"):
        with pytest.raises(ValueError, match="integer from 1 through 5"):
            normalized_depth_from_depth_columns(
                empty, empty, 1.0, 0.0, False, True, invalid  # type: ignore[arg-type]
            )


def test_depth_one_builder_keeps_exact_bbo_schema_and_values(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    pq.write_table(_raw_table(), raw)
    store = _store(tmp_path, 1)
    store.build_date(
        "2026-03-02", [CompactSource("stock", (raw,), ("0050",))]
    )
    table = store.read_symbol("2026-03-02", "stock", "0050")
    assert table.schema.remove_metadata() == BBO_SCHEMA
    assert table["bid_px"].to_pylist() == [77.95, 78.05]
    assert table["bid_qty"].to_pylist() == [2.0, 2.0]
    assert table["ask_px"].to_pylist() == [78.00, 78.05]
    assert table["ask_qty"].to_pylist() == [2.0, 2.0]


@pytest.mark.parametrize("depth", [2, 3, 5])
def test_topn_cache_builds_exact_values_nulls_metadata_and_reuses(
    tmp_path: Path, depth: int
) -> None:
    raw = tmp_path / "daily.parquet"
    pq.write_table(_raw_table(trial=True), raw, row_group_size=1)
    store = _store(tmp_path, depth)
    source = CompactSource("stock", (raw,), ("0050", "9999"))
    batch_sizes: list[int] = []
    original_compact_batch = builder.compact_batch

    def observed_compact_batch(batch, *args, **kwargs):
        batch_sizes.append(batch.num_rows)
        return original_compact_batch(batch, *args, **kwargs)

    with patch.object(
        builder, "iter_source_batches", wraps=builder.iter_source_batches
    ) as scan, patch.object(builder, "compact_batch", side_effect=observed_compact_batch):
        cold = store.build_date("2026-03-02", [source])
        warm = store.build_date("2026-03-02", [source])

    assert scan.call_count == 1
    assert cold["build_invocation_scan_count"] == 1
    assert warm["build_invocation_scan_count"] == 0
    assert batch_sizes and max(batch_sizes) <= 1
    table = store.read_symbol("2026-03-02", "stock", "0050")
    assert table.schema.remove_metadata() == TOP5_SCHEMA
    metadata = validate_compact_schema(table.schema, expected_depth_levels=depth)
    assert metadata["profile"] == "top5"
    assert metadata["depth_levels"] == str(depth)
    assert table["tradable"].to_pylist() == [0, 1]
    assert table["bid_px_1"].to_pylist() == [77.95, 78.05]
    assert table["bid_qty_1"].to_pylist() == [2.0, 2.0]
    assert table["bid_px_2"].to_pylist() == [77.90, 78.00]
    assert table["bid_qty_2"].to_pylist() == [7.0, 1.0]
    for side in ("bid", "ask"):
        for level in range(1, 6):
            assert table[f"{side}_px_{level}"].is_null().to_pylist() == table[
                f"{side}_qty_{level}"
            ].is_null().to_pylist()
    if depth >= 3:
        assert table["ask_px_3"].to_pylist() == [None, None]
        assert table["ask_qty_3"].to_pylist() == [None, None]
    for level in range(depth + 1, 6):
        assert table[f"bid_px_{level}"].null_count == table.num_rows
        assert table[f"bid_qty_{level}"].null_count == table.num_rows
        assert table[f"ask_px_{level}"].null_count == table.num_rows
        assert table[f"ask_qty_{level}"].null_count == table.num_rows
    empty = store.read_symbol("2026-03-02", "stock", "9999")
    assert empty.num_rows == 0
    assert empty.schema.remove_metadata() == TOP5_SCHEMA
    validate_compact_schema(empty.schema, expected_depth_levels=depth)


def test_topn_stock_future_and_price_only_population(tmp_path: Path) -> None:
    stock = tmp_path / "stock.parquet"
    future = tmp_path / "future.parquet"
    pq.write_table(_raw_table(price_only=True), stock)
    pq.write_table(_raw_table(), future)
    store = _store(tmp_path, 3)
    manifest = store.build_date(
        "2026-03-02",
        [
            CompactSource(
                "stock", (stock,), ("0050",), price_only_depth_qty=1.5, volume_scale=2.0
            ),
            CompactSource("stock_future", (future,), ("0050",)),
        ],
    )
    assert manifest["build_invocation_scan_count"] == 2
    assert manifest["sources"]["stock"]["scan_count"] == 1
    assert manifest["sources"]["stock_future"]["scan_count"] == 1
    price_only = store.read_symbol("2026-03-02", "stock", "0050")
    assert price_only["bid_qty_1"].to_pylist() == [3.0, 3.0]
    assert price_only["bid_qty_2"].to_pylist() == [6.0, 3.0]
    future_table = store.read_symbol("2026-03-02", "stock_future", "0050")
    assert future_table["ask_px_1"].to_pylist() == [78.00, 78.05]


def test_different_topn_depths_are_separate_cache_misses(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    pq.write_table(_raw_table(), raw)
    source = CompactSource("stock", (raw,), ("0050",))
    depth2 = _store(tmp_path, 2)
    depth3 = _store(tmp_path, 3)
    assert depth2.build_date("2026-03-02", [source])["cache_state"] == "miss"
    assert depth3.build_date("2026-03-02", [source])["cache_state"] == "miss"
    assert depth2.date_path("2026-03-02") != depth3.date_path("2026-03-02")


def test_standalone_cache_cli_builds_topn_profile(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    pq.write_table(_raw_table(), raw)
    args = build_cache.parse_args(
        [
            "--date",
            "2026-03-02",
            "--cache-root",
            str(tmp_path / "cache"),
            "--stock-path",
            str(raw),
            "--spot-symbols",
            "0050",
            "--compact-depth-levels",
            "3",
            "--batch-rows",
            "1",
            "--max-gb",
            "1",
            "--min-free-gb",
            "0",
        ]
    )
    result = build_cache.run(args)
    assert result["cache_state"] == "miss"
    assert result["depth_levels"] == 3
    assert result["manifest"]["schema_version"] == "top5_v1"


def test_consolidation_rejects_mixed_part_schemas(tmp_path: Path) -> None:
    bbo = tmp_path / "bbo.arrow"
    top5 = tmp_path / "top5.arrow"
    builder.write_arrow(bbo, pa.Table.from_batches([], schema=BBO_SCHEMA), "none")
    builder.write_arrow(top5, pa.Table.from_batches([], schema=TOP5_SCHEMA), "none")
    with pytest.raises(CompactCacheError, match="mixed or incompatible"):
        builder.consolidate_symbol(
            tmp_path / "out.arrow",
            [bbo, top5],
            depth_levels=3,
            symbol="0050",
            source="stock",
            trade_date="2026-03-02",
            compression="none",
            base_latency_ns=0,
            non_tradable_rows=0,
        )


def test_topn_validation_failure_cleans_temp_and_does_not_publish(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    pq.write_table(_raw_table(), raw)
    store = _store(tmp_path, 2)
    with patch(
        "hftbacktest_slim.cache.store.validate_compact_partition",
        side_effect=CompactCacheError("forced Top-N validation failure"),
    ):
        with pytest.raises(CompactCacheError, match="forced Top-N"):
            store.build_date(
                "2026-03-02", [CompactSource("stock", (raw,), ("0050",))]
            )
    assert not store.date_path("2026-03-02").exists()
    assert not list(store.namespace_root.glob(".tmp-2026-03-02-*"))
