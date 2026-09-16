from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from future_spot.arbitrage.hbt_backtest import HbtPairBacktester
from future_spot.arbitrage.hbt_types import HbtPairBacktestConfig
from future_spot.arbitrage.models import PairConfig
from hftbacktest_slim import CompactBuildConfig, CompactCacheStore, CompactSource
from hftbacktest_slim.engine.arrow_reader import SLIM_ROW_DTYPE, read_rows
from scripts.compact_hbt_adapter import compact_to_reference_events
from scripts.hbt_types import HbtAssetConfig
from scripts.tw_stock_data_to_npz import build_events_from_parquet_frame


TRADE_DATE = "2026-03-02"
SEMANTIC_STATS = (
    "input_rows",
    "converted_rows",
    "skipped_symbol_rows",
    "skipped_status_rows",
    "skipped_time_rows",
    "non_tradable_rows",
    "raw_events",
    "output_events",
    "depth_events",
    "trade_events",
    "opening_jump_qty",
    "first_exch_ts",
    "last_exch_ts",
    "min_feed_latency",
    "max_feed_latency",
    "best_bid_mismatches",
    "best_ask_mismatches",
    "trade_qty_mismatches",
    "qa_rows_checked",
)


def _semantic_fixture() -> pa.Table:
    """Rows exercising the complete selected-depth conversion contract."""

    values: dict[str, list] = {
        "symbol": ["0050"] * 7,
        # Equal exchange/local times and opposing timestamp orders exercise
        # stable source_seq tie-breaking and per-symbol latency correction.
        "exchtime": [100, 100, 200, 300, 400, 500, 600],
        "localtime": [90, 90, 220, 290, 400, 490, 620],
        "status": [0, 0, 0, 0, 1 << 23, 0, 0],
        "last_price": [77.95, 78.00, 78.05, 78.00, 78.00, 78.05, 78.10],
        "total_volume": [10, 12, 15, 15, 18, 21, 25],
    }
    bid_prices = (
        (77.80, 77.95, 77.90, 77.90, 77.85),  # out of order + repeat
        (77.95, 77.90, 77.85, 77.80, 77.75),  # all five
        (78.00, 77.95, None, None, None),  # trailing missing
        (78.05, 78.00, None, None, None),  # crossed against ask
        (78.00, 77.95, None, None, None),  # non-tradable clear
        (78.00, 77.95, 77.90, None, None),
        (None, None, None, None, None),  # ask-only book
    )
    ask_prices = (
        (78.10, 78.00, 78.05, 78.00, 78.15),
        (78.00, 78.05, 78.10, 78.15, 78.20),
        (78.05, 78.10, None, None, None),
        (78.00, 78.05, None, None, None),  # locked/crossed row
        (78.05, 78.10, None, None, None),
        (None, None, None, None, None),  # bid-only book
        (78.10, 78.15, 78.20, None, None),
    )
    bid_qty = (
        (1.0, 2.0, 3.0, 4.0, 5.0),
        (1.0, 2.0, 3.0, 4.0, 5.0),
        (1.5, 2.5, None, None, None),
        (1.0, 1.0, None, None, None),
        (1.0, 1.0, None, None, None),
        (2.0, 3.0, 4.0, None, None),
        (None, None, None, None, None),
    )
    ask_qty = (
        (1.0, 2.0, 3.0, 4.0, 5.0),
        (1.0, 2.0, 3.0, 4.0, 5.0),
        (1.5, 2.5, None, None, None),
        (1.0, 1.0, None, None, None),
        (1.0, 1.0, None, None, None),
        (None, None, None, None, None),
        (2.0, 3.0, 4.0, None, None),
    )
    for level in range(1, 6):
        values[f"bid_price{level}"] = [row[level - 1] for row in bid_prices]
        values[f"bid_volume{level}"] = [row[level - 1] for row in bid_qty]
        values[f"ask_price{level}"] = [row[level - 1] for row in ask_prices]
        values[f"ask_volume{level}"] = [row[level - 1] for row in ask_qty]
    return pa.table(values)


def _converter_args(depth: int, *, price_only_depth_qty: float | None = None):
    return argparse.Namespace(
        levels=depth,
        timestamp_unit="ns",
        timezone="Asia/Taipei",
        date=TRADE_DATE,
        base_latency_ns=0,
        volume_scale=1.0,
        price_only_depth_qty=price_only_depth_qty,
        trade_side="infer",
        no_trades=False,
        no_depth=False,
        qa_sample_rows=1000,
        source_kind="stock",
    )


def _store(tmp_path: Path, depth: int) -> CompactCacheStore:
    return CompactCacheStore(
        CompactBuildConfig(
            cache_root=tmp_path / "cache",
            depth_levels=depth,
            batch_rows=2,
            max_cache_bytes=1024**3,
            min_free_bytes=0,
        )
    )


@pytest.mark.parametrize("depth", [1, 2, 3, 4, 5])
def test_direct_and_compact_reference_events_are_exact_at_every_depth(
    tmp_path: Path, depth: int
) -> None:
    raw = _semantic_fixture()
    raw_path = tmp_path / "raw.parquet"
    pq.write_table(raw, raw_path, row_group_size=2)
    store = _store(tmp_path, depth)
    manifest = store.build_date(
        TRADE_DATE, [CompactSource("stock", (raw_path,), ("0050",))]
    )
    compact = store.read_symbol(TRADE_DATE, "stock", "0050")
    actual, actual_stats = compact_to_reference_events(compact, trade_date=TRADE_DATE)
    expected, expected_stats = build_events_from_parquet_frame(
        pl.from_arrow(raw), _converter_args(depth)
    )

    np.testing.assert_array_equal(actual, expected)
    assert actual.dtype == expected.dtype
    assert {
        key: asdict(actual_stats)[key] for key in SEMANTIC_STATS
    } == {key: asdict(expected_stats)[key] for key in SEMANTIC_STATS}
    assert manifest["build_invocation_scan_count"] == 1
    assert manifest["sources"]["stock"]["scan_count"] == 1
    assert manifest["sources"]["stock"]["output_rows"] == raw.num_rows


def test_price_only_reference_parity_and_decimal_prices_at_every_depth(
    tmp_path: Path,
) -> None:
    raw = _semantic_fixture()
    values = raw.to_pydict()
    for side in ("bid", "ask"):
        for level in range(1, 6):
            values[f"{side}_volume{level}"] = [None] * raw.num_rows
    price_only = pa.table(values)
    raw_path = tmp_path / "price-only.parquet"
    pq.write_table(price_only, raw_path)

    for depth in range(1, 6):
        store = _store(tmp_path, depth)
        store.build_date(
            TRADE_DATE,
            [
                CompactSource(
                    "stock",
                    (raw_path,),
                    ("0050",),
                    price_only_depth_qty=1.0,
                )
            ],
        )
        compact = store.read_symbol(TRADE_DATE, "stock", "0050")
        actual, _ = compact_to_reference_events(compact, trade_date=TRADE_DATE)
        expected, _ = build_events_from_parquet_frame(
            pl.from_arrow(price_only),
            _converter_args(depth, price_only_depth_qty=1.0),
        )
        np.testing.assert_array_equal(actual, expected)
        prices = actual["px"][np.isfinite(actual["px"]) & (actual["px"] > 0)]
        assert any(np.isclose(prices, 77.95))
        assert any(np.isclose(prices, 78.00))
        assert any(np.isclose(prices, 78.05))
        if depth == 5:
            assert any(np.isclose(prices, 77.90))


def _runtime_raw(symbol: str, *, future: bool) -> pa.Table:
    values: dict[str, list] = {
        "symbol": [symbol] * 4,
        "exchtime": [100, 100, 120, 140],
        "localtime": [90, 110, 130, 150],
        "status": [0, 0, 0, 0],
        "last_price": ([110.0] * 4 if future else [100.0] * 4),
        "total_volume": [1, 2, 3, 4],
    }
    best_bid = 110.0 if future else 99.0
    best_ask = 111.0 if future else 100.0
    for level in range(1, 6):
        values[f"bid_price{level}"] = [best_bid - level + 1] * 4
        values[f"ask_price{level}"] = [best_ask + level - 1] * 4
        # Level one passes the strategy's minimum-size guard but remains much
        # smaller than the 1,000-share spot order.
        values[f"bid_volume{level}"] = [float(level)] * 4
        values[f"ask_volume{level}"] = [float(level)] * 4
    return pa.table(values)


def _runtime_paths(tmp_path: Path, depth: int) -> tuple[Path, Path]:
    stock_path = tmp_path / "stock.parquet"
    future_path = tmp_path / "future.parquet"
    if not stock_path.exists():
        pq.write_table(_runtime_raw("S", future=False), stock_path, row_group_size=2)
        pq.write_table(_runtime_raw("F", future=True), future_path, row_group_size=2)
    store = _store(tmp_path, depth)
    store.build_date(
        TRADE_DATE,
        [
            CompactSource("stock", (stock_path,), ("S",)),
            CompactSource("stock_future", (future_path,), ("F",)),
        ],
    )
    date = store.date_path(TRADE_DATE)
    return date / "source=stock" / "S.arrow", date / "source=stock_future" / "F.arrow"


def _pair_config(spot: Path, future: Path, *, strategy_clock: str):
    pair = PairConfig(
        name="S_F",
        spot_symbol="S",
        future_symbol="F",
        spot_shares_per_pair=1000,
        future_shares_per_pair=1000,
        spot_order_qty=2000,
        future_order_qty=2,
        future_pnl_multiplier=1000,
        entry_threshold_pct=0.01,
        exit_threshold_pct=0.0,
        stop_loss_pct=-1.0,
        min_effective_tick_multiple=0.0,
        spot_tick_size=1.0,
        future_tick_size=1.0,
        stock_min_bid_size=1,
        stock_min_ask_size=1,
        first_leg_time_in_force="FOK",
        second_leg_time_in_force="IOC",
        flatten_first_leg_time_in_force="IOC",
    )
    return HbtPairBacktestConfig(
        pair=pair,
        spot=HbtAssetConfig(
            "S",
            spot,
            "stock",
            1000.0,
            tick_size=1.0,
            feed_latency_offset_ns=1,
            order_entry_latency_ns=3,
            order_response_latency_ns=4,
        ),
        future=HbtAssetConfig(
            "F",
            future,
            "future",
            1000.0,
            tick_size=1.0,
            feed_latency_offset_ns=2,
            order_entry_latency_ns=2,
            order_response_latency_ns=3,
        ),
        execution_engine="slim",
        strategy_engine="python",
        strategy_clock=strategy_clock,
        first_leg="future",
        step_ns=10,
        response_timeout_ns=50,
        max_steps=5,
        max_trades=1,
        post_first_feed_wait="none",
    )


@pytest.mark.parametrize("strategy_clock", ["step", "event"])
def test_slim_result_tables_are_identical_across_bbo_and_topn_profiles(
    tmp_path: Path, strategy_clock: str
) -> None:
    baseline = None
    baseline_native = None
    for depth in (1, 2, 3, 5):
        spot, future = _runtime_paths(tmp_path, depth)
        loaded = read_rows(spot)
        assert loaded.rows.dtype == SLIM_ROW_DTYPE
        assert loaded.local_timestamp_adjustment_ns == 10
        if baseline_native is None:
            baseline_native = loaded.rows.copy()
        else:
            np.testing.assert_array_equal(loaded.rows, baseline_native)

        backtester = HbtPairBacktester(
            _pair_config(spot, future, strategy_clock=strategy_clock)
        )
        trades, summary = backtester.run()
        result = (
            trades.reset_index(drop=True),
            summary.reset_index(drop=True),
            backtester.market_frame().reset_index(drop=True),
            backtester.latency_frame().reset_index(drop=True),
        )
        assert result[0].loc[0, "first_exec_qty"] == pytest.approx(2.0)
        assert result[0].loc[0, "second_exec_qty"] == pytest.approx(2.0)
        if baseline is None:
            baseline = result
        else:
            for actual, expected in zip(result, baseline):
                pd.testing.assert_frame_equal(actual, expected, check_exact=True)


def test_warm_reuse_is_zero_scan_for_every_depth(tmp_path: Path) -> None:
    raw_path = tmp_path / "raw.parquet"
    pq.write_table(_semantic_fixture(), raw_path)
    source = CompactSource("stock", (raw_path,), ("0050",))
    for depth in range(1, 6):
        store = _store(tmp_path, depth)
        cold = store.build_date(TRADE_DATE, [source])
        warm = store.build_date(TRADE_DATE, [source])
        assert cold["cache_state"] == "miss"
        assert cold["build_invocation_scan_count"] == 1
        assert warm["cache_state"] == "hit"
        assert warm["build_invocation_scan_count"] == 0
