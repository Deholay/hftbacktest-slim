from __future__ import annotations

import threading
from types import SimpleNamespace

import pandas as pd
import pytest

from scripts.tw_market_status import (
    decode_taifex_status,
    decode_twse_status,
    expand_taifex_status_columns,
    expand_twse_status_columns,
)


def test_decode_twse_status_extracts_every_field() -> None:
    data_flag = 1 | (5 << 1) | (3 << 4) | (1 << 7)
    limit_flag = 2 | (1 << 2) | (3 << 4) | (2 << 6)
    data_status = 2 | (1 << 2) | (1 << 4) | (1 << 5) | (1 << 7)
    status = data_flag | (limit_flag << 8) | (data_status << 16)

    assert decode_twse_status(status) == {
        "raw_status": status,
        "data_flag": data_flag,
        "disclosure_tag": 1,
        "ask_level": 5,
        "bid_level": 3,
        "is_traded": 1,
        "limit_flag": limit_flag,
        "price_tag": 2,
        "best_ask": 1,
        "best_bid": 3,
        "limit_tag": 2,
        "data_status": data_status,
        "reserve": 2,
        "close_tag": 1,
        "open_tag": 0,
        "match_tag": 1,
        "close_delay_tag": 1,
        "open_delay_tag": 0,
        "trial_status_tag": 1,
    }


def test_decode_taifex_status_extracts_each_byte() -> None:
    status = 0x44332211
    assert decode_taifex_status(status) == {
        "raw_status": status,
        "build_type": 0x11,
        "match_flag": 0x22,
        "orderbook_action": 0x33,
        "continuous_flag": 0x44,
    }


def test_expand_twse_status_columns_matches_scalar_decoder() -> None:
    statuses = [0, 0x00D6B9EB]
    expanded = expand_twse_status_columns(pd.DataFrame({"status": statuses}))

    for row, status in zip(expanded.to_dict("records"), statuses):
        decoded = decode_twse_status(status)
        for key, value in decoded.items():
            assert row[f"status_{key}"] == value


def test_expand_taifex_status_columns_supports_empty_prefix() -> None:
    status = 0x44332211
    expanded = expand_taifex_status_columns(
        pd.DataFrame({"status": [status]}),
        prefix="",
    )

    assert expanded.loc[0, "raw_status"] == status
    assert expanded.loc[0, "build_type"] == 0x11
    assert expanded.loc[0, "match_flag"] == 0x22
    assert expanded.loc[0, "orderbook_action"] == 0x33
    assert expanded.loc[0, "continuous_flag"] == 0x44


def test_historical_replay_uses_market_specific_status_layouts(tmp_path) -> None:
    from future_spot.arbitrage.models import HistoricalSourceConfig
    from future_spot.arbitrage.models import Quote
    from future_spot.arbitrage.providers import HistoricalParquetReplayProvider

    stock_path = tmp_path / "stock.parquet"
    pd.DataFrame(
        {
            "symbol": ["0050", "0050"],
            "exchtime": [1_700_000_000_000_000_000, 1_700_000_000_001_000_000],
            "bid": [77.9, 77.95],
            "ask": [77.95, 78.0],
            "status": [1 << 23, 1 << 22],
        }
    ).to_parquet(stock_path)
    loader = HistoricalParquetReplayProvider.new_loader()
    stock_events = loader._load_source_events_uncached(
        "stock",
        HistoricalSourceConfig(
            path=str(stock_path),
            timestamp_col="exchtime",
            bid_col="bid",
            ask_col="ask",
            bid_size_col=None,
            ask_size_col=None,
            status_col="status",
            filter_trial_status=True,
        ),
        {"0050"},
    )

    assert len(stock_events) == 2
    assert stock_events[0].quote.raw["status_trial_status_tag"] == 1
    assert stock_events[1].quote.raw["status_open_delay_tag"] == 1
    assert stock_events[1].quote.raw["status_trial_status_tag"] == 0

    provider = HistoricalParquetReplayProvider.new_loader()
    provider._lock = threading.Lock()
    provider._stock_quotes = {"0050": stock_events[0].quote}
    provider._future_quotes = {"TXFA6": Quote("TXFA6", 100.0, 101.0)}
    provider._active_stock_quotes = None
    provider._active_future_quotes = None
    provider._active_quote_update = None
    provider.config = SimpleNamespace(
        historical=SimpleNamespace(stock=SimpleNamespace(filter_trial_status=True))
    )
    with pytest.raises(RuntimeError, match="stock:0050"):
        provider.get_pair_market(
            SimpleNamespace(spot_symbol="0050", future_symbol="TXFA6")
        )

    future_path = tmp_path / "future.parquet"
    pd.DataFrame(
        {
            "symbol": ["TXFA6"],
            "exchtime": [1_700_000_000_000_000_000],
            "bid": [100.0],
            "ask": [101.0],
            "status": [0x44332211],
        }
    ).to_parquet(future_path)
    future_events = loader._load_source_events_uncached(
        "future",
        HistoricalSourceConfig(
            path=str(future_path),
            timestamp_col="exchtime",
            bid_col="bid",
            ask_col="ask",
            bid_size_col=None,
            ask_size_col=None,
            status_col="status",
            filter_trial_status=True,
        ),
        {"TXFA6"},
    )

    assert len(future_events) == 1
    raw = future_events[0].quote.raw
    assert raw["status_build_type"] == 0x11
    assert raw["status_match_flag"] == 0x22
    assert raw["status_orderbook_action"] == 0x33
    assert raw["status_continuous_flag"] == 0x44
    assert "status_trial_status_tag" not in raw
