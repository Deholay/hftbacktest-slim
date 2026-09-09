from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from future_spot.test import run_full_backtest
from future_spot.test.backtest_pipeline import run_backtest_pipeline


def test_daily_report_pipeline_writes_only_compact_filled_csv(tmp_path: Path) -> None:
    spot_tick_ts = 1_788_829_202_263_735_000
    future_tick_ts = 1_788_829_202_263_000_000
    args = SimpleNamespace(
        output_dir=tmp_path,
        report_mode="daily",
        start_date="2026-09-08",
        end_date="2026-09-08",
        calendar=Path("Calendar.csv"),
        excluded_dates=(),
    )
    trades = pd.DataFrame(
        {
            "run_key": ["2026-09-08::2492_HBFJ6", "ignored"],
            "timestamp_tw": ["2026-09-08 09:00:02.265303+08:00", "later"],
            "signal": ["ENTER_LONG_SPOT_SHORT_FUTURE", "EXIT"],
            "status": ["FILLED", "RISK_SKIP"],
            "spot_ask": [313.0, 311.0],
            "spot_bid": [312.0, 310.5],
            "future_ask": [320.0, 312.5],
            "future_bid": [315.0, 309.5],
            "spot_tick_exch_timestamp": pd.Series(
                [spot_tick_ts, pd.NA], dtype="Int64"
            ),
            "future_tick_exch_timestamp": pd.Series(
                [future_tick_ts, pd.NA], dtype="Int64"
            ),
        }
    )
    outputs = SimpleNamespace(
        records=[SimpleNamespace(run_key="2026-09-08::2492_HBFJ6")],
        event_paths={},
        pair_results={},
        summary=pd.DataFrame(),
        trades=trades,
        market=pd.DataFrame(),
        latency=pd.DataFrame(),
        run_errors=pd.DataFrame(),
        conversion_status=pd.DataFrame(),
        settings=pd.DataFrame(),
        position_carry_status=pd.DataFrame(),
        stage_timings=pd.DataFrame(),
        daily_partitions=False,
        cache_hit=False,
    )

    with (
        patch(
            "future_spot.test.backtest_pipeline.prepare_args",
            side_effect=lambda value: value,
        ),
        patch(
            "future_spot.test.backtest_pipeline.daily_pipeline.select_trade_dates",
            return_value=["2026-09-08"],
        ),
        patch(
            "future_spot.test.backtest_pipeline.daily_pipeline.build_daily_pair_records",
            return_value=(outputs.records, pd.DataFrame()),
        ),
        patch(
            "future_spot.test.backtest_pipeline.hbt_pipeline.execute_hbt_runs",
            return_value=outputs,
        ),
    ):
        artifacts = run_backtest_pipeline(args)

    output = tmp_path / "daily_backtest_summary.csv"
    assert [path.name for path in tmp_path.iterdir()] == [output.name]
    assert artifacts.frame("daily_backtest_summary")["run_key"].tolist() == [
        "2026-09-08::2492_HBFJ6"
    ]
    text = output.read_text(encoding="utf-8-sig")
    assert str(spot_tick_ts) in text
    assert str(future_tick_ts) in text
    assert "e+" not in text.lower()


def test_daily_one_command_runner_skips_report_tables_and_figures(tmp_path: Path) -> None:
    args = SimpleNamespace(report_mode="daily", log_level="INFO")
    artifacts = SimpleNamespace(
        trade_dates=["2026-09-08"],
        records=[object()],
        output_dir=tmp_path,
    )

    with (
        patch("future_spot.test.run_full_backtest.parse_args", return_value=args),
        patch(
            "future_spot.test.run_full_backtest.prepare_args",
            side_effect=lambda value: value,
        ),
        patch(
            "future_spot.test.run_full_backtest.run_backtest_pipeline",
            return_value=artifacts,
        ),
        patch("future_spot.test.run_full_backtest.build_report_tables") as reports,
        patch("future_spot.test.run_full_backtest.save_report_plots") as plots,
    ):
        assert run_full_backtest.main() == 0

    reports.assert_not_called()
    plots.assert_not_called()
