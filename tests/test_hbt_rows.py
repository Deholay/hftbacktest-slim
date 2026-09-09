from __future__ import annotations

import pandas as pd

from future_spot.arbitrage.hbt_rows import execution_rows_frame
from scripts.daily_result_store import DailyResultStore
from scripts.io_utils import write_csv


def test_execution_rows_preserve_nullable_nanosecond_timestamps_in_csv(tmp_path) -> None:
    first_ts = 1_788_829_202_225_303_000
    second_ts = 1_788_829_202_265_303_000
    rows = [
        {"timestamp": first_ts, "status": "RISK_SKIP"},
        {
            "timestamp": second_ts,
            "status": "FILLED",
            "signal_timestamp": first_ts,
            "completion_timestamp": second_ts,
            "spot_tick_exch_timestamp": second_ts - 1_568_000,
            "spot_tick_local_timestamp": second_ts - 1_568_000,
            "future_tick_exch_timestamp": second_ts - 2_303_000,
            "future_tick_local_timestamp": second_ts - 2_303_000,
            "first_local_timestamp": first_ts,
            "first_exch_timestamp": first_ts,
            "first_order_req_local_ts": first_ts,
            "first_order_exch_ts": first_ts,
            "first_order_resp_local_ts": first_ts,
            "second_local_timestamp": second_ts,
            "second_exch_timestamp": second_ts,
            "second_order_req_local_ts": second_ts,
            "second_order_exch_ts": second_ts,
            "second_order_resp_local_ts": second_ts,
        },
    ]

    frame = execution_rows_frame(rows)

    timestamp_columns = [
        name
        for name in frame.columns
        if name == "timestamp" or name.endswith("_timestamp") or name.endswith("_ts")
    ]
    assert timestamp_columns
    assert all(frame[name].dtype == pd.Int64Dtype() for name in timestamp_columns)
    assert frame.loc[1, "first_local_timestamp"] == first_ts
    assert frame.loc[1, "second_local_timestamp"] == second_ts

    output = tmp_path / "trades_all_daily_pairs.csv"
    write_csv(frame, output)
    csv_text = output.read_text(encoding="utf-8-sig")

    assert str(first_ts) in csv_text
    assert str(second_ts) in csv_text
    assert "e+" not in csv_text.lower()

    store = DailyResultStore(tmp_path / "core")
    store.publish(
        "2026-09-08",
        {"trades": frame},
        input_identity={"test": "exact timestamps"},
        carry_in={},
        carry_out={},
        run_keys=[],
    )
    streamed_output = store.write_table_csv(
        ["2026-09-08"],
        "trades",
        tmp_path / "streamed_trades_all_daily_pairs.csv",
    )
    streamed_text = streamed_output.read_text(encoding="utf-8-sig")

    assert str(first_ts) in streamed_text
    assert str(second_ts) in streamed_text
    assert "e+" not in streamed_text.lower()
