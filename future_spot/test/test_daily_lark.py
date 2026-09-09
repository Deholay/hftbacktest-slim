from __future__ import annotations

import json
from datetime import date
from pathlib import Path
import subprocess

import pytest

from future_spot.arbitrage import daily_lark


def _write_calendar(path: Path) -> None:
    path.write_text(
        "trade_dates,NDate,LDate\n"
        "2026-09-04,2026-09-07,2026-09-03\n"
        "2026-09-07,2026-09-08,2026-09-04\n"
        "2026-09-08,2026-09-09,2026-09-07\n",
        encoding="utf-8",
    )


def test_choose_trade_date_uses_latest_eligible_calendar_date(tmp_path: Path) -> None:
    calendar = tmp_path / "Calendar.csv"
    _write_calendar(calendar)

    assert daily_lark.choose_trade_date(
        calendar,
        None,
        today=date(2026, 9, 8),
        excluded_dates=("2026-09-08",),
    ) == "2026-09-07"
    assert daily_lark.choose_trade_date(
        calendar,
        "2026-09-04",
        excluded_dates=(),
    ) == "2026-09-04"


def test_build_backtest_command_enforces_daily_date_and_output(tmp_path: Path) -> None:
    output = tmp_path / "result"
    command = daily_lark.build_backtest_command(
        python="python-test",
        trade_date="2026-09-08",
        output_dir=output,
        extra_args=("--report-mode", "full", "--workers", "1"),
    )

    assert command[-8:] == [
        "--start-date",
        "2026-09-08",
        "--end-date",
        "2026-09-08",
        "--report-mode",
        "daily",
        "--output-dir",
        str(output),
    ]
    assert command[0] == "python-test"
    assert "--workers" in command


def test_success_message_keeps_full_raw_ticks() -> None:
    rows = [
        {
            "timestamp_tw": "2026-09-08 09:00:02.265303+08:00",
            "signal": "ENTER_LONG_SPOT_SHORT_FUTURE",
            "spot_bid": "312.0",
            "spot_ask": "313.0",
            "future_bid": "315.0",
            "future_ask": "320.0",
            "spot_tick_exch_timestamp": "1788829202263735000",
            "future_tick_exch_timestamp": "1788829202263000000",
        }
    ]

    message = daily_lark.format_success_message(
        "2026-09-08",
        rows,
        Path("/mnt/z/hftbacktest_daily_reports/20260908/daily_backtest_summary.csv"),
        max_rows=10,
    )

    assert "1788829202263735000" in message
    assert "1788829202263000000" in message
    assert "Z:\\hftbacktest_daily_reports\\20260908\\daily_backtest_summary.csv" in message


def test_read_daily_summary_rejects_an_old_schema(tmp_path: Path) -> None:
    csv_path = tmp_path / "daily_backtest_summary.csv"
    csv_path.write_text("run_key,timestamp_tw\nx,y\n", encoding="utf-8")

    with pytest.raises(daily_lark.DailyLarkError, match="missing required columns"):
        daily_lark.read_daily_summary(csv_path)


def test_send_lark_text_posts_signed_json(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self, _limit: int) -> bytes:
            return b'{"StatusCode": 0, "StatusMessage": "success"}'

    def fake_urlopen(http_request, *, timeout: float):
        captured["request"] = http_request
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(daily_lark.request, "urlopen", fake_urlopen)
    daily_lark.send_lark_text(
        "https://open.larksuite.com/open-apis/bot/v2/hook/test-id",
        "test-secret",
        "測試訊息",
        timestamp=1234567890,
    )

    http_request = captured["request"]
    payload = json.loads(http_request.data.decode("utf-8"))
    assert payload == {
        "timestamp": "1234567890",
        "sign": daily_lark.lark_signature("1234567890", "test-secret"),
        "msg_type": "text",
        "content": {"text": "測試訊息"},
    }
    assert captured["timeout"] == 20.0


def test_main_runs_backtest_then_notifies_without_putting_secret_in_child_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = tmp_path / "Calendar.csv"
    output_root = tmp_path / "output"
    _write_calendar(calendar)
    captured: dict[str, object] = {}

    def fake_run(command, *, cwd: Path, check: bool):
        captured["command"] = command
        captured["cwd"] = cwd
        captured["check"] = check
        output_dir = Path(command[command.index("--output-dir") + 1])
        output_dir.mkdir(parents=True)
        (output_dir / "daily_backtest_summary.csv").write_text(
            "run_key,timestamp_tw,signal,spot_ask,spot_bid,future_ask,future_bid,"
            "spot_tick_exch_timestamp,future_tick_exch_timestamp\n",
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0)

    def fake_send(url: str, secret: str, text: str) -> None:
        captured["url"] = url
        captured["secret"] = secret
        captured["text"] = text

    monkeypatch.setattr(daily_lark.subprocess, "run", fake_run)
    monkeypatch.setattr(daily_lark, "send_lark_text", fake_send)

    result = daily_lark.main(
        [
            "--trade-date",
            "2026-09-08",
            "--calendar",
            str(calendar),
            "--output-root",
            str(output_root),
            "--webhook-url",
            "https://open.larksuite.com/open-apis/bot/v2/hook/test-id",
            "--webhook-secret",
            "test-secret",
        ]
    )

    assert result == 0
    assert "test-secret" not in captured["command"]
    assert captured["secret"] == "test-secret"
    assert "FILLED 筆數：0" in captured["text"]
