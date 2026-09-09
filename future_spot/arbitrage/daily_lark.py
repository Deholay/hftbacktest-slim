"""Run one notebook-profile backtest date and notify a Lark custom bot."""

from __future__ import annotations

import argparse
import base64
import csv
from datetime import date, datetime
import hashlib
import hmac
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Iterable, Mapping, Sequence
from urllib import error, parse, request
from zoneinfo import ZoneInfo

from .full_market_runner import DAILY_BACKTEST_SUMMARY_COLUMNS, KNOWN_BAD_TRADE_DATES


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent
BACKTEST_ENTRYPOINT = PROJECT_ROOT / "test" / "run_full_backtest.py"
DEFAULT_CALENDAR = PROJECT_ROOT / "Calendar.csv"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "output" / "daily_lark"
DEFAULT_COMPACT_CACHE_ROOT = WORKSPACE_ROOT / "notebooks" / "output" / "compact_bbo_v2"

# Keep market-data, matching, clock, latency, and capital settings aligned with
# notebooks/hbt_pair_backtest_visualization.ipynb. The notification workflow
# intentionally selects daily reporting and submits the second leg immediately
# after the first response without waiting for another feed update.
DAILY_LARK_BACKTEST_ARGS = (
    "--engine",
    "slim",
    "--strategy-clock",
    "event",
    "--market-data-cache",
    "compact",
    "--compact-cache-root",
    str(DEFAULT_COMPACT_CACHE_ROOT),
    "--compact-cache-compression",
    "lz4",
    "--carry-positions",
    "--total-capital",
    "50000000",
    "--futures-margin-rate",
    "0.20",
    "--spot-equity-rate",
    "0.40",
    "--no-leverage",
    "--future-order-latency-ms",
    "0",
    "--future-response-latency-ms",
    "0",
    "--future-feed-latency-offset-ms",
    "0",
    "--spot-order-latency-ms",
    "0",
    "--spot-response-latency-ms",
    "0",
    "--spot-feed-latency-offset-ms",
    "0",
    "--post-first-feed-wait",
    "none",
    "--min-entry-interval-sec",
    "0.001",
    "--record-market-every-steps",
    "60",
    "--low-memory-reports",
    "--report-mode",
    "daily",
    "--report-chunk-rows",
    "25000",
    "--event-futures-parquet-dir",
    "/mnt/z/ticks_parquet_stock_future",
)


class DailyLarkError(RuntimeError):
    """Raised when the daily runner or Lark notification cannot complete."""


def _parse_iso_date(value: str, *, field: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise DailyLarkError(f"{field} must use YYYY-MM-DD: {value!r}") from exc


def calendar_trade_dates(calendar_path: Path) -> list[date]:
    """Read and validate trade dates without coercing them through pandas."""
    try:
        with calendar_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None or "trade_dates" not in reader.fieldnames:
                raise DailyLarkError(
                    f"calendar is missing the trade_dates column: {calendar_path}"
                )
            values = {
                _parse_iso_date(row["trade_dates"].strip(), field="trade_dates")
                for row in reader
                if row.get("trade_dates", "").strip()
            }
    except OSError as exc:
        raise DailyLarkError(f"cannot read calendar: {calendar_path}: {exc}") from exc
    if not values:
        raise DailyLarkError(f"calendar has no trade dates: {calendar_path}")
    return sorted(values)


def choose_trade_date(
    calendar_path: Path,
    requested: str | None,
    *,
    today: date | None = None,
    excluded_dates: Iterable[str] = KNOWN_BAD_TRADE_DATES,
) -> str:
    """Choose an explicit date or the previous eligible Taiwan trade date."""
    dates = calendar_trade_dates(calendar_path)
    excluded = {_parse_iso_date(value, field="excluded date") for value in excluded_dates}
    eligible = [value for value in dates if value not in excluded]
    if requested:
        selected = _parse_iso_date(requested, field="trade date")
        if selected not in dates:
            raise DailyLarkError(f"trade date is not present in {calendar_path}: {requested}")
        if selected in excluded:
            raise DailyLarkError(f"trade date is excluded by the backtest defaults: {requested}")
        return selected.isoformat()

    cutoff = today or datetime.now(ZoneInfo("Asia/Taipei")).date()
    candidates = [value for value in eligible if value < cutoff]
    if not candidates:
        raise DailyLarkError(f"calendar has no eligible trade date before {cutoff}")
    return candidates[-1].isoformat()


def build_backtest_command(
    *,
    python: str,
    trade_date: str,
    output_dir: Path,
) -> list[str]:
    """Build a one-date command using the notebook's invariant profile."""
    return [
        python,
        str(BACKTEST_ENTRYPOINT),
        *DAILY_LARK_BACKTEST_ARGS,
        "--start-date",
        trade_date,
        "--end-date",
        trade_date,
        "--output-dir",
        str(output_dir),
    ]


def write_daily_summary_from_trades(trades_path: Path, csv_path: Path) -> None:
    """Write the Lark opportunity CSV from a summary-mode trade audit."""
    try:
        with trades_path.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            _normalize_reader_fieldnames(reader, source=trades_path)
            actual = set(reader.fieldnames or ())
            required = (*DAILY_BACKTEST_SUMMARY_COLUMNS, "status")
            missing = [name for name in required if name not in actual]
            if missing:
                raise DailyLarkError(
                    f"summary trade output is missing required columns: {', '.join(missing)}"
                )
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            with csv_path.open("w", encoding="utf-8", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=DAILY_BACKTEST_SUMMARY_COLUMNS)
                writer.writeheader()
                for row in reader:
                    normalized = _strip_row_values(row)
                    if normalized.get("status") == "FILLED":
                        writer.writerow(
                            {
                                name: normalized.get(name, "")
                                for name in DAILY_BACKTEST_SUMMARY_COLUMNS
                            }
                        )
    except OSError as exc:
        raise DailyLarkError(f"cannot build daily summary from {trades_path}: {exc}") from exc


def read_daily_summary(csv_path: Path) -> list[dict[str, str]]:
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            _normalize_reader_fieldnames(reader, source=csv_path)
            actual = set(reader.fieldnames or ())
            missing = [name for name in DAILY_BACKTEST_SUMMARY_COLUMNS if name not in actual]
            if missing:
                raise DailyLarkError(
                    f"daily summary is missing required columns: {', '.join(missing)}"
                )
            return [_strip_row_values(row) for row in reader]
    except OSError as exc:
        raise DailyLarkError(f"cannot read daily summary: {csv_path}: {exc}") from exc


def _normalize_reader_fieldnames(reader: csv.DictReader, *, source: Path) -> None:
    original = reader.fieldnames or []
    normalized = [name.strip() for name in original]
    if len(normalized) != len(set(normalized)):
        raise DailyLarkError(f"CSV contains duplicate columns after trimming whitespace: {source}")
    reader.fieldnames = normalized


def _strip_row_values(row: Mapping[str, str | None]) -> dict[str, str]:
    return {
        key: "" if value is None else value.strip()
        for key, value in row.items()
        if key is not None
    }


def windows_display_path(path: Path) -> str:
    """Render /mnt/<drive>/... paths in the form familiar to Windows users."""
    resolved = path.resolve()
    parts = resolved.parts
    if len(parts) >= 4 and parts[1] == "mnt" and len(parts[2]) == 1:
        suffix = "\\".join(parts[3:])
        return f"{parts[2].upper()}:\\{suffix}"
    return str(resolved)


def format_success_message(
    trade_date: str,
    rows: Sequence[Mapping[str, str]],
    csv_path: Path,
    *,
    max_rows: int,
) -> str:
    lines = [
        "期現套利每日回測完成",
        f"交易日：{trade_date}",
        f"FILLED 筆數：{len(rows)}",
        f"CSV：{windows_display_path(csv_path)}",
    ]
    if not rows:
        lines.append("結果：今天沒有符合條件的進場／出場機會。")
        return "\n".join(lines)

    lines.append("機會摘要：")
    for index, row in enumerate(rows[:max_rows], start=1):
        lines.append(
            "{index}. {run_key} | {time} | {signal} | "
            "現貨 bid/ask={spot_bid}/{spot_ask} | "
            "期貨 bid/ask={future_bid}/{future_ask}".format(
                index=index,
                run_key=row.get("run_key", ""),
                time=row.get("timestamp_tw", ""),
                signal=row.get("signal", ""),
                spot_bid=row.get("spot_bid", ""),
                spot_ask=row.get("spot_ask", ""),
                future_bid=row.get("future_bid", ""),
                future_ask=row.get("future_ask", ""),
            )
        )
    omitted = len(rows) - max_rows
    if omitted > 0:
        lines.append(f"其餘 {omitted} 筆請查看 CSV。")
    return "\n".join(lines)


def lark_signature(timestamp: str, secret: str) -> str:
    string_to_sign = f"{timestamp}\n{secret}".encode("utf-8")
    digest = hmac.new(string_to_sign, digestmod=hashlib.sha256).digest()
    return base64.b64encode(digest).decode("ascii")


def _validate_webhook_url(webhook_url: str) -> None:
    parsed = parse.urlparse(webhook_url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "open.larksuite.com"
        or not parsed.path.startswith("/open-apis/bot/v2/hook/")
    ):
        raise DailyLarkError("webhook URL is not a Lark custom-bot HTTPS endpoint")


def send_lark_text(
    webhook_url: str,
    webhook_secret: str,
    text: str,
    *,
    timestamp: int | None = None,
    timeout: float = 20.0,
) -> None:
    """Send a signed text message without logging either credential."""
    _validate_webhook_url(webhook_url)
    timestamp_text = str(timestamp if timestamp is not None else int(datetime.now().timestamp()))
    payload = {
        "timestamp": timestamp_text,
        "sign": lark_signature(timestamp_text, webhook_secret),
        "msg_type": "text",
        "content": {"text": text},
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    http_request = request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "hftbacktest-daily/1"},
        method="POST",
    )
    try:
        with request.urlopen(http_request, timeout=timeout) as response:
            response_body = response.read(64 * 1024)
    except (error.URLError, TimeoutError, OSError) as exc:
        raise DailyLarkError(f"Lark notification request failed: {exc}") from exc
    try:
        result = json.loads(response_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DailyLarkError("Lark returned an invalid JSON response") from exc
    status_code = result.get("code", result.get("StatusCode", 0))
    if status_code != 0:
        message = result.get("msg", result.get("StatusMessage", "unknown Lark error"))
        raise DailyLarkError(f"Lark rejected the notification: code={status_code}, message={message}")


def _notify_failure(webhook_url: str, webhook_secret: str, message: str) -> None:
    try:
        send_lark_text(webhook_url, webhook_secret, message)
    except DailyLarkError as exc:
        print(f"WARNING: could not send Lark failure notification: {exc}", file=sys.stderr)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run one notebook-profile futures/spot backtest date and notify Lark."
    )
    parser.add_argument(
        "--trade-date",
        default=os.environ.get("DAILY_BACKTEST_DATE") or None,
        help="YYYY-MM-DD; default is the previous eligible Taiwan trade date.",
    )
    parser.add_argument("--calendar", type=Path, default=DEFAULT_CALENDAR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--max-message-rows", type=int, default=10)
    parser.add_argument("--python", default=sys.executable, help=argparse.SUPPRESS)
    parser.add_argument("--webhook-url", default=None)
    parser.add_argument("--webhook-secret", default=None)
    args = parser.parse_args(argv)
    if args.max_message_rows < 1:
        parser.error("--max-message-rows must be positive")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    webhook_url = args.webhook_url or os.environ.get("LARK_WEBHOOK_URL", "")
    webhook_secret = args.webhook_secret or os.environ.get("LARK_WEBHOOK_SECRET", "")
    if not webhook_url or not webhook_secret:
        print(
            "ERROR: set LARK_WEBHOOK_URL and LARK_WEBHOOK_SECRET, or pass both webhook options.",
            file=sys.stderr,
        )
        return 2

    try:
        trade_date = choose_trade_date(args.calendar.resolve(), args.trade_date)
    except DailyLarkError as exc:
        _notify_failure(webhook_url, webhook_secret, f"期現套利每日回測未啟動\n原因：{exc}")
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    output_dir = (args.output_root / trade_date.replace("-", "")).resolve()
    csv_path = output_dir / "daily_backtest_summary.csv"
    trades_path = output_dir / "trades_all_daily_pairs.csv"
    command = build_backtest_command(
        python=args.python,
        trade_date=trade_date,
        output_dir=output_dir,
    )
    print(f"Running daily backtest for {trade_date}; output={output_dir}")
    try:
        completed = subprocess.run(command, cwd=WORKSPACE_ROOT, check=False)
    except OSError as exc:
        message = f"期現套利每日回測失敗\n交易日：{trade_date}\n原因：無法啟動回測程式：{exc}"
        _notify_failure(webhook_url, webhook_secret, message)
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    if completed.returncode != 0:
        message = (
            f"期現套利每日回測失敗\n交易日：{trade_date}\n"
            f"回測程式 exit code：{completed.returncode}\n輸出目錄：{windows_display_path(output_dir)}"
        )
        _notify_failure(webhook_url, webhook_secret, message)
        return completed.returncode or 1

    try:
        if not csv_path.is_file():
            write_daily_summary_from_trades(trades_path, csv_path)
        rows = read_daily_summary(csv_path)
        message = format_success_message(
            trade_date,
            rows,
            csv_path,
            max_rows=args.max_message_rows,
        )
        send_lark_text(webhook_url, webhook_secret, message)
    except DailyLarkError as exc:
        print(f"ERROR: backtest succeeded but notification failed: {exc}", file=sys.stderr)
        return 1
    print(f"Lark notification sent; FILLED rows={len(rows)}; csv={csv_path}")
    return 0


__all__ = [
    "DailyLarkError",
    "build_backtest_command",
    "calendar_trade_dates",
    "choose_trade_date",
    "format_success_message",
    "lark_signature",
    "main",
    "read_daily_summary",
    "send_lark_text",
    "windows_display_path",
    "write_daily_summary_from_trades",
]
