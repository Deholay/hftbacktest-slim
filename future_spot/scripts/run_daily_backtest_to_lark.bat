@echo off
setlocal EnableExtensions

if not defined LARK_WEBHOOK_URL (
    echo ERROR: LARK_WEBHOOK_URL is not set.
    exit /b 2
)
if not defined LARK_WEBHOOK_SECRET (
    echo ERROR: LARK_WEBHOOK_SECRET is not set.
    exit /b 2
)

rem Optional first argument: YYYY-MM-DD. Empty means latest eligible trade date.
set "DAILY_BACKTEST_DATE=%~1"

rem WSLENV transfers credentials without embedding them in this file or the command line.
if defined WSLENV (
    set "WSLENV=LARK_WEBHOOK_URL:LARK_WEBHOOK_SECRET:DAILY_BACKTEST_DATE:%WSLENV%"
) else (
    set "WSLENV=LARK_WEBHOOK_URL:LARK_WEBHOOK_SECRET:DAILY_BACKTEST_DATE"
)

wsl.exe --cd /home/zoufuc/hftbacktest --exec .venv/bin/python future_spot/scripts/run_daily_backtest_to_lark.py --output-root /mnt/z/hftbacktest_daily_reports
set "BACKTEST_EXIT_CODE=%ERRORLEVEL%"

if not "%BACKTEST_EXIT_CODE%"=="0" (
    echo Daily backtest or Lark notification failed with exit code %BACKTEST_EXIT_CODE%.
) else (
    echo Daily backtest and Lark notification completed.
)
exit /b %BACKTEST_EXIT_CODE%
