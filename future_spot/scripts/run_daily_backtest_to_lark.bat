@echo off
wsl.exe --cd /home/zoufuc/hftbacktest --exec .venv/bin/python /home/zoufuc/hftbacktest/future_spot/scripts/run_daily_backtest_to_lark.py --webhook-url "https://open.larksuite.com/open-apis/bot/v2/hook/ed5f5772-e51a-4a84-844f-f6dec1d2b0a2" --webhook-secret "dZTy1aykdpQZhcg7QEfVzc" %*
exit /b %ERRORLEVEL%
