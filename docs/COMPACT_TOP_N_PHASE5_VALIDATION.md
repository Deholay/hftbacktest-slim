# Compact Top-N Phase 5 validation

Validation date: 2026-09-16. Code baseline: `ad9ca3e` plus the Phase 5
validation fixes in this change. This report records measured evidence; raw
outputs remain ignored under `future_spot/output/`.

## Outcome

The deterministic, real-data source, compact-cache, slim full-date, and
carry/restart gates pass. Direct-reference versus compact-reference mismatch
count is zero for every selected depth N=1..5. Slim N=1 versus N=2/N=3/N=5
mismatch count is zero in deterministic scenarios, and slim N=1 versus N=3 is
also exactly equal for the complete 157-pair real-data date.

Three defects found by rollout were repaired with regression tests:

- summary reporting now accepts valid headerless empty compatibility CSVs on
  zero-trade dates;
- daily result identity is date-local, so extending an output from a verified
  prefix no longer contaminates the first date with the new range end;
- full-market reference reconstruction reads each partition from the already
  validated date manifest instead of revalidating every partition for every
  leg. The reference adapter still validates the selected table contract
  before atomic NPZ publication.

The latter changes the HBT/result identity contract, so
`HBT_CACHE_SCHEMA_VERSION` is 12. Existing result partitions with identity 11
rebuild conservatively. Compact builder 5, package 0.8.0, `bbo_v2`,
`top5_v1`, Rust engine `rust-0.4.0`, and native ABI 3 are unchanged.

## Deterministic parity

The fixture covers repeated bid/ask prices, raw columns out of price order,
all five levels, trailing missing levels, a single-sided row, locked/crossed
quotes, decimal ETF prices, price-only quantities, non-tradable clearing,
volume changes, exchange/local disagreement, equal timestamps, and
`source_seq` ties.

| N | profile/schema | direct events vs compact events | mismatches |
| ---: | --- | --- | ---: |
| 1 | `bbo` / `bbo_v2` | exact dtype, rows, fields, ordering, trades, clears, stats | 0 |
| 2 | `top5` / `top5_v1` | exact | 0 |
| 3 | `top5` / `top5_v1` | exact | 0 |
| 4 | `top5` / `top5_v1` | exact | 0 |
| 5 | `top5` / `top5_v1` | exact | 0 |

Slim native rows are exact for N=1/2/3/5, including timestamp adjustment and
stable order. Fixed-step and event-clock scenarios cover crossing and
non-crossing FOK, IOC, independent feed/entry/response latency, equal-time
priority, post-first-feed waits, non-tradable clear/resume, second-leg failure
and flattening, end-of-data, and timeouts. Strategy-visible BBO, order state,
fills, timestamps, latency, decisions, and result tables have zero mismatches.
A requested quantity larger than displayed level-1 size retains the existing
immediate no-partial-fill behavior.

## Real-data gates and rollout

Sources:

- stock daily: `/mnt/z/數據平台/ticker_store/daily_parquet/twstock_20260302.parquet`,
  13,491,153 rows, 833,637,001 bytes;
- futures daily: `/mnt/z/ticks_parquet_stock_future/2026-03-02.parquet`,
  11,429,430 rows, 414,519,416 bytes.

Daily-versus-symbol parity is exact for 2026-03-02: 0050 (203,737 rows),
2330 (76,903), and 2317 (107,452). It is also exact for 0050 on 2026-02-23
(128,843 rows). Dtypes, nulls, timestamps, duplicates, symbol values, and raw
Top-5 values are included in the parity hash. The 0050 source retains 77.90,
77.95, 78.00, and 78.05. An independent symbol-materialized futures source was
not available, so no daily-versus-symbol futures claim is made; the DHFC6 raw
daily-to-compact-reference comparison at N=3 is exact (377,109 events, zero
mismatches).

Real 0050 direct/compact reference results:

| N | event rows | mismatch count |
| ---: | ---: | ---: |
| 1 | 854,766 | 0 |
| 2 | 1,261,110 | 0 |
| 3 | 1,667,454 | 0 |
| 4 | 2,073,798 | 0 |
| 5 | 2,480,142 | 0 |

Rollout evidence:

- one pair/date at N=1/3/5: exact slim equality, distinct identities and cache
  namespaces, zero errors;
- five pairs/date at N=3: five requested pairs, 26 trade rows, 100 latency
  rows, capital/carry reports present, two cold raw scans, zero errors;
- complete 2026-03-02 slim date at N=1 and N=3: all 157 requested pairs,
  3,302 trade rows, 6,604 entry/exit rows, 780 latency rows, exact equality in
  all eight semantic tables, zero errors;
- complete 2026-03-02 reference compact date at N=3: all 157 pairs, 3,423
  trade rows, 6,846 entry/exit rows, 780 latency rows, zero errors, and
  402,205,768 bytes of depth-qualified reference NPZ sidecars. Reference and
  slim have the same financial summary and carry fields; three pairs differ
  only in diagnostic decision-row counts and 50 latency rows expose different
  displayed future best-size values. This is the documented reference-depth
  versus slim-BBO matching boundary, not a direct/compact source mismatch;
- two-date 2485_GZFC6 run: a 10-contract position carries from 2026-03-02 into
  2026-03-03 with no implicit roll. The expanded-range restart reuses the
  verified first partition, executes only the second date, and is exactly equal
  to uninterrupted output in every persisted table.

Plots were disabled for the resource run; the requested and effective report
interval is 2026-03-02 through 2026-03-03 for the carry run. No expiry falls in
this range; expiry-residual policy remains
covered by deterministic tests. The full-date capital replay reports 102
candidate entries, 102 accepted, zero rejected, 54 accepted exits, zero
discarded open lots, and 48 ending open-lot days. Known incomplete dates,
including 2026-04-23, remain explicit default exclusions and are fingerprinted
in the run manifest; they were not silently used to improve this sample.

`run_errors.csv` contains zero rows for the final full slim N=1, full slim N=3,
full reference N=3, uninterrupted two-date, and restarted two-date runs. All
requested full-date pairs are present. Final manifests identify result schema
12, engine, compact profile/schema/depth, compact identity, package/adapter and
native versions, and the audited exclusions. Carry reports contain six open
full-date positions and the bounded carry case remains open on both dates;
discarded open lots are zero. This is explicit open-position reporting, not
realized-PnL inflation.

## Compact benchmark

Machine: WSL2 Linux 6.6.87.2, Intel i7-12700 (10 cores/20 logical CPUs),
Python 3.12.3, NumPy 2.2.6, PyArrow 24.0.0, Polars 1.42.1, pandas 3.0.3,
rustc/cargo 1.98.0. LZ4, batch size 131,072, three warm-read repetitions.
Universe: 0050/2330/2317 and DHFC6/CAFC6, 24,920,583 input rows and 487,762
selected output rows. These are real-data measurements, not annual results.

| N | cold total s | build s | validation/publication upper bound s | warm reuse s | physical read median s | native projection median s | cold scans |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 28.592 | 27.316 | 1.275 | 0.808 | 0.090 | 0.114 | 2 |
| 3 | 28.583 | 26.610 | 1.973 | 0.830 | 0.230 | 0.337 | 2 |
| 5 | 28.317 | 26.284 | 2.033 | 0.865 | 0.229 | 0.339 | 2 |

The validation/publication number is a conservative residual that also
includes identity/preflight overhead. Every cold run is exactly one stock plus
one futures scan. Every warm reuse and warm read performs zero raw scans.

| N | peak cold RSS MiB | peak warm-reuse RSS MiB | peak read/projection RSS MiB | actual Arrow MiB | cache growth MiB | conservative completed GiB | required completed+temporary GiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 806.25 | 187.84 | 269.49 | 10.05 | 10.07 | 2.67 | 5.35 |
| 3 | 843.45 | 197.00 | 592.55 | 12.19 | 12.22 | 7.13 | 14.26 |
| 5 | 841.56 | 197.84 | 593.27 | 12.95 | 12.99 | 7.13 | 14.26 |

N=3 and N=5 deliberately use the same fixed-`top5_v1` 256-byte estimate;
actual compressed sizes are reported separately. The overall cache root, not
each profile namespace, owns the cap. Free space fell only by actual cache
growth, and no completed cache was deleted. These single-date measurements
demonstrate batch/date/symbol boundedness but do not constitute a monthly or
annual peak-memory claim.

The complete 157-pair warm-cache N=3 slim run measured 22.009 seconds for
matching, 0.166 seconds for daily persistence, 1.024 seconds for compatibility
CSV streaming, and 106.002 seconds for the full pipeline. Report tables were
built in summary/low-memory mode after the pipeline. Reference reconstruction
is a different workload: the first complete N=3 canary spent 99.023 seconds in
validated cache reuse plus reference reconstruction, 8.073 seconds matching,
0.157 seconds persisting the date, 1.032 seconds streaming compatibility CSVs,
and 131.965 seconds in the full pipeline. It materialized 293 unique
depth-qualified NPZ files totaling 383.57 MiB.

## Reproduction commands

Use `.venv/bin/python`; the system Python in this workspace does not contain
the project dependencies.

```bash
.venv/bin/python -m pytest -q tests/test_compact_topn_phase5.py
.venv/bin/python scripts/validate_daily_symbol_parity.py --date 2026-03-02 --symbols 0050 2330 2317 --output /tmp/phase5_source_parity_20260302.json

.venv/bin/python -m hftbacktest_slim.cli.build_cache \
  --date 2026-03-02 \
  --cache-root future_spot/output/phase5_benchmark/cache \
  --stock-path /mnt/z/數據平台/ticker_store/daily_parquet/twstock_20260302.parquet \
  --future-path /mnt/z/ticks_parquet_stock_future/2026-03-02.parquet \
  --spot-symbols 0050 2330 2317 \
  --future-symbols DHFC6 CAFC6 \
  --compact-depth-levels 3 --compression lz4 --batch-rows 131072 \
  --max-gb 200 --min-free-gb 0 \
  --output future_spot/output/phase5_benchmark/build_n3_cold.json

.venv/bin/python -m hftbacktest_slim.cli.benchmark_read \
  --date 2026-03-02 \
  --cache-root future_spot/output/phase5_benchmark/cache \
  --compact-depth-levels 3 --repetitions 3 \
  --output future_spot/output/phase5_benchmark/read_n3.json

.venv/bin/python future_spot/test/run_full_backtest.py \
  --start-date 2026-03-02 --end-date 2026-03-02 \
  --engine slim --market-data-cache compact \
  --compact-cache-root future_spot/output/phase5_stage3_cache \
  --compact-depth-levels 3 --workers 4 --report-mode summary \
  --no-plots --skip-entry-exit-by-pair \
  --output-dir output/phase5_stage3_slim_n3_final

.venv/bin/python future_spot/test/run_full_backtest.py \
  --start-date 2026-03-02 --end-date 2026-03-02 \
  --engine reference --market-data-cache compact \
  --compact-cache-root future_spot/output/phase5_stage3_cache \
  --compact-depth-levels 3 --workers 4 --report-mode summary \
  --no-plots --skip-entry-exit-by-pair \
  --output-dir output/phase5_stage3_reference_n3_v12_fix
```

Repeat the build/read commands with `--compact-depth-levels 1` and
`--compact-depth-levels 5`; use a fresh cache root for a true cold run.

Release validation results:

```text
.venv/bin/python -m pytest -q tests future_spot/test hftbacktest_slim/tests
310 passed

.venv/bin/python -m compileall -q scripts future_spot hftbacktest_slim/src/hftbacktest_slim
passed

cargo test --workspace
16 passed

cargo fmt --check --all
passed

cargo clippy --workspace --all-targets -- -D warnings
passed

.venv/bin/python -m pytest -q \
  hftbacktest_slim/tests/test_dependency_boundary.py \
  hftbacktest_slim/tests/test_external_install.py \
  hftbacktest_slim/tests/test_native_loading.py \
  tests/test_execution_adapters.py
17 passed

git diff --check
passed
```

## Limitations

- Slim projects only normalized level 1. It does not expose levels 2–5 through
  `DepthView` and does not implement native depth-sensitive matching.
- Slim remains immediate crossing FOK/IOC, with no partial fill, displayed-size
  cap, passive/GTC/GTX order, or queue model.
- This validation is one full date plus a bounded two-date carry/restart run,
  not a month or annual resource validation. Existing month evidence remains
  the rollout baseline; no new annual performance claim is made.
- The reference engine remains required for multi-level depth and unsupported
  slim semantics. Reference/slim equality is required only for an approved
  common semantic baseline, not across different depth-sensitive profiles.
