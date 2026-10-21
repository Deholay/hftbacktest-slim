# Taiwan Market Microstructure Backtesting

Research tools for Taiwan stocks, ETFs, odd lots, and stock futures, built
around [HftBacktest](https://github.com/nkaz001/hftbacktest). The project converts
Top-5 market-by-price snapshots into replayable inputs and provides a
full-market futures/spot arbitrage pipeline with independent leg latency,
position carry, shared-capital replay, and durable reports.

The repository includes two explicit execution paths: installed HftBacktest as
the default reference engine, and the project-owned `hftbacktest_slim` runtime
for immediate BBO execution. Strategy decisions, risk, carry, capital, and
reporting remain in `future_spot/`.

## Documentation

| Document | Contents |
| --- | --- |
| [Slim engine introduction](docs/SLIM_ENGINE.md) | Architecture, matching limits, clocks, sequence ordering, compact depth, and usage examples. |
| [Acceleration strategy](docs/HBT_ACCELERATION_STRATEGY.md) | Execution semantics, bounded processing, implementation phases, and acceptance gates. |
| [Slim package migration](docs/HFTBACKTEST_SLIM_MIGRATION.md) | Package ownership, dependency boundaries, public API, and version history. |
| [Compact Top-N depth design](docs/COMPACT_TOP_N_DEPTH_PHASES.md) | Normalization, schemas, depth selection, cache identities, and rollout phases. |
| [Compact Top-N validation](docs/COMPACT_TOP_N_PHASE5_VALIDATION.md) | Recorded parity results, real-data rollout, scan counts, benchmarks, and limitations. |
| [Research report](docs/report.md) | Earlier research and acceleration overview in Traditional Chinese; use the versioned design and validation documents for current contracts. |
| [Slim package reference](hftbacktest_slim/README.md) | Neutral Python API, package installation, cache commands, and native-library discovery. |
| [Futures/spot runner guide](future_spot/README.md) | Data paths, latency, carry, capital, reporting, and daily Lark notifications. |

## Current version and defaults

`main` combines configurable compact depth from `feat/extend_depth` with
sequence ordering and daily reporting from `feat/future_spot`. The full research
project and its strategy-independent slim package share this repository.

| Component or setting | Version or default |
| --- | --- |
| Python package | `hftbacktest-slim 0.9.0` |
| Rust engine / native ABI | `rust-0.5.0` / `4` |
| Compact schemas / builder | `bbo_v2`, `top5_v1` / `5` |
| HBT/result cache identity | `13` |
| Execution engine | `reference` |
| Strategy clock | `step`, every `1000 ms` |
| Equal-timestamp ordering | `hbt`; slim also supports explicit `sequence` |
| Compact depth | `1`; selectable from `1` through `5` |

Rebuild the native library after updating the runtime. Result identity 13
separates the combined implementation from older matching results. Compact
schemas and builder version are unchanged by this integration, but cache reuse
still requires conservative source, implementation, and content validation.

## Quick start

Use Python 3.11 or newer for repository workflows. Run installation and examples
from the repository root. Slim also requires a Rust/Cargo toolchain.

```bash
python3 -m pip install -r requirements.txt
```

Run a small reference-engine smoke test before a full-market job:

```bash
python3 future_spot/test/run_full_backtest.py \
  --start-date 2026-05-21 \
  --end-date 2026-05-21 \
  --workers 1 \
  --max-pairs 5
```

The reference path uses the Numba scanner by default. Select
`--strategy-engine python` for debugging or a custom strategy implementing
`scripts.strategy_api`.

To run slim, build its shared library and select the engine explicitly:

```bash
cargo build --workspace --release

python3 future_spot/test/run_full_backtest.py \
  --engine slim \
  --start-date 2026-05-21 \
  --end-date 2026-05-21 \
  --workers 1 \
  --max-pairs 5 \
  --compact-cache-root data/tw_compact_v2
```

Slim automatically selects compact data. It supports exactly two assets per
engine, immediate crossing FOK/IOC orders, and full fills without a displayed
size cap. Passive orders, queue models, partial fills, and depth-sensitive
matching use the reference path. Storing levels 2–5 in compact data does not
extend slim matching beyond level 1.

For event-clock execution with three stored levels and source-sequence ordering:

```bash
python3 future_spot/test/run_full_backtest.py \
  --engine slim \
  --compact-depth-levels 3 \
  --strategy-clock event \
  --equal-timestamp-ordering sequence \
  --start-date 2026-03-02 \
  --end-date 2026-03-02 \
  --workers 1 \
  --max-pairs 5 \
  --output-dir future_spot/output/example_slim_n3_sequence_20260302
```

Clock and ordering are separate result-defining choices. The defaults remain
fixed-step execution and HBT-compatible ordering. See the
[slim engine introduction](docs/SLIM_ENGINE.md) for their semantics, reference
Top-N reconstruction, cache namespaces, and Python API examples.

Default data paths use the Linux/WSL `/mnt/z` mount. Adapt
`--futures-parquet-template`, the TWSE/TPEX daily templates,
`--stock-tick-parquet-template`, and `--data-platform-base` to your storage.
The [runner guide](future_spot/README.md) documents the full configuration.
Optional provider SDKs are excluded from `requirements.txt`.

For interactive inspection, use the thin visualization notebook:

```bash
jupyter notebook notebooks/hbt_pair_backtest_visualization.ipynb
```

## Futures/spot workflow and outputs

The runner selects trading dates and eligible pairs, builds or validates market
data, and replays each pair with independent feed, order-entry, and response
latency per leg. Futures are the default first leg. After its fill, the strategy
rechecks the spot-leg condition and applies the configured flatten action if
execution fails. `--post-first-feed-wait spot` explicitly waits for a new spot
feed after the first futures response.

With carry enabled, dates execute sequentially and held contracts enter the
next day's universe without an implicit roll. Pairs within one date can use
worker processes. Results persist by date under `core/dates/trade_date=.../`
before proceeding; resume requires a verified contiguous manifest and carry
chain. Compatibility CSVs and reports are generated in bounded chunks.

| Output | Purpose |
| --- | --- |
| `summary_all_daily_pairs.csv` | Pair/date status, positions, and realized PnL. |
| `trades_all_daily_pairs.csv` | Order and execution audit. |
| `entry_exit_all_daily_pairs.csv` | Entry/exit signals and execution details. |
| `market_all_daily_pairs.csv` | Sampled market state and non-HOLD signals. |
| `latency_all_daily_pairs.csv` | Local and exchange event timelines. |
| `position_carry_status.csv` | Initial/final positions, expiry, and carry audit. |
| `hbt_settings.csv`, `conversion_status.csv` | Execution settings and conversion/cache audit. |
| `run_errors.csv` | Missing inputs and pair failures; inspect after every run. |
| `backtest_manifest.json` | Result-defining arguments, inputs, and implementation identity. |
| `reports/`, `figures/` | Capital, ROI, failure, latency, and performance reports. |

`--report-mode summary` is the bounded default. Detailed report tables default
to Parquet. Full diagnostic reporting requires
`--report-mode full --full-report-max-rows N`. Use
`--skip-entry-exit-by-pair` to omit separate files for each pair.

Shared-capital replay processes saved fill candidates in timestamp order and
reports candidate, accepted, and rejected entries separately. `--leverage`
uses configured futures margin and spot own-funds ratios; `--no-leverage`
charges 100% capital to both legs. Realized portfolio ROI uses
capital-constrained realized PnL divided by starting own capital, excluding
open-position marks.

### Daily opportunity report

```bash
python3 future_spot/test/run_full_backtest.py \
  --start-date 2026-03-02 \
  --end-date 2026-03-02 \
  --report-mode daily
```

Daily mode writes `daily_backtest_summary.csv` with filled signals, both legs'
BBO, and raw exchange timestamps. It disables cross-date carry, capital reports,
and figures; required configs and market-data caches may still be built or
reused. This report describes opportunities rather than capital-constrained
portfolio performance.

The Windows/WSL daily Lark runner uses slim, the event clock, sequence ordering,
and zero latency. See
[daily Lark notifications](future_spot/README.md#daily-run-with-lark-notification-windowswsl)
for setup.

## Historical portfolio results

The saved portfolio report covers the requested interval **2026-01-01 through
2026-07-31**. Recognized exit-date PnL spans **2026-01-08 through 2026-07-30**.
These results have not been rerun with the combined `0.9.0` implementation.

| Metric | Result |
| --- | ---: |
| Realized PnL | NT$7,069,581.38 |
| ROI on NT$50M starting capital | 14.14% |
| Peak capital usage | NT$49,779,287.42 (99.56%) |
| Candidate / accepted entries | 8,206 / 4,185 (51.00%) |
| Capital-rejected entries | 4,021 across 29 days |
| Accepted exits | 4,179 |

![Futures/spot realized portfolio performance](docs/figure/futures-spot-portfolio-overview-20260101-20260731.png)

The simulation used NT$50M shared own capital with leverage disabled: both legs
consumed 100% of notional, each capped at NT$25M. It excludes open-position
marks. Seven incomplete-tick dates and eight expiry-residual pair runs were
excluded; capital replay additionally removed six expiry-residual positions.
Two missing event-data errors were recorded on 2026-07-10. Exact inputs,
exclusions, latency, and implementation fingerprints belong to that run's
`backtest_manifest.json`. These are research results, not live performance.

The saved output directory is:

```text
future_spot/output/hbt_daily_full_market_20260101_20260731_future_order_1ms_response_1ms_feed_0ms_spot_order_1ms_response_35ms_feed_0ms/
```

Its result-defining execution and capital options included:

```bash
python3 future_spot/test/run_full_backtest.py \
  --start-date 2026-01-01 \
  --end-date 2026-07-31 \
  --future-order-latency-ms 1 \
  --future-response-latency-ms 1 \
  --future-feed-latency-offset-ms 0 \
  --spot-order-latency-ms 1 \
  --spot-response-latency-ms 35 \
  --spot-feed-latency-offset-ms 0 \
  --post-first-feed-wait spot \
  --post-first-feed-timeout-ms 5000 \
  --no-leverage
```

## Project layout

| Path | Responsibility |
| --- | --- |
| `hftbacktest_slim/` | Strategy-independent Python/Rust runtime, compact schemas, normalization, cache, and CLI tools. |
| `scripts/strategy_api.py` | Shared strategy context, decision, strategy, runner, and registry contracts. |
| `scripts/hbt_types.py`, `scripts/hbt_common.py`, `scripts/io_utils.py` | Neutral reference types, execution helpers, and I/O. |
| `scripts/tw_stock_data_to_npz.py` | Provider snapshots to HftBacktest events. |
| `scripts/tw_stock_hftbacktest.py`, `scripts/tw_stock_strategies.py` | Stock setup, notebook strategies, and DataFrame summaries. |
| `scripts/compact_hbt_adapter.py` | Compact-to-reference event reconstruction. |
| `future_spot/arbitrage/` | Pricing, risk, execution, configuration, carry, capital, and reporting. |
| `future_spot/scripts/` | Thin strategy CLI entrypoints. |
| `future_spot/test/run_full_backtest.py` | Supported backtest, report, and plot entrypoint. |
| `notebooks/` | Thin conversion, strategy, and visualization experiments. |
| `docs/` | Engine introduction, design contracts, validation evidence, and research reports. |

New strategy families use the neutral slim API or the root strategy contracts;
they must not import `future_spot` for reusable behavior. The slim package must
not import strategy code, root HBT helpers, or installed HftBacktest. See the
[package boundary](docs/HFTBACKTEST_SLIM_MIGRATION.md) and
[strategy interface notebook](notebooks/hbt_strategy_interface_example.ipynb).

## Market-data rules

Provider rows must be converted before reference replay. HftBacktest consumes
an in-memory event array or an NPZ with key `data` and fields
`ev, exch_ts, local_ts, px, qty, order_id, ival, fval`.

Stock conversion uses `data_platform_client/data_stock/api`. The older
`data_platform/data_stock/api` path truncated decimal ETF prices. A healthy
`0050` conversion at tick size `0.05` retains prices such as `77.90`, `77.95`,
`78.00`, and `78.05`.

Top-5 input is market-by-price. Normalize valid levels and aggregate repeated
prices before choosing BBO or emitting depth. A level is a distinct price;
individual orders and exact queue positions cannot be recovered. Trade side is
inferred from the previous BBO unless explicitly configured otherwise.
ETF and odd-lot feeds lack Top-5 quantities; their default
`price_only_depth_qty=1.0` supports structural replay, not queue-volume inference.

HftBacktest depth quantity methods require integer tick indices:

```python
price = 77.10
tick = round(price / 0.05)
qty = depth.ask_qty_at_tick(tick)
```

For missing `ask5`/`bid5`, inspect distinct source/event prices and the actual
instrument tick size before changing conversion or matching logic.

## Validation

Build the native library before running the Python suite:

```bash
cargo build --workspace --release
python3 -m pytest -q tests future_spot/test hftbacktest_slim/tests
python3 -m compileall -q scripts future_spot hftbacktest_slim/src/hftbacktest_slim
cargo test --workspace
cargo fmt --check --all
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

The `0.9.0` integration passed 325 Python tests, 17 Rust tests, compilation,
formatting, and Clippy. Coverage includes package isolation, external-install
smoke tests, Top-N projection under both ordering modes, and carry/restart
regressions. Historical real-data measurements remain tied to the versions and
limitations in the [Top-N validation report](docs/COMPACT_TOP_N_PHASE5_VALIDATION.md).
