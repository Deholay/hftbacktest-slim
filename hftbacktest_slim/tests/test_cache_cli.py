from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hftbacktest_slim.cli import benchmark_read, build_cache


def _raw(path: Path) -> None:
    values: dict[str, list] = {
        "symbol": ["0050"],
        "exchtime": [100],
        "localtime": [110],
        "status": [0],
        "last_price": [77.95],
        "total_volume": [1],
    }
    for level in range(1, 6):
        values[f"bid_price{level}"] = [77.90 if level == 1 else None]
        values[f"ask_price{level}"] = [77.95 if level == 1 else None]
        values[f"bid_volume{level}"] = [1.0 if level == 1 else None]
        values[f"ask_volume{level}"] = [1.0 if level == 1 else None]
    pq.write_table(pa.table(values), path)


def test_package_build_and_benchmark_json_shapes(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    cache = tmp_path / "cache"
    _raw(raw)
    build_args = Namespace(
        date="2026-03-02",
        cache_root=cache,
        stock_path=raw,
        future_path=None,
        spot_symbols=["0050"],
        future_symbols=[],
        settings_parquet=None,
        compression="lz4",
        compact_depth_levels=1,
        batch_rows=2,
        max_gb=1.0,
        min_free_gb=0.0,
        rebuild=False,
        output=None,
    )
    built = build_cache.run(build_args)
    assert set(built) == {
        "date",
        "cache_root",
        "compression",
        "depth_levels",
        "cache_state",
        "wall_seconds",
        "cpu_seconds",
        "peak_rss_kib",
        "manifest",
    }
    assert built["cache_state"] == "miss"
    assert built["depth_levels"] == 1
    assert built["manifest"]["identity"]["depth_levels"] == 1
    benchmark = benchmark_read.run(
        Namespace(
            cache_root=cache,
            date="2026-03-02",
            repetitions=1,
            output=None,
        )
    )
    assert set(benchmark) == {
        "date",
        "cache_root",
        "files",
        "bytes",
        "peak_rss_kib",
        "runs",
        "median_wall_seconds",
        "median_rows_per_second",
    }
    assert set(benchmark["runs"][0]) == {
        "wall_seconds",
        "cpu_seconds",
        "rows",
        "rows_per_second",
        "checksum",
    }
    assert benchmark["runs"][0]["rows"] == 1


def test_build_cli_depth_default_choices_and_forwarding(tmp_path: Path) -> None:
    default = build_cache.parse_args(
        ["--date", "2026-03-02", "--cache-root", str(tmp_path)]
    )
    explicit = build_cache.parse_args(
        [
            "--date",
            "2026-03-02",
            "--cache-root",
            str(tmp_path),
            "--compact-depth-levels",
            "3",
        ]
    )
    assert default.compact_depth_levels == 1
    assert explicit.compact_depth_levels == 3
    with pytest.raises(SystemExit):
        build_cache.parse_args(
            [
                "--date",
                "2026-03-02",
                "--cache-root",
                str(tmp_path),
                "--compact-depth-levels",
                "6",
            ]
        )

    args = Namespace(
        date="2026-03-02",
        cache_root=tmp_path / "cache",
        stock_path=tmp_path / "source.parquet",
        future_path=None,
        spot_symbols=["0050"],
        future_symbols=[],
        settings_parquet=None,
        compression="lz4",
        compact_depth_levels=3,
        batch_rows=2,
        max_gb=1.0,
        min_free_gb=0.0,
        rebuild=False,
        output=None,
    )
    with patch.object(build_cache, "CompactCacheStore") as store_class:
        store_class.return_value.build_date.return_value = {"cache_state": "miss"}
        result = build_cache.run(args)
    assert store_class.call_args.args[0].depth_levels == 3
    assert store_class.call_args.args[0].profile == "top5"
    assert result["depth_levels"] == 3
