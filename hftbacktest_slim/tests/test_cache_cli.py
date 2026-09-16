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
        "profile",
        "schema_version",
        "depth_levels",
        "cache_state",
        "wall_seconds",
        "cpu_seconds",
        "peak_rss_kib",
        "peak_rss_bytes",
        "package_version",
        "engine_version",
        "native_abi_version",
        "builder_version",
        "batch_rows",
        "source_rows",
        "output_rows",
        "raw_scan_count",
        "build_seconds",
        "validation_publication_seconds",
        "projected_completed_bytes",
        "largest_temporary_date_bytes",
        "required_additional_bytes",
        "actual_output_bytes",
        "cache_bytes_before",
        "cache_bytes_after",
        "cache_growth_bytes",
        "free_bytes_before",
        "free_bytes_after",
        "manifest",
    }
    assert built["cache_state"] == "miss"
    assert built["depth_levels"] == 1
    assert built["manifest"]["identity"]["depth_levels"] == 1
    benchmark = benchmark_read.run(
        Namespace(
            cache_root=cache,
            date="2026-03-02",
            compact_depth_levels=1,
            repetitions=1,
            output=None,
        )
    )
    assert set(benchmark) == {
        "date",
        "cache_root",
        "cache_state",
        "raw_scan_count",
        "profile",
        "schema_version",
        "depth_levels",
        "builder_version",
        "compression",
        "files",
        "bytes",
        "rows",
        "peak_rss_kib",
        "peak_rss_bytes",
        "runs",
        "projection_runs",
        "median_wall_seconds",
        "median_rows_per_second",
        "median_projection_wall_seconds",
        "median_projection_rows_per_second",
    }
    assert set(benchmark["runs"][0]) == {
        "wall_seconds",
        "cpu_seconds",
        "rows",
        "rows_per_second",
        "checksum",
    }
    assert benchmark["runs"][0]["rows"] == 1
    assert benchmark["projection_runs"][0]["rows"] == 1
    assert benchmark["raw_scan_count"] == 0
    assert benchmark["depth_levels"] == 1


def test_benchmark_read_selects_topn_namespace(tmp_path: Path) -> None:
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
        compact_depth_levels=3,
        batch_rows=2,
        max_gb=1.0,
        min_free_gb=0.0,
        rebuild=False,
        output=None,
    )
    build_cache.run(build_args)
    result = benchmark_read.run(
        Namespace(
            cache_root=cache,
            date="2026-03-02",
            compact_depth_levels=3,
            repetitions=1,
            output=None,
        )
    )
    assert result["profile"] == "top5"
    assert result["schema_version"] == "top5_v1"
    assert result["depth_levels"] == 3
    assert result["rows"] == 1


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
        store_class.return_value.build_date.return_value = {
            "cache_state": "miss",
            "profile": "top5",
            "schema_version": "top5_v1",
            "builder_version": 5,
            "elapsed_seconds": 0.0,
            "output_rows": 0,
            "output_bytes": 0,
            "build_invocation_scan_count": 1,
            "identity": {"sources": [{"files": [{"rows": 0}]}]},
        }
        result = build_cache.run(args)
    assert store_class.call_args.args[0].depth_levels == 3
    assert store_class.call_args.args[0].profile == "top5"
    assert result["depth_levels"] == 3


def test_benchmark_cli_depth_default_choices(tmp_path: Path) -> None:
    default = benchmark_read.parse_args(
        ["--date", "2026-03-02", "--cache-root", str(tmp_path)]
    )
    explicit = benchmark_read.parse_args(
        [
            "--date",
            "2026-03-02",
            "--cache-root",
            str(tmp_path),
            "--compact-depth-levels",
            "5",
        ]
    )
    assert default.compact_depth_levels == 1
    assert explicit.compact_depth_levels == 5
    with pytest.raises(SystemExit):
        benchmark_read.parse_args(
            [
                "--date",
                "2026-03-02",
                "--cache-root",
                str(tmp_path),
                "--compact-depth-levels",
                "0",
            ]
        )
