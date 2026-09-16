"""Build or validate one date of the shared compact BBO cache."""

from __future__ import annotations

import argparse
import json
import resource
import shutil
import time
from pathlib import Path

import pyarrow.parquet as pq

from ..cache.config import CompactBuildConfig, CompactSource
from ..cache.publication import directory_bytes, projected_build_space
from ..cache.store import CompactCacheStore
from ..version import NATIVE_ABI_VERSION, SLIM_ENGINE_VERSION, __version__


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--stock-path", type=Path)
    parser.add_argument("--future-path", type=Path)
    parser.add_argument("--spot-symbols", nargs="*", default=[])
    parser.add_argument("--future-symbols", nargs="*", default=[])
    parser.add_argument(
        "--settings-parquet",
        type=Path,
        help="Read spot/future symbol universes from a persisted settings partition.",
    )
    parser.add_argument(
        "--compression", choices=("none", "lz4", "zstd"), default="lz4"
    )
    parser.add_argument(
        "--compact-depth-levels",
        type=int,
        choices=(1, 2, 3, 4, 5),
        default=1,
        help="Symmetric bid/ask compact depth written to bbo_v2 or top5_v1.",
    )
    parser.add_argument("--batch-rows", type=int, default=131_072)
    parser.add_argument("--max-gb", type=float, default=200.0)
    parser.add_argument("--min-free-gb", type=float, default=200.0)
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def settings_symbols(path: Path) -> tuple[list[str], list[str]]:
    table = pq.ParquetFile(path).read(columns=["leg", "symbol"])
    legs = table["leg"].to_pylist()
    symbols = table["symbol"].to_pylist()
    spot = sorted(
        {str(symbol) for leg, symbol in zip(legs, symbols) if leg == "spot"}
    )
    future = sorted(
        {str(symbol) for leg, symbol in zip(legs, symbols) if leg == "future"}
    )
    return spot, future


def run(args: argparse.Namespace) -> dict:
    spots = list(dict.fromkeys(str(value) for value in args.spot_symbols))
    futures = list(dict.fromkeys(str(value) for value in args.future_symbols))
    if args.settings_parquet:
        settings_spot, settings_future = settings_symbols(args.settings_parquet)
        spots = list(dict.fromkeys([*spots, *settings_spot]))
        futures = list(dict.fromkeys([*futures, *settings_future]))
    sources = []
    if args.stock_path and spots:
        sources.append(CompactSource("stock", (args.stock_path,), tuple(spots)))
    if args.future_path and futures:
        sources.append(
            CompactSource("stock_future", (args.future_path,), tuple(futures))
        )
    if not sources:
        raise ValueError("provide at least one source path and its symbol universe")

    config = CompactBuildConfig(
        cache_root=args.cache_root,
        compression=args.compression,
        depth_levels=getattr(args, "compact_depth_levels", 1),
        batch_rows=args.batch_rows,
        max_cache_bytes=int(args.max_gb * 1024**3),
        min_free_bytes=int(args.min_free_gb * 1024**3),
        rebuild=args.rebuild,
    )
    probe = args.cache_root if args.cache_root.exists() else args.cache_root.parent
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    cache_bytes_before = directory_bytes(args.cache_root)
    free_bytes_before = shutil.disk_usage(probe).free
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    manifest = CompactCacheStore(config).build_date(args.date, sources)
    wall_seconds = time.perf_counter() - started_wall
    peak_rss_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    source_rows = sum(
        int(file_identity["rows"])
        for source in manifest["identity"]["sources"]
        for file_identity in source["files"]
    )
    projected = projected_build_space(source_rows, config.depth_levels)
    cache_bytes_after = directory_bytes(args.cache_root)
    free_bytes_after = shutil.disk_usage(
        args.cache_root if args.cache_root.exists() else probe
    ).free
    build_seconds = (
        float(manifest["elapsed_seconds"])
        if manifest["cache_state"] == "miss"
        else None
    )
    return {
        "date": args.date,
        "cache_root": str(args.cache_root.resolve()),
        "compression": args.compression,
        "profile": manifest["profile"],
        "schema_version": manifest["schema_version"],
        "depth_levels": config.depth_levels,
        "cache_state": manifest["cache_state"],
        "wall_seconds": wall_seconds,
        "cpu_seconds": time.process_time() - started_cpu,
        "peak_rss_kib": peak_rss_kib,
        "peak_rss_bytes": peak_rss_kib * 1024,
        "package_version": __version__,
        "engine_version": SLIM_ENGINE_VERSION,
        "native_abi_version": NATIVE_ABI_VERSION,
        "builder_version": manifest["builder_version"],
        "batch_rows": config.batch_rows,
        "source_rows": source_rows,
        "output_rows": manifest["output_rows"],
        "raw_scan_count": manifest["build_invocation_scan_count"],
        "build_seconds": build_seconds,
        "validation_publication_seconds": (
            None if build_seconds is None else max(0.0, wall_seconds - build_seconds)
        ),
        **projected,
        "actual_output_bytes": manifest["output_bytes"],
        "cache_bytes_before": cache_bytes_before,
        "cache_bytes_after": cache_bytes_after,
        "cache_growth_bytes": cache_bytes_after - cache_bytes_before,
        "free_bytes_before": free_bytes_before,
        "free_bytes_after": free_bytes_after,
        "manifest": manifest,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = run(args)
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


__all__ = ("main", "parse_args", "run", "settings_symbols")


if __name__ == "__main__":
    raise SystemExit(main())
