"""One-scan streaming construction of compact per-symbol market-depth partitions."""

from __future__ import annotations

import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from ..errors import CompactCacheError
from ..market_data.normalize import (
    normalized_bbo_from_depth_columns,
    normalized_depth_from_depth_columns,
)
from ..market_data.ordering import timestamp_ordering_facts, write_order_sidecar
from ..market_data.schema import (
    BBO_SCHEMA,
    PROJECTED_COLUMNS,
    profile_for_depth_levels,
    schema_for_depth_levels,
    schema_version_for_depth_levels,
    top5_schema_metadata,
)
from ..market_data.status import twse_trial_status_mask
from ..market_data.validation import (
    aggregate_depth_statistics,
    validate_compact_partition,
)
from .config import CompactBuildConfig, CompactSource
from .manifest import file_sha256
from .publication import directory_bytes, runtime_budget_check, write_json


@dataclass
class _SymbolState:
    parts: list[Path] = field(default_factory=list)
    non_tradable_rows: int = 0


def build_source(
    temp: Path,
    trade_date: str,
    source: CompactSource,
    config: CompactBuildConfig,
) -> dict[str, Any]:
    """Scan every physical file once and route all requested symbols."""

    started = time.perf_counter()
    source_dir = temp / source_dirname(source.kind)
    parts_dir = source_dir / ".parts"
    parts_dir.mkdir(parents=True)
    states = {symbol: _SymbolState() for symbol in source.symbols}
    aliases = symbol_aliases(source.symbols)
    scans = 0
    input_rows = 0
    source_offset = 0
    for path_value in source.paths:
        path = Path(path_value)
        scans += 1
        for batch in iter_source_batches(path, config.batch_rows):
            input_rows += batch.num_rows
            seq = np.arange(
                source_offset,
                source_offset + batch.num_rows,
                dtype=np.uint64,
            )
            source_offset += batch.num_rows
            symbols = string_array(batch, "symbol", fallback="symbol_id")
            for raw_symbol in np.unique(symbols):
                symbol = aliases.get(str(raw_symbol))
                if symbol is None:
                    continue
                mask = symbols == raw_symbol
                compact = compact_batch(batch, seq, mask, source, config)
                if compact.num_rows == 0:
                    continue
                state = states[symbol]
                part = parts_dir / f"{symbol}-{len(state.parts):06d}.arrow"
                write_arrow(part, compact, config.compression)
                state.parts.append(part)
                _observe_symbol_state(state, compact)
            runtime_budget_check(Path(config.cache_root), temp, config)

    symbol_manifest: dict[str, Any] = {}
    for symbol, state in states.items():
        output = source_dir / f"{symbol}.arrow"
        if not state.parts:
            symbol_manifest[symbol] = write_empty_symbol(
                output,
                symbol=symbol,
                source=source.kind,
                trade_date=trade_date,
                compression=config.compression,
                base_latency_ns=config.base_latency_ns,
                non_tradable_rows=state.non_tradable_rows,
                depth_levels=config.depth_levels,
            )
        else:
            symbol_manifest[symbol] = consolidate_symbol(
                output,
                state.parts,
                symbol=symbol,
                source=source.kind,
                trade_date=trade_date,
                compression=config.compression,
                base_latency_ns=config.base_latency_ns,
                non_tradable_rows=state.non_tradable_rows,
                depth_levels=config.depth_levels,
            )
        runtime_budget_check(Path(config.cache_root), temp, config)
    # The target is the specific build-owned parts directory; completed symbol
    # files and any other cache dates are never deletion candidates.
    shutil.rmtree(parts_dir)
    output_bytes = directory_bytes(source_dir)
    valid_symbols = sorted(
        symbol
        for symbol, item in symbol_manifest.items()
        if item.get("status") == "valid"
    )
    missing_symbols = sorted(
        symbol
        for symbol, item in symbol_manifest.items()
        if item.get("status") == "missing"
    )
    empty_symbols = sorted(
        symbol
        for symbol, item in symbol_manifest.items()
        if item.get("empty") is True
    )
    payload = {
        "kind": source.kind,
        "scan_count": scans,
        "statistics_compact_read_count": len(valid_symbols),
        "input_rows": input_rows,
        "input_bytes": sum(Path(path).stat().st_size for path in source.paths),
        "output_rows": sum(item.get("rows", 0) for item in symbol_manifest.values()),
        "non_tradable_rows": sum(
            item.get("non_tradable_rows", 0) for item in symbol_manifest.values()
        ),
        "output_bytes": output_bytes,
        "elapsed_seconds": time.perf_counter() - started,
        "profile": config.profile,
        "schema_version": config.schema_version,
        "depth_levels": config.depth_levels,
        "price_only_quantity_policy": {
            "mode": (
                "source_quantity"
                if source.price_only_depth_qty is None
                else "configured_placeholder"
            ),
            "quantity": source.price_only_depth_qty,
        },
        "volume_scale": source.volume_scale,
        "requested_symbol_count": len(source.symbols),
        "valid_symbol_count": len(valid_symbols),
        "empty_symbol_count": len(empty_symbols),
        "missing_symbol_count": len(missing_symbols),
        "missing_symbols": missing_symbols,
        "empty_symbols": empty_symbols,
        "depth": aggregate_depth_statistics(symbol_manifest, config.depth_levels),
        "symbols": symbol_manifest,
    }
    write_json(source_dir / "manifest.json", payload)
    return payload


def _observe_symbol_state(state: _SymbolState, table: pa.Table) -> None:
    tradable = table["tradable"].to_numpy(zero_copy_only=False)
    state.non_tradable_rows += int(np.count_nonzero(tradable == 0))


def compact_batch(
    batch: pa.RecordBatch,
    source_seq: np.ndarray,
    mask: np.ndarray,
    source: CompactSource,
    config: CompactBuildConfig,
) -> pa.Table:
    indexes = np.flatnonzero(mask)
    exchange = numeric(batch, "exchtime", indexes, np.int64)
    local = numeric(batch, "localtime", indexes, np.int64, fallback=exchange)
    keep = np.ones(len(indexes), dtype=bool)
    if source.status_allow and "status" in batch.schema.names:
        status = string_array(batch, "status")[indexes]
        keep &= np.isin(status, np.asarray(source.status_allow, dtype=str))
    if config.session_start_ns is not None:
        keep &= exchange >= config.session_start_ns
    if config.session_end_ns is not None:
        keep &= exchange <= config.session_end_ns
    indexes = indexes[keep]
    exchange = exchange[keep]
    local = local[keep]
    seq = source_seq[indexes]
    if not len(indexes):
        return pa.Table.from_batches(
            [], schema=schema_for_depth_levels(config.depth_levels)
        )
    bid_prices = matrix(batch, "bid_price", indexes)
    ask_prices = matrix(batch, "ask_price", indexes)
    bid_quantities = matrix(batch, "bid_volume", indexes)
    ask_quantities = matrix(batch, "ask_volume", indexes)
    last_px = numeric(batch, "last_price", indexes, np.float64, default=np.nan)
    total_volume = numeric(batch, "total_volume", indexes, np.int64, default=0)
    tradable = np.ones(len(indexes), dtype=np.uint8)
    if source.kind in {"stock", "etf", "odd_lot"}:
        if "status" not in batch.schema.names or not pa.types.is_integer(
            batch.schema.field("status").type
        ):
            raise CompactCacheError(
                f"{source.kind} compact conversion requires packed integer status"
            )
        status = numeric(batch, "status", indexes, np.uint32, default=0)
        tradable[twse_trial_status_mask(status)] = 0
    if config.depth_levels == 1:
        bid_px, bid_qty = best_side(bid_prices, bid_quantities, True, source)
        ask_px, ask_qty = best_side(ask_prices, ask_quantities, False, source)
        return pa.Table.from_arrays(
            [
                seq,
                exchange,
                local,
                bid_px,
                ask_px,
                bid_qty,
                ask_qty,
                last_px,
                total_volume,
                tradable,
            ],
            schema=BBO_SCHEMA,
        )

    bid_px, bid_qty = depth_side(
        bid_prices, bid_quantities, True, source, config.depth_levels
    )
    ask_px, ask_qty = depth_side(
        ask_prices, ask_quantities, False, source, config.depth_levels
    )
    depth_arrays = [
        *_nullable_depth_arrays(bid_px, bid_qty, price=True),
        *_nullable_depth_arrays(bid_px, bid_qty, price=False),
        *_nullable_depth_arrays(ask_px, ask_qty, price=True),
        *_nullable_depth_arrays(ask_px, ask_qty, price=False),
    ]
    return pa.Table.from_arrays(
        [seq, exchange, local, *depth_arrays, last_px, total_volume, tradable],
        schema=schema_for_depth_levels(config.depth_levels),
    )


def best_side(
    prices: np.ndarray,
    quantities: np.ndarray,
    bid: bool,
    source: CompactSource,
) -> tuple[np.ndarray, np.ndarray]:
    return normalized_bbo_from_depth_columns(
        np.ascontiguousarray(prices),
        np.ascontiguousarray(quantities),
        source.volume_scale,
        0.0 if source.price_only_depth_qty is None else source.price_only_depth_qty,
        source.price_only_depth_qty is not None,
        bid,
    )


def depth_side(
    prices: np.ndarray,
    quantities: np.ndarray,
    bid: bool,
    source: CompactSource,
    depth_levels: int,
) -> tuple[np.ndarray, np.ndarray]:
    return normalized_depth_from_depth_columns(
        np.ascontiguousarray(prices),
        np.ascontiguousarray(quantities),
        source.volume_scale,
        0.0 if source.price_only_depth_qty is None else source.price_only_depth_qty,
        source.price_only_depth_qty is not None,
        bid,
        depth_levels,
    )


def _nullable_depth_arrays(
    prices: np.ndarray,
    quantities: np.ndarray,
    *,
    price: bool,
) -> list[pa.Array]:
    """Return five Arrow arrays with one shared null mask per price/qty level."""

    values = prices if price else quantities
    row_count, selected_levels = values.shape
    arrays: list[pa.Array] = []
    for level in range(5):
        if level >= selected_levels:
            arrays.append(pa.nulls(row_count, type=pa.float64()))
            continue
        missing = ~(
            np.isfinite(prices[:, level])
            & (prices[:, level] > 0.0)
            & np.isfinite(quantities[:, level])
            & (quantities[:, level] > 0.0)
        )
        arrays.append(pa.array(values[:, level], mask=missing, type=pa.float64()))
    return arrays


def consolidate_symbol(
    output: Path,
    parts: Sequence[Path],
    *,
    depth_levels: int = 1,
    **metadata: Any,
) -> dict[str, Any]:
    exchange_parts: list[np.ndarray] = []
    local_parts: list[np.ndarray] = []
    sequence_parts: list[np.ndarray] = []
    row_count = 0
    expected_schema = schema_for_depth_levels(depth_levels)
    for part in parts:
        with pa.memory_map(str(part), "r") as handle:
            table = ipc.open_file(handle).read_all().combine_chunks()
        if table.schema.remove_metadata() != expected_schema:
            raise CompactCacheError(
                f"mixed or incompatible compact part schema: {part}"
            )
        exchange_parts.append(table["exch_ts"].to_numpy(zero_copy_only=False))
        local_parts.append(table["local_ts_raw"].to_numpy(zero_copy_only=False))
        sequence_parts.append(table["source_seq"].to_numpy(zero_copy_only=False))
        row_count += table.num_rows
    exchange = np.concatenate(exchange_parts)
    local = np.concatenate(local_parts)
    source_seq = np.concatenate(sequence_parts)
    ordering = timestamp_ordering_facts(
        exchange,
        local,
        source_seq,
        base_latency_ns=int(metadata["base_latency_ns"]),
    )
    file_metadata = _file_metadata(
        depth_levels,
        metadata,
        local_timestamp_adjustment_ns=ordering["local_timestamp_adjustment_ns"],
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    schema = expected_schema.with_metadata(file_metadata)
    options = ipc.IpcWriteOptions(
        compression=None if metadata["compression"] == "none" else metadata["compression"]
    )
    with output.open("wb") as sink, ipc.new_file(
        sink, schema, options=options
    ) as writer:
        for part in parts:
            with pa.memory_map(str(part), "r") as handle:
                writer.write_table(ipc.open_file(handle).read_all())

    sidecars: dict[str, Any] = {}
    if ordering["exchange_order"] is not None:
        sidecars["exchange_order"] = write_order_sidecar(
            output,
            "exchange",
            ordering["exchange_order"],
            write_arrow=write_arrow,
            file_sha256=file_sha256,
        )
    if ordering["local_order"] is not None:
        sidecars["local_order"] = write_order_sidecar(
            output,
            "local",
            ordering["local_order"],
            write_arrow=write_arrow,
            file_sha256=file_sha256,
        )
    validation = validate_compact_partition(
        output,
        expected_depth_levels=depth_levels,
        trade_date=str(metadata["trade_date"]),
        source=str(metadata["source"]),
        symbol=str(metadata["symbol"]),
        require_identity_metadata=True,
    )
    facts = validation.facts
    return {
        "file": output.name,
        "rows": row_count,
        "bytes": output.stat().st_size,
        "sha256": file_sha256(output),
        "profile": facts["profile"],
        "schema_version": facts["schema_version"],
        "depth_levels": facts["depth_levels"],
        "first_exch_ts": facts["first_exch_ts"],
        "last_exch_ts": facts["last_exch_ts"],
        "raw_min_feed_latency_ns": facts["raw_min_feed_latency_ns"],
        "raw_max_feed_latency_ns": facts["raw_max_feed_latency_ns"],
        "local_timestamp_adjustment_ns": facts["local_timestamp_adjustment_ns"],
        "exchange_ordered": ordering["exchange_ordered"],
        "local_ordered": ordering["local_ordered"],
        "requires_dual_order": ordering["requires_dual_order"],
        "min_price": facts["min_price"],
        "max_price": facts["max_price"],
        "non_tradable_rows": facts["non_tradable_rows"],
        "depth": facts["depth"],
        "sidecars": sidecars,
        "status": "valid",
    }


def write_empty_symbol(
    output: Path,
    *,
    depth_levels: int = 1,
    **metadata: Any,
) -> dict[str, Any]:
    file_metadata = _file_metadata(
        depth_levels, metadata, local_timestamp_adjustment_ns=0
    )
    table = pa.Table.from_batches(
        [], schema=schema_for_depth_levels(depth_levels)
    ).replace_schema_metadata(file_metadata)
    write_arrow(output, table, metadata["compression"])
    validation = validate_compact_partition(
        output,
        expected_depth_levels=depth_levels,
        trade_date=str(metadata["trade_date"]),
        source=str(metadata["source"]),
        symbol=str(metadata["symbol"]),
        require_identity_metadata=True,
    )
    facts = validation.facts
    return {
        "file": output.name,
        "rows": 0,
        "bytes": output.stat().st_size,
        "sha256": file_sha256(output),
        "profile": facts["profile"],
        "schema_version": facts["schema_version"],
        "depth_levels": facts["depth_levels"],
        "first_exch_ts": facts["first_exch_ts"],
        "last_exch_ts": facts["last_exch_ts"],
        "raw_min_feed_latency_ns": facts["raw_min_feed_latency_ns"],
        "raw_max_feed_latency_ns": facts["raw_max_feed_latency_ns"],
        "local_timestamp_adjustment_ns": facts["local_timestamp_adjustment_ns"],
        "exchange_ordered": True,
        "local_ordered": True,
        "requires_dual_order": False,
        "min_price": facts["min_price"],
        "max_price": facts["max_price"],
        "non_tradable_rows": facts["non_tradable_rows"],
        "depth": facts["depth"],
        "sidecars": {},
        "empty": True,
        "status": "valid",
    }


def _file_metadata(
    depth_levels: int,
    metadata: dict[str, Any],
    *,
    local_timestamp_adjustment_ns: int,
) -> dict[bytes, bytes]:
    values = {
        **{key: str(value) for key, value in metadata.items()},
        "schema_version": schema_version_for_depth_levels(depth_levels),
        "profile": profile_for_depth_levels(depth_levels),
        "depth_levels": str(depth_levels),
        "local_timestamp_adjustment_ns": str(local_timestamp_adjustment_ns),
        "exchange_ordering": "exch_ts,source_seq",
        "local_ordering": "corrected_local_ts,source_seq",
    }
    encoded = {key.encode(): value.encode() for key, value in values.items()}
    if depth_levels > 1:
        encoded.update(
            top5_schema_metadata(
                depth_levels,
                local_timestamp_adjustment_ns=local_timestamp_adjustment_ns,
            )
        )
    return encoded


def iter_source_batches(path: Path, batch_rows: int) -> Iterator[pa.RecordBatch]:
    if path.suffix.lower() == ".parquet":
        parquet = pq.ParquetFile(path)
        names = [name for name in PROJECTED_COLUMNS if name in parquet.schema_arrow.names]
        yield from parquet.iter_batches(
            batch_size=batch_rows,
            columns=names,
            use_threads=True,
        )
        return
    with pa.memory_map(str(path), "r") as handle:
        reader = ipc.open_file(handle)
        names = [name for name in PROJECTED_COLUMNS if name in reader.schema.names]
        for index in range(reader.num_record_batches):
            table = pa.Table.from_batches([reader.get_batch(index)]).select(names)
            yield from table.to_batches(max_chunksize=batch_rows)


def numeric(
    batch: pa.RecordBatch,
    name: str,
    indexes: np.ndarray,
    dtype: Any,
    *,
    fallback: np.ndarray | None = None,
    default: float | int = 0,
) -> np.ndarray:
    if name not in batch.schema.names:
        source = fallback if fallback is not None else np.full(len(indexes), default)
        return np.asarray(source, dtype=dtype)
    arrow_type = pa.float64() if dtype == np.float64 else pa.int64()
    values = pc.cast(
        batch.column(batch.schema.get_field_index(name)), arrow_type, safe=False
    )
    values = pc.fill_null(values, pa.scalar(default, type=values.type)).to_numpy(
        zero_copy_only=False
    )[indexes]
    if fallback is not None:
        values = np.where(values == 0, fallback, values)
    return np.asarray(values, dtype=dtype)


def matrix(batch: pa.RecordBatch, prefix: str, indexes: np.ndarray) -> np.ndarray:
    return np.column_stack(
        [
            numeric(
                batch,
                f"{prefix}{level}",
                indexes,
                np.float64,
                default=np.nan,
            )
            for level in range(1, 6)
        ]
    )


def string_array(
    batch: pa.RecordBatch,
    name: str,
    fallback: str | None = None,
) -> np.ndarray:
    if name not in batch.schema.names:
        if fallback is None or fallback not in batch.schema.names:
            raise CompactCacheError(f"source is missing {name!r} and {fallback!r}")
        name = fallback
    column = batch.column(batch.schema.get_field_index(name))
    return np.asarray(pc.cast(column, pa.string(), safe=False).to_pylist(), dtype=str)


def symbol_aliases(symbols: Iterable[str]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for symbol in symbols:
        for value in (str(symbol), str(symbol).lstrip("0")):
            if value and value in aliases and aliases[value] != symbol:
                raise CompactCacheError(f"ambiguous symbol alias: {value}")
            if value:
                aliases[value] = str(symbol)
    return aliases


def write_arrow(path: Path, table: pa.Table, compression: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    options = ipc.IpcWriteOptions(
        compression=None if compression == "none" else compression
    )
    with path.open("wb") as sink, ipc.new_file(
        sink, table.schema, options=options
    ) as writer:
        writer.write_table(table)


def source_dirname(source: str) -> str:
    return f"source={source}"


__all__: tuple[str, ...] = ()
