"""Reference-HBT reconstruction for versioned compact BBO and Top-N caches."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import pyarrow as pa

from hftbacktest_slim import (  # noqa: E402
    BBO_SCHEMA,
    BBO_SCHEMA_VERSION,
    TOP5_SCHEMA,
    TOP5_SCHEMA_VERSION,
    validate_compact_schema,
)
from hftbacktest_slim.market_data import validate_compact_table  # noqa: E402
from scripts.tw_stock_data_to_npz import (
    ConversionStats,
    build_events_from_parquet_frame,
    save_event_data,
)


ADAPTER_VERSION = 3


def _compact_descriptor(table: pa.Table) -> dict[str, Any]:
    """Validate one compact table and return its reconstruction identity."""

    physical = table.schema.remove_metadata()
    top5 = physical == TOP5_SCHEMA
    metadata = validate_compact_schema(
        table.schema,
        require_metadata=top5,
    )
    if top5:
        validation = validate_compact_table(
            table,
            require_identity_metadata=True,
            require_metadata=True,
        )
        metadata = validation.metadata
        schema_version = TOP5_SCHEMA_VERSION
        profile = "top5"
        depth_levels = int(metadata["depth_levels"])
    elif physical == BBO_SCHEMA:
        schema_version = BBO_SCHEMA_VERSION
        profile = "bbo"
        depth_levels = 1
    else:  # pragma: no cover - validate_compact_schema supplies the detail
        raise ValueError("unsupported compact physical schema")
    return {
        "metadata": metadata,
        "schema_version": schema_version,
        "profile": profile,
        "depth_levels": depth_levels,
        "source_kind": metadata.get("source", "stock"),
    }


def _converter_table(table: pa.Table, descriptor: dict[str, Any]) -> pa.Table:
    depth_levels = int(descriptor["depth_levels"])
    values: dict[str, pa.ChunkedArray] = {
        "exchtime": table["exch_ts"],
        "localtime": table["local_ts_raw"],
        "last_price": table["last_px"],
        "total_volume": table["total_volume"],
        "tradable": table["tradable"],
    }
    for level in range(1, depth_levels + 1):
        compact_suffix = "" if descriptor["profile"] == "bbo" else f"_{level}"
        values[f"bid_price{level}"] = table[f"bid_px{compact_suffix}"]
        values[f"bid_volume{level}"] = table[f"bid_qty{compact_suffix}"]
        values[f"ask_price{level}"] = table[f"ask_px{compact_suffix}"]
        values[f"ask_volume{level}"] = table[f"ask_qty{compact_suffix}"]
    return pa.table(values)


def _convert_compact(
    table: pa.Table,
    *,
    trade_date: str,
    base_latency_ns: int,
    volume_scale: float,
    trade_side: str,
    no_trades: bool,
) -> tuple[np.ndarray, ConversionStats, dict[str, Any]]:
    descriptor = _compact_descriptor(table)
    if descriptor["profile"] == "top5" and volume_scale != 1.0:
        raise ValueError(
            "top5_v1 quantities were scaled by the compact builder; "
            "volume_scale must be 1.0 during reference reconstruction"
        )
    source_seq = table["source_seq"].to_numpy(zero_copy_only=False)
    exchange_ts = table["exch_ts"].to_numpy(zero_copy_only=False)
    local_ts = table["local_ts_raw"].to_numpy(zero_copy_only=False)
    order = np.lexsort((source_seq, local_ts, exchange_ts))
    ordered = table.take(pa.array(order))
    frame = pl.from_arrow(_converter_table(ordered, descriptor))
    args = argparse.Namespace(
        levels=descriptor["depth_levels"],
        timestamp_unit="ns",
        timezone="Asia/Taipei",
        date=trade_date,
        base_latency_ns=base_latency_ns,
        volume_scale=volume_scale,
        price_only_depth_qty=None,
        trade_side=trade_side,
        no_trades=no_trades,
        no_depth=False,
        qa_sample_rows=1000,
        source_kind=descriptor["source_kind"],
    )
    events, stats = build_events_from_parquet_frame(frame, args)
    return events, stats, descriptor


def compact_to_reference_events(
    table: pa.Table,
    *,
    trade_date: str,
    base_latency_ns: int = 0,
    volume_scale: float = 1.0,
    trade_side: str = "infer",
    no_trades: bool = False,
) -> tuple[np.ndarray, ConversionStats]:
    """Reconstruct the selected compact depth as reference HBT events."""

    events, stats, _ = _convert_compact(
        table,
        trade_date=trade_date,
        base_latency_ns=base_latency_ns,
        volume_scale=volume_scale,
        trade_side=trade_side,
        no_trades=no_trades,
    )
    return events, stats


def _reference_identity(
    *,
    compact_schema_version: str,
    compact_profile: str,
    depth_levels: int,
    compact_identity_sha256: str,
    trade_date: str,
    base_latency_ns: int,
    effective_quantity_scale: float,
    trade_side: str,
    no_trades: bool,
    npz_compression: str,
) -> dict[str, Any]:
    return {
        "adapter_version": ADAPTER_VERSION,
        "compact_schema_version": compact_schema_version,
        "compact_profile": compact_profile,
        "depth_levels": depth_levels,
        "compact_identity_sha256": compact_identity_sha256,
        "trade_date": trade_date,
        "base_latency_ns": base_latency_ns,
        "effective_quantity_scale": effective_quantity_scale,
        "trade_side_inference_policy": trade_side,
        "no_trades": no_trades,
        "npz_compression": npz_compression,
    }


def reference_npz_is_reusable(
    output: Path,
    *,
    compact_schema_version: str,
    compact_profile: str,
    depth_levels: int,
    compact_identity_sha256: str,
    trade_date: str,
    base_latency_ns: int = 0,
    effective_quantity_scale: float = 1.0,
    trade_side: str = "infer",
    no_trades: bool = False,
    npz_compression: str = "uncompressed",
) -> bool:
    """Return whether an atomic NPZ/sidecar pair has the exact requested identity."""

    output = Path(output)
    sidecar = output.with_suffix(output.suffix + ".compact.json")
    if not output.is_file() or not sidecar.is_file():
        return False
    try:
        saved = json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    expected = _reference_identity(
        compact_schema_version=compact_schema_version,
        compact_profile=compact_profile,
        depth_levels=depth_levels,
        compact_identity_sha256=compact_identity_sha256,
        trade_date=trade_date,
        base_latency_ns=base_latency_ns,
        effective_quantity_scale=effective_quantity_scale,
        trade_side=trade_side,
        no_trades=no_trades,
        npz_compression=npz_compression,
    )
    if any(saved.get(key) != value for key, value in expected.items()):
        return False
    if isinstance(saved.get("event_rows"), bool) or not isinstance(
        saved.get("event_rows"), int
    ) or saved["event_rows"] < 0:
        return False
    try:
        digest = hashlib.sha256(output.read_bytes()).hexdigest()
        with np.load(output, allow_pickle=False) as archive:
            if "data" not in archive.files or len(archive["data"]) != saved["event_rows"]:
                return False
    except (OSError, ValueError, EOFError):
        return False
    return saved.get("npz_sha256") == digest


def write_reference_npz_from_compact(
    table: pa.Table,
    output: Path,
    *,
    trade_date: str,
    compact_identity_sha256: str,
    base_latency_ns: int = 0,
    volume_scale: float = 1.0,
    trade_side: str = "infer",
    no_trades: bool = False,
    npz_compression: str = "uncompressed",
) -> dict[str, Any]:
    """Atomically publish a reference NPZ and its compact-source identity."""
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    events, stats, descriptor = _convert_compact(
        table,
        trade_date=trade_date,
        base_latency_ns=base_latency_ns,
        volume_scale=volume_scale,
        trade_side=trade_side,
        no_trades=no_trades,
    )
    identity = _reference_identity(
        compact_schema_version=descriptor["schema_version"],
        compact_profile=descriptor["profile"],
        depth_levels=descriptor["depth_levels"],
        compact_identity_sha256=compact_identity_sha256,
        trade_date=trade_date,
        base_latency_ns=base_latency_ns,
        effective_quantity_scale=volume_scale,
        trade_side=trade_side,
        no_trades=no_trades,
        npz_compression=npz_compression,
    )
    temporary_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output.stem}-", suffix=".npz", dir=output.parent
    )
    os.close(temporary_descriptor)
    temporary = Path(temporary_name)
    args = argparse.Namespace(output=temporary, npz_compression=npz_compression)
    try:
        save_event_data(events, stats, args)
        os.replace(temporary, output)
        manifest_path = output.with_suffix(output.suffix + ".compact.json")
        manifest_descriptor, manifest_name = tempfile.mkstemp(
            prefix=f".{manifest_path.name}-", suffix=".tmp", dir=output.parent
        )
        os.close(manifest_descriptor)
        manifest_temp = Path(manifest_name)
        try:
            payload = {
                **identity,
                "event_rows": len(events),
                "npz_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
            }
            manifest_temp.write_text(
                json.dumps(payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            os.replace(manifest_temp, manifest_path)
        finally:
            manifest_temp.unlink(missing_ok=True)
    finally:
        temporary.unlink(missing_ok=True)
    return {**identity, "event_rows": len(events), "output": str(output)}


COMPACT_HBT_ADAPTER_VERSION = ADAPTER_VERSION


__all__ = (
    "ADAPTER_VERSION",
    "COMPACT_HBT_ADAPTER_VERSION",
    "compact_to_reference_events",
    "reference_npz_is_reusable",
    "write_reference_npz_from_compact",
)
