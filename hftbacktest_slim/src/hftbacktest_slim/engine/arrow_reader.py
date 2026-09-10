"""Project compact BBO or Top-N storage into native ABI-v3 BBO rows."""

from __future__ import annotations

from dataclasses import dataclass
from os import PathLike
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from ..errors import ArrowDataError, CompactCacheError
from ..market_data.schema import (
    BBO_PROFILE,
    COMPACT_SCHEMA_VERSION,
    PHYSICAL_FIELDS,
    SLIM_ROW_DTYPE,
    TOP5_PROFILE,
    TOP5_SCHEMA,
    TOP5_SCHEMA_VERSION,
    decoded_metadata,
    validate_bbo_schema,
)
from ..market_data.validation import validate_compact_table


SUPPORTED_SCHEMA_VERSION = COMPACT_SCHEMA_VERSION


@dataclass(frozen=True, slots=True)
class LoadedRows:
    """One native row array plus profile and local-time identity."""

    rows: np.ndarray
    local_timestamp_adjustment_ns: int
    schema_version: str = COMPACT_SCHEMA_VERSION
    profile: str = BBO_PROFILE
    depth_levels: int = 1


def read_rows(path: str | PathLike[str] | Path) -> LoadedRows:
    """Load one valid (including empty) compact Arrow file into ABI row order.

    ``bbo_v2`` retains its legacy optional-metadata behavior. ``top5_v1``
    requires its complete profile metadata and is content-validated before
    normalized level 1 is copied into the unchanged native BBO row layout.
    """

    resolved = Path(path)
    try:
        with pa.memory_map(str(resolved), "r") as handle:
            table = ipc.open_file(handle).read_all().combine_chunks()
    except (OSError, pa.ArrowException) as exc:
        raise ArrowDataError(f"failed to read compact Arrow partition {resolved}: {exc}") from exc

    metadata = decoded_metadata(table.schema, resolved)
    physical = table.schema.remove_metadata()
    if physical == TOP5_SCHEMA:
        try:
            validation = validate_compact_table(
                table,
                require_identity_metadata=True,
                require_metadata=True,
                path=resolved,
            )
        except CompactCacheError as exc:
            raise ArrowDataError(str(exc)) from exc
        metadata = validation.metadata
        schema_version = TOP5_SCHEMA_VERSION
        profile = TOP5_PROFILE
        depth_levels = int(metadata["depth_levels"])
        projection = {
            "source_seq": "source_seq",
            "exch_ts": "exch_ts",
            "local_ts_raw": "local_ts_raw",
            "bid_px": "bid_px_1",
            "ask_px": "ask_px_1",
            "bid_qty": "bid_qty_1",
            "ask_qty": "ask_qty_1",
            "last_px": "last_px",
            "total_volume": "total_volume",
            "tradable": "tradable",
        }
    else:
        validate_bbo_schema(table.schema, resolved)
        declared_version = metadata.get("schema_version")
        if declared_version is not None and declared_version != SUPPORTED_SCHEMA_VERSION:
            raise ArrowDataError(
                f"compact Arrow partition {resolved} declares schema version "
                f"{declared_version!r}; expected {SUPPORTED_SCHEMA_VERSION!r}"
            )
        schema_version = COMPACT_SCHEMA_VERSION
        profile = BBO_PROFILE
        depth_levels = 1
        projection = {name: name for name in SLIM_ROW_DTYPE.names or ()}
    try:
        adjustment = int(metadata.get("local_timestamp_adjustment_ns", "0"))
    except ValueError as exc:
        raise ArrowDataError(
            f"compact Arrow partition {resolved} has invalid "
            "local_timestamp_adjustment_ns metadata"
        ) from exc

    rows = np.empty(table.num_rows, dtype=SLIM_ROW_DTYPE)
    for name in SLIM_ROW_DTYPE.names or ():
        try:
            rows[name] = table[projection[name]].to_numpy(zero_copy_only=False)
        except (pa.ArrowException, TypeError, ValueError) as exc:
            raise ArrowDataError(
                f"failed to project compact Arrow field {projection[name]!r} "
                f"as {name!r} from {resolved}: {exc}"
            ) from exc
    if np.any((rows["tradable"] != 0) & (rows["tradable"] != 1)):
        raise ArrowDataError(
            f"compact Arrow partition {resolved} has tradable values outside 0/1"
        )
    return LoadedRows(
        rows=rows,
        local_timestamp_adjustment_ns=adjustment,
        schema_version=schema_version,
        profile=profile,
        depth_levels=depth_levels,
    )


def _read_rows(path: str | PathLike[str] | Path) -> tuple[np.ndarray, int]:
    """Internal tuple-shaped bridge for pre-move binding-oriented tests."""

    loaded = read_rows(path)
    return loaded.rows, loaded.local_timestamp_adjustment_ns


__all__ = (
    "LoadedRows",
    "PHYSICAL_FIELDS",
    "SLIM_ROW_DTYPE",
    "SUPPORTED_SCHEMA_VERSION",
    "read_rows",
)
