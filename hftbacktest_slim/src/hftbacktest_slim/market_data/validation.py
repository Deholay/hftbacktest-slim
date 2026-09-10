"""Bounded compact-partition content validation and incremental facts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from ..errors import ArrowDataError, CompactCacheError, CompactValidationError
from .ordering import timestamp_ordering_facts
from .schema import (
    BBO_PROFILE,
    BBO_SCHEMA_VERSION,
    TOP5_PROFILE,
    validate_compact_schema,
)


@dataclass(frozen=True)
class PartitionValidation:
    """JSON facts plus the bounded ordering inputs required by sidecar checks."""

    facts: dict[str, Any]
    ordering: dict[str, Any]
    metadata: dict[str, str]


def _error(
    message: str,
    *,
    path: Path,
    trade_date: str | None,
    source: str | None,
    symbol: str | None,
    side: str | None = None,
    level: int | None = None,
) -> CompactValidationError:
    identity = (
        f"date={trade_date or 'unknown'} source={source or 'unknown'} "
        f"symbol={symbol or 'unknown'} file={path.name}"
    )
    location = ""
    if side is not None:
        location += f" {side}"
    if level is not None:
        location += f" level {level}"
    return CompactValidationError(f"{identity}{location}: {message}")


def _column(batch: pa.RecordBatch, name: str) -> pa.Array:
    return batch.column(batch.schema.get_field_index(name))


def _required_integer(
    batch: pa.RecordBatch,
    name: str,
    *,
    path: Path,
    trade_date: str | None,
    source: str | None,
    symbol: str | None,
) -> np.ndarray:
    column = _column(batch, name)
    if column.null_count:
        raise _error(
            f"{name} contains null values",
            path=path,
            trade_date=trade_date,
            source=source,
            symbol=symbol,
        )
    return column.to_numpy(zero_copy_only=False)


def _empty_depth(depth_levels: int) -> dict[str, Any]:
    return {
        "bid": {
            str(level): {"valid_rows": 0, "null_rows": 0}
            for level in range(1, depth_levels + 1)
        },
        "ask": {
            str(level): {"valid_rows": 0, "null_rows": 0}
            for level in range(1, depth_levels + 1)
        },
        "complete_bid_rows": 0,
        "complete_ask_rows": 0,
        "complete_book_rows": 0,
        "two_sided_rows": 0,
        "bid_only_rows": 0,
        "ask_only_rows": 0,
        "empty_book_rows": 0,
    }


def _validate_compact(
    data: Path | pa.Table,
    *,
    path: Path,
    expected_depth_levels: int | None = None,
    trade_date: str | None = None,
    source: str | None = None,
    symbol: str | None = None,
    require_identity_metadata: bool = False,
    require_metadata: bool = True,
) -> PartitionValidation:
    handle: pa.MemoryMappedFile | None = None
    if isinstance(data, pa.Table):
        table = data.combine_chunks()

        class _TableReader:
            schema = table.schema
            batches = table.to_batches()
            num_record_batches = len(batches)

            @classmethod
            def get_batch(cls, index: int) -> pa.RecordBatch:
                return cls.batches[index]

        reader = _TableReader()
    else:
        try:
            handle = pa.memory_map(str(data), "r")
            reader = ipc.open_file(handle)
        except (OSError, pa.ArrowException) as exc:
            if handle is not None:
                handle.close()
            raise CompactCacheError(
                f"failed to read compact partition {path}: {exc}"
            ) from exc
    try:
        try:
            metadata = validate_compact_schema(
                reader.schema,
                path,
                expected_depth_levels=expected_depth_levels,
                require_metadata=require_metadata,
            )
        except ArrowDataError as exc:
            raise _error(
                str(exc),
                path=path,
                trade_date=trade_date,
                source=source,
                symbol=symbol,
            ) from exc
        schema_version = metadata.get("schema_version", BBO_SCHEMA_VERSION)
        profile = (
            BBO_PROFILE if schema_version == BBO_SCHEMA_VERSION else TOP5_PROFILE
        )
        depth_levels = 1 if profile == BBO_PROFILE else int(metadata["depth_levels"])
        if require_identity_metadata:
            if metadata.get("profile") != profile:
                raise _error(
                    f"metadata profile must be {profile!r}",
                    path=path,
                    trade_date=trade_date,
                    source=source,
                    symbol=symbol,
                )
            if metadata.get("depth_levels") != str(depth_levels):
                raise _error(
                    f"metadata depth_levels must be {depth_levels}",
                    path=path,
                    trade_date=trade_date,
                    source=source,
                    symbol=symbol,
                )

        depth = _empty_depth(depth_levels)
        rows = 0
        first_exch_ts: int | None = None
        last_exch_ts: int | None = None
        min_price: float | None = None
        max_price: float | None = None
        non_tradable_rows = 0
        trade_events = 0
        previous_volume: int | None = None
        previous_seq: int | None = None
        exchange_parts: list[np.ndarray] = []
        local_parts: list[np.ndarray] = []
        sequence_parts: list[np.ndarray] = []

        for batch_index in range(reader.num_record_batches):
            batch = reader.get_batch(batch_index)
            batch_rows = batch.num_rows
            rows += batch_rows
            exchange = _required_integer(
                batch,
                "exch_ts",
                path=path,
                trade_date=trade_date,
                source=source,
                symbol=symbol,
            ).astype(np.int64, copy=False)
            local = _required_integer(
                batch,
                "local_ts_raw",
                path=path,
                trade_date=trade_date,
                source=source,
                symbol=symbol,
            ).astype(np.int64, copy=False)
            sequence = _required_integer(
                batch,
                "source_seq",
                path=path,
                trade_date=trade_date,
                source=source,
                symbol=symbol,
            ).astype(np.uint64, copy=False)
            if len(sequence):
                if previous_seq is not None and int(sequence[0]) <= previous_seq:
                    raise _error(
                        "source_seq is not strictly increasing",
                        path=path,
                        trade_date=trade_date,
                        source=source,
                        symbol=symbol,
                    )
                if np.any(sequence[1:] <= sequence[:-1]):
                    raise _error(
                        "source_seq is not strictly increasing",
                        path=path,
                        trade_date=trade_date,
                        source=source,
                        symbol=symbol,
                    )
                previous_seq = int(sequence[-1])
                batch_min = int(exchange.min())
                batch_max = int(exchange.max())
                first_exch_ts = (
                    batch_min
                    if first_exch_ts is None
                    else min(first_exch_ts, batch_min)
                )
                last_exch_ts = (
                    batch_max
                    if last_exch_ts is None
                    else max(last_exch_ts, batch_max)
                )
            exchange_parts.append(exchange)
            local_parts.append(local)
            sequence_parts.append(sequence)

            tradable = _required_integer(
                batch,
                "tradable",
                path=path,
                trade_date=trade_date,
                source=source,
                symbol=symbol,
            )
            invalid_tradable = (tradable != 0) & (tradable != 1)
            if np.any(invalid_tradable):
                raise _error(
                    "tradable contains a value outside 0 or 1",
                    path=path,
                    trade_date=trade_date,
                    source=source,
                    symbol=symbol,
                )
            non_tradable_rows += int(np.count_nonzero(tradable == 0))

            volume = _required_integer(
                batch,
                "total_volume",
                path=path,
                trade_date=trade_date,
                source=source,
                symbol=symbol,
            ).astype(np.int64, copy=False)
            if len(volume):
                if (
                    previous_volume is not None
                    and volume[0] > previous_volume
                    and tradable[0] != 0
                ):
                    trade_events += 1
                if len(volume) > 1:
                    trade_events += int(
                        np.count_nonzero(
                            (np.diff(volume) > 0) & (tradable[1:] != 0)
                        )
                    )
                previous_volume = int(volume[-1])

            side_presence: dict[str, list[np.ndarray]] = {"bid": [], "ask": []}
            for side in ("bid", "ask"):
                previous_values: np.ndarray | None = None
                previous_present: np.ndarray | None = None
                for level in range(1, 6 if profile == TOP5_PROFILE else 2):
                    suffix = f"_{level}" if profile == TOP5_PROFILE else ""
                    price = _column(batch, f"{side}_px{suffix}")
                    quantity = _column(batch, f"{side}_qty{suffix}")
                    price_null = price.is_null().to_numpy(zero_copy_only=False)
                    quantity_null = quantity.is_null().to_numpy(zero_copy_only=False)
                    price_values = price.to_numpy(zero_copy_only=False)
                    quantity_values = quantity.to_numpy(zero_copy_only=False)
                    if profile == TOP5_PROFILE and level > depth_levels:
                        if np.any(~price_null | ~quantity_null):
                            raise _error(
                                f"disabled level contains data for depth_levels={depth_levels}",
                                path=path,
                                trade_date=trade_date,
                                source=source,
                                symbol=symbol,
                                side=side,
                                level=level,
                            )
                        continue
                    if profile == BBO_PROFILE:
                        price_missing = price_null | np.isnan(price_values)
                        quantity_missing = quantity_null | np.isnan(quantity_values)
                    else:
                        price_missing = price_null
                        quantity_missing = quantity_null
                    price_only = ~price_missing & quantity_missing
                    quantity_only = price_missing & ~quantity_missing
                    if np.any(price_only):
                        raise _error(
                            "price without quantity",
                            path=path,
                            trade_date=trade_date,
                            source=source,
                            symbol=symbol,
                            side=side,
                            level=level,
                        )
                    if np.any(quantity_only):
                        raise _error(
                            "quantity without price",
                            path=path,
                            trade_date=trade_date,
                            source=source,
                            symbol=symbol,
                            side=side,
                            level=level,
                        )
                    present = ~price_missing
                    invalid = present & ~(
                        np.isfinite(price_values)
                        & (price_values > 0.0)
                        & np.isfinite(quantity_values)
                        & (quantity_values > 0.0)
                    )
                    if np.any(invalid):
                        raise _error(
                            "price and quantity must be finite and greater than zero",
                            path=path,
                            trade_date=trade_date,
                            source=source,
                            symbol=symbol,
                            side=side,
                            level=level,
                        )
                    if previous_present is not None:
                        if np.any(present & ~previous_present):
                            raise _error(
                                "level is populated after a null level",
                                path=path,
                                trade_date=trade_date,
                                source=source,
                                symbol=symbol,
                                side=side,
                                level=level,
                            )
                        both = present & previous_present
                        invalid_order = (
                            price_values >= previous_values
                            if side == "bid"
                            else price_values <= previous_values
                        )
                        if np.any(both & invalid_order):
                            wording = (
                                "strictly descending"
                                if side == "bid"
                                else "strictly ascending"
                            )
                            raise _error(
                                f"{side} levels are not {wording} "
                                "(repeated prices are invalid)",
                                path=path,
                                trade_date=trade_date,
                                source=source,
                                symbol=symbol,
                                side=side,
                                level=level,
                            )
                    valid_count = int(np.count_nonzero(present))
                    depth[side][str(level)]["valid_rows"] += valid_count
                    depth[side][str(level)]["null_rows"] += batch_rows - valid_count
                    if valid_count:
                        selected = price_values[present]
                        selected_min = float(selected.min())
                        selected_max = float(selected.max())
                        min_price = (
                            selected_min
                            if min_price is None
                            else min(min_price, selected_min)
                        )
                        max_price = (
                            selected_max
                            if max_price is None
                            else max(max_price, selected_max)
                        )
                    side_presence[side].append(present)
                    previous_values = price_values
                    previous_present = present

            bid_best = side_presence["bid"][0]
            ask_best = side_presence["ask"][0]
            complete_bid = np.logical_and.reduce(side_presence["bid"])
            complete_ask = np.logical_and.reduce(side_presence["ask"])
            depth["complete_bid_rows"] += int(np.count_nonzero(complete_bid))
            depth["complete_ask_rows"] += int(np.count_nonzero(complete_ask))
            depth["complete_book_rows"] += int(
                np.count_nonzero(complete_bid & complete_ask)
            )
            depth["two_sided_rows"] += int(np.count_nonzero(bid_best & ask_best))
            depth["bid_only_rows"] += int(np.count_nonzero(bid_best & ~ask_best))
            depth["ask_only_rows"] += int(np.count_nonzero(~bid_best & ask_best))
            depth["empty_book_rows"] += int(np.count_nonzero(~bid_best & ~ask_best))

        exchange_all = (
            np.concatenate(exchange_parts)
            if exchange_parts
            else np.empty(0, dtype=np.int64)
        )
        local_all = (
            np.concatenate(local_parts)
            if local_parts
            else np.empty(0, dtype=np.int64)
        )
        sequence_all = (
            np.concatenate(sequence_parts)
            if sequence_parts
            else np.empty(0, dtype=np.uint64)
        )
        ordering = timestamp_ordering_facts(
            exchange_all,
            local_all,
            sequence_all,
            base_latency_ns=int(metadata.get("base_latency_ns", "0")),
        )
        adjustment = int(metadata.get("local_timestamp_adjustment_ns", "0"))
        raw_latency = local_all - exchange_all
        corrected_latency = raw_latency + adjustment
        facts = {
            "rows": rows,
            "profile": profile,
            "schema_version": schema_version,
            "depth_levels": depth_levels,
            "first_exch_ts": first_exch_ts,
            "last_exch_ts": last_exch_ts,
            "raw_min_feed_latency_ns": (
                int(raw_latency.min()) if len(raw_latency) else None
            ),
            "raw_max_feed_latency_ns": (
                int(raw_latency.max()) if len(raw_latency) else None
            ),
            "local_timestamp_adjustment_ns": adjustment,
            "min_latency_ns": (
                int(corrected_latency.min()) if len(corrected_latency) else None
            ),
            "max_latency_ns": (
                int(corrected_latency.max()) if len(corrected_latency) else None
            ),
            "min_price": min_price,
            "max_price": max_price,
            "non_tradable_rows": non_tradable_rows,
            "trade_events": trade_events,
            "depth": depth,
        }
        return PartitionValidation(facts=facts, ordering=ordering, metadata=metadata)
    except pa.ArrowException as exc:
        raise _error(
            f"failed to decode Arrow content: {exc}",
            path=path,
            trade_date=trade_date,
            source=source,
            symbol=symbol,
        ) from exc
    finally:
        if handle is not None:
            handle.close()


def validate_compact_partition(
    path: Path,
    *,
    expected_depth_levels: int | None = None,
    trade_date: str | None = None,
    source: str | None = None,
    symbol: str | None = None,
    require_identity_metadata: bool = False,
) -> PartitionValidation:
    """Validate one Arrow file batch-by-batch and return exact manifest facts.

    Depth columns are converted one record batch at a time. Only the three
    arrays required by the pre-existing timestamp/sidecar contract are retained
    across batches.
    """

    resolved = Path(path)
    return _validate_compact(
        resolved,
        path=resolved,
        expected_depth_levels=expected_depth_levels,
        trade_date=trade_date,
        source=source,
        symbol=symbol,
        require_identity_metadata=require_identity_metadata,
        require_metadata=True,
    )


def validate_compact_table(
    table: pa.Table,
    *,
    expected_depth_levels: int | None = None,
    trade_date: str | None = None,
    source: str | None = None,
    symbol: str | None = None,
    require_identity_metadata: bool = False,
    require_metadata: bool = True,
    path: Path | None = None,
) -> PartitionValidation:
    """Validate an already-loaded table through the canonical content rules."""

    return _validate_compact(
        table,
        path=Path("<memory>") if path is None else Path(path),
        expected_depth_levels=expected_depth_levels,
        trade_date=trade_date,
        source=source,
        symbol=symbol,
        require_identity_metadata=require_identity_metadata,
        require_metadata=require_metadata,
    )


def aggregate_depth_statistics(
    symbols: dict[str, dict[str, Any]], depth_levels: int
) -> dict[str, Any]:
    """Return stable source/date depth totals from symbol manifest facts."""

    total = _empty_depth(depth_levels)
    for symbol in sorted(symbols):
        if symbols[symbol].get("status") == "missing":
            continue
        facts = symbols[symbol].get("depth")
        if not isinstance(facts, dict):
            raise CompactValidationError(
                f"symbol manifest {symbol!r} is missing depth statistics"
            )
        for side in ("bid", "ask"):
            for level in range(1, depth_levels + 1):
                for key in ("valid_rows", "null_rows"):
                    total[side][str(level)][key] += int(facts[side][str(level)][key])
        for key in (
            "complete_bid_rows",
            "complete_ask_rows",
            "complete_book_rows",
            "two_sided_rows",
            "bid_only_rows",
            "ask_only_rows",
            "empty_book_rows",
        ):
            total[key] += int(facts[key])
    return total


__all__ = (
    "PartitionValidation",
    "aggregate_depth_statistics",
    "validate_compact_partition",
    "validate_compact_table",
)
