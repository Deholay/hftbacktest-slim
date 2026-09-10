"""Versioned physical contracts for compact BBO and fixed Top-5 rows."""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pyarrow as pa

from ..errors import ArrowDataError


BBO_PROFILE = "bbo"
TOP5_PROFILE = "top5"
BBO_SCHEMA_VERSION = "bbo_v2"
TOP5_SCHEMA_VERSION = "top5_v1"
# Historical BBO callers use this name. It must continue to identify bbo_v2.
COMPACT_SCHEMA_VERSION = BBO_SCHEMA_VERSION
MAX_DEPTH_LEVELS = 5

DEPTH_AGGREGATION_POLICY = "valid_positive_distinct_price_sum_qty_v1"
BID_DEPTH_ORDERING = "price_descending_v1"
ASK_DEPTH_ORDERING = "price_ascending_v1"
MISSING_LEVEL_NULL_POLICY = "enabled_trailing_and_disabled_arrow_null_v1"
PRICE_ONLY_QUANTITY_POLICY = "source_quantity_or_configured_placeholder_v1"
TIMESTAMP_ORDERING_POLICY = "per_symbol_latency_correction_dual_stable_order_v1"

BBO_PHYSICAL_FIELDS: tuple[tuple[str, pa.DataType], ...] = (
    ("source_seq", pa.uint64()),
    ("exch_ts", pa.int64()),
    ("local_ts_raw", pa.int64()),
    ("bid_px", pa.float64()),
    ("ask_px", pa.float64()),
    ("bid_qty", pa.float64()),
    ("ask_qty", pa.float64()),
    ("last_px", pa.float64()),
    ("total_volume", pa.int64()),
    ("tradable", pa.uint8()),
)
# Historical BBO export retained as an exact alias.
PHYSICAL_FIELDS = BBO_PHYSICAL_FIELDS
BBO_SCHEMA = pa.schema(
    [pa.field(name, data_type, nullable=True) for name, data_type in BBO_PHYSICAL_FIELDS]
)

TOP5_PHYSICAL_FIELDS: tuple[tuple[str, pa.DataType], ...] = (
    ("source_seq", pa.uint64()),
    ("exch_ts", pa.int64()),
    ("local_ts_raw", pa.int64()),
    *((f"bid_px_{level}", pa.float64()) for level in range(1, 6)),
    *((f"bid_qty_{level}", pa.float64()) for level in range(1, 6)),
    *((f"ask_px_{level}", pa.float64()) for level in range(1, 6)),
    *((f"ask_qty_{level}", pa.float64()) for level in range(1, 6)),
    ("last_px", pa.float64()),
    ("total_volume", pa.int64()),
    ("tradable", pa.uint8()),
)
TOP5_SCHEMA = pa.schema(
    [pa.field(name, data_type, nullable=True) for name, data_type in TOP5_PHYSICAL_FIELDS]
)

SLIM_ROW_DTYPE = np.dtype(
    [
        ("source_seq", "u8"),
        ("exch_ts", "i8"),
        ("local_ts_raw", "i8"),
        ("bid_px", "f8"),
        ("ask_px", "f8"),
        ("bid_qty", "f8"),
        ("ask_qty", "f8"),
        ("last_px", "f8"),
        ("total_volume", "i8"),
        ("tradable", "u1"),
    ],
    align=True,
)

PROJECTED_COLUMNS = [
    "symbol",
    "symbol_id",
    "exchtime",
    "localtime",
    "status",
    "last_price",
    "total_volume",
    "sequence",
] + [
    f"{side}_{kind}{level}"
    for level in range(1, 6)
    for side in ("bid", "ask")
    for kind in ("price", "volume")
]


def validate_depth_levels(value: object) -> int:
    """Return a supported symmetric depth count, rejecting coercible values."""

    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 5:
        raise ValueError("depth_levels must be an integer from 1 through 5")
    return value


def profile_for_depth_levels(depth_levels: object) -> str:
    return BBO_PROFILE if validate_depth_levels(depth_levels) == 1 else TOP5_PROFILE


def schema_version_for_depth_levels(depth_levels: object) -> str:
    return (
        BBO_SCHEMA_VERSION
        if validate_depth_levels(depth_levels) == 1
        else TOP5_SCHEMA_VERSION
    )


def schema_for_depth_levels(depth_levels: object) -> pa.Schema:
    return BBO_SCHEMA if validate_depth_levels(depth_levels) == 1 else TOP5_SCHEMA


def physical_fields_for_depth_levels(
    depth_levels: object,
) -> tuple[tuple[str, pa.DataType], ...]:
    return (
        BBO_PHYSICAL_FIELDS
        if validate_depth_levels(depth_levels) == 1
        else TOP5_PHYSICAL_FIELDS
    )


def top5_schema_metadata(
    depth_levels: object,
    *,
    local_timestamp_adjustment_ns: int,
) -> dict[bytes, bytes]:
    """Build the required top5_v1 Arrow metadata for one partition."""

    depth = validate_depth_levels(depth_levels)
    if depth == 1:
        raise ValueError("top5_v1 depth_levels must be an integer from 2 through 5")
    if isinstance(local_timestamp_adjustment_ns, bool) or not isinstance(
        local_timestamp_adjustment_ns, int
    ):
        raise ValueError("local_timestamp_adjustment_ns must be an integer")
    values = {
        "schema_version": TOP5_SCHEMA_VERSION,
        "profile": TOP5_PROFILE,
        "depth_levels": str(depth),
        "local_timestamp_adjustment_ns": str(local_timestamp_adjustment_ns),
        "aggregation_policy": DEPTH_AGGREGATION_POLICY,
        "bid_depth_ordering": BID_DEPTH_ORDERING,
        "ask_depth_ordering": ASK_DEPTH_ORDERING,
    }
    return {key.encode("utf-8"): value.encode("utf-8") for key, value in values.items()}


def decoded_metadata(schema: pa.Schema, path: Path | None = None) -> dict[str, str]:
    """Decode UTF-8 schema metadata with a typed data error on corruption."""

    try:
        return {
            key.decode("utf-8"): value.decode("utf-8")
            for key, value in (schema.metadata or {}).items()
        }
    except UnicodeDecodeError as exc:
        label = "" if path is None else f" in {path}"
        raise ArrowDataError(f"invalid UTF-8 compact schema metadata{label}") from exc


def _validate_physical_schema(
    schema: pa.Schema,
    expected: pa.Schema,
    profile_label: str,
    path: Path | None,
) -> None:
    physical = schema.remove_metadata()
    if physical == expected:
        return
    label = "compact Arrow schema" if path is None else f"compact Arrow partition {path}"
    if physical.names != expected.names:
        raise ArrowDataError(
            f"{label} has incompatible fields/order {physical.names}; expected {expected.names}"
        )
    for actual, wanted in zip(physical, expected):
        if actual.type != wanted.type or actual.nullable != wanted.nullable:
            raise ArrowDataError(
                f"compact Arrow field {actual.name!r} in {label} has incompatible "
                f"type/nullability {actual.type}/{actual.nullable}; expected "
                f"{wanted.type}/{wanted.nullable}"
            )
    raise ArrowDataError(f"{label} does not match the canonical {profile_label} schema")


def validate_bbo_schema(schema: pa.Schema, path: Path | None = None) -> None:
    """Require the unchanged bbo_v2 names, order, types, and nullability."""

    _validate_physical_schema(schema, BBO_SCHEMA, BBO_SCHEMA_VERSION, path)


def validate_top5_schema(schema: pa.Schema, path: Path | None = None) -> None:
    """Require exact fixed top5_v1 names, order, types, and nullability."""

    _validate_physical_schema(schema, TOP5_SCHEMA, TOP5_SCHEMA_VERSION, path)


def _validate_integer_metadata(
    metadata: dict[str, str], key: str, *, require: bool
) -> int | None:
    value = metadata.get(key)
    if value is None:
        if require:
            raise ArrowDataError(f"compact Arrow schema is missing {key} metadata")
        return None
    if re.fullmatch(r"-?(?:0|[1-9][0-9]*)", value) is None:
        raise ArrowDataError(f"compact Arrow schema has invalid {key} metadata")
    return int(value)


def validate_schema_metadata(
    schema: pa.Schema,
    path: Path | None = None,
    *,
    require: bool = True,
) -> dict[str, str]:
    """Validate metadata under the historical bbo_v2 compatibility contract."""

    metadata = decoded_metadata(schema, path)
    declared = metadata.get("schema_version")
    if declared is None and require:
        raise ArrowDataError("compact Arrow schema is missing schema_version metadata")
    if declared is not None and declared != BBO_SCHEMA_VERSION:
        raise ArrowDataError(
            f"compact Arrow schema declares {declared!r}; expected {BBO_SCHEMA_VERSION!r}"
        )
    profile = metadata.get("profile")
    if profile is not None and profile != BBO_PROFILE:
        raise ArrowDataError(
            f"compact Arrow schema declares profile {profile!r}; expected {BBO_PROFILE!r}"
        )
    depth = _validate_integer_metadata(metadata, "depth_levels", require=False)
    if depth is not None and depth != 1:
        raise ArrowDataError("bbo_v2 depth_levels metadata must be 1")
    adjustment = metadata.get("local_timestamp_adjustment_ns")
    if adjustment is None and require:
        raise ArrowDataError(
            "compact Arrow schema is missing local_timestamp_adjustment_ns metadata"
        )
    if adjustment is not None:
        try:
            int(adjustment)
        except ValueError as exc:
            raise ArrowDataError(
                "compact Arrow schema has invalid local_timestamp_adjustment_ns metadata"
            ) from exc
    return metadata


def validate_top5_schema_metadata(
    schema: pa.Schema,
    path: Path | None = None,
    *,
    expected_depth_levels: int | None = None,
) -> dict[str, str]:
    """Validate every required top5_v1 identity and ordering metadata field."""

    metadata = decoded_metadata(schema, path)
    if metadata.get("schema_version") != TOP5_SCHEMA_VERSION:
        raise ArrowDataError(
            f"compact Arrow schema declares {metadata.get('schema_version')!r}; "
            f"expected {TOP5_SCHEMA_VERSION!r}"
        )
    if metadata.get("profile") != TOP5_PROFILE:
        raise ArrowDataError(
            f"compact Arrow schema declares profile {metadata.get('profile')!r}; "
            f"expected {TOP5_PROFILE!r}"
        )
    depth = _validate_integer_metadata(metadata, "depth_levels", require=True)
    if depth not in (2, 3, 4, 5):
        raise ArrowDataError("top5_v1 depth_levels metadata must be from 2 through 5")
    if expected_depth_levels is not None:
        try:
            expected = validate_depth_levels(expected_depth_levels)
        except ValueError as exc:
            raise ArrowDataError(str(exc)) from exc
        if expected == 1 or depth != expected:
            raise ArrowDataError(
                f"top5_v1 depth_levels metadata is {depth}; expected {expected}"
            )
    _validate_integer_metadata(
        metadata, "local_timestamp_adjustment_ns", require=True
    )
    for key, expected in (
        ("aggregation_policy", DEPTH_AGGREGATION_POLICY),
        ("bid_depth_ordering", BID_DEPTH_ORDERING),
        ("ask_depth_ordering", ASK_DEPTH_ORDERING),
    ):
        if metadata.get(key) != expected:
            raise ArrowDataError(
                f"compact Arrow schema has invalid or missing {key} metadata"
            )
    return metadata


def validate_compact_schema(
    schema: pa.Schema,
    path: Path | None = None,
    *,
    expected_depth_levels: int | None = None,
    require_metadata: bool = True,
) -> dict[str, str]:
    """Validate a physical profile and its metadata as one coherent contract."""

    metadata = decoded_metadata(schema, path)
    declared = metadata.get("schema_version")
    if declared is None and not require_metadata:
        declared = (
            BBO_SCHEMA_VERSION
            if schema.remove_metadata() == BBO_SCHEMA
            else TOP5_SCHEMA_VERSION
            if schema.remove_metadata() == TOP5_SCHEMA
            else None
        )
    if declared == BBO_SCHEMA_VERSION:
        validate_bbo_schema(schema, path)
        result = validate_schema_metadata(schema, path, require=require_metadata)
        if expected_depth_levels is not None:
            try:
                expected = validate_depth_levels(expected_depth_levels)
            except ValueError as exc:
                raise ArrowDataError(str(exc)) from exc
            if expected != 1:
                raise ArrowDataError(
                    "bbo_v2 schema disagrees with the requested depth_levels"
                )
        return result
    if declared == TOP5_SCHEMA_VERSION:
        validate_top5_schema(schema, path)
        return validate_top5_schema_metadata(
            schema, path, expected_depth_levels=expected_depth_levels
        )
    if declared is None:
        raise ArrowDataError("compact Arrow schema is missing schema_version metadata")
    raise ArrowDataError(f"unsupported compact Arrow schema version: {declared!r}")


__all__ = (
    "ASK_DEPTH_ORDERING",
    "BBO_PHYSICAL_FIELDS",
    "BBO_PROFILE",
    "BBO_SCHEMA",
    "BBO_SCHEMA_VERSION",
    "BID_DEPTH_ORDERING",
    "COMPACT_SCHEMA_VERSION",
    "DEPTH_AGGREGATION_POLICY",
    "MAX_DEPTH_LEVELS",
    "MISSING_LEVEL_NULL_POLICY",
    "PHYSICAL_FIELDS",
    "PROJECTED_COLUMNS",
    "PRICE_ONLY_QUANTITY_POLICY",
    "SLIM_ROW_DTYPE",
    "TOP5_PHYSICAL_FIELDS",
    "TOP5_PROFILE",
    "TOP5_SCHEMA",
    "TOP5_SCHEMA_VERSION",
    "TIMESTAMP_ORDERING_POLICY",
    "decoded_metadata",
    "physical_fields_for_depth_levels",
    "profile_for_depth_levels",
    "schema_for_depth_levels",
    "schema_version_for_depth_levels",
    "top5_schema_metadata",
    "validate_bbo_schema",
    "validate_compact_schema",
    "validate_depth_levels",
    "validate_schema_metadata",
    "validate_top5_schema",
    "validate_top5_schema_metadata",
)
