"""Neutral public boundary for the project-owned slim replay runtime."""

from .api import (
    AbiMismatchError,
    ArrowDataError,
    AssetConfig,
    CompactCacheBudgetError,
    CompactCacheError,
    CompactValidationError,
    DepthView,
    EngineClosedError,
    FeedLatency,
    NativeCallError,
    NativeLibraryError,
    NativeLibraryNotFoundError,
    NATIVE_ABI_VERSION,
    OrderLatency,
    OrderStatus,
    OrderSubmissionError,
    OrderType,
    OrderView,
    Side,
    SlimConfigurationError,
    SlimError,
    SlimEngine,
    SLIM_ENGINE_VERSION,
    TimeInForce,
    UnsupportedCapabilityError,
    __version__,
)


_COMPACT_EXPORTS = {
    "BBO_SCHEMA": (".market_data.schema", "BBO_SCHEMA"),
    "BBO_SCHEMA_VERSION": (".market_data.schema", "BBO_SCHEMA_VERSION"),
    "COMPACT_BUILDER_VERSION": (".cache.config", "COMPACT_BUILDER_VERSION"),
    "COMPACT_ROW_ESTIMATE_BYTES": (
        ".cache.config",
        "COMPACT_ROW_ESTIMATE_BYTES",
    ),
    "COMPACT_SCHEMA_VERSION": (".market_data.schema", "COMPACT_SCHEMA_VERSION"),
    "TOP5_PHYSICAL_FIELDS": (".market_data.schema", "TOP5_PHYSICAL_FIELDS"),
    "TOP5_ROW_ESTIMATE_BYTES": (".cache.config", "TOP5_ROW_ESTIMATE_BYTES"),
    "TOP5_SCHEMA": (".market_data.schema", "TOP5_SCHEMA"),
    "TOP5_SCHEMA_VERSION": (".market_data.schema", "TOP5_SCHEMA_VERSION"),
    "CompactBuildConfig": (".cache.config", "CompactBuildConfig"),
    "CompactCacheStore": (".cache.store", "CompactCacheStore"),
    "CompactSource": (".cache.config", "CompactSource"),
    "compact_row_estimate_bytes": (
        ".cache.config",
        "compact_row_estimate_bytes",
    ),
    "profile_for_depth_levels": (
        ".market_data.schema",
        "profile_for_depth_levels",
    ),
    "schema_for_depth_levels": (
        ".market_data.schema",
        "schema_for_depth_levels",
    ),
    "schema_version_for_depth_levels": (
        ".market_data.schema",
        "schema_version_for_depth_levels",
    ),
    "top5_schema_metadata": (".market_data.schema", "top5_schema_metadata"),
    "validate_compact_schema": (".market_data.schema", "validate_compact_schema"),
    "validate_depth_levels": (".market_data.schema", "validate_depth_levels"),
    "validate_top5_schema": (".market_data.schema", "validate_top5_schema"),
    "validate_top5_schema_metadata": (
        ".market_data.schema",
        "validate_top5_schema_metadata",
    ),
    "aggregate_depth_side": (".market_data.normalize", "_aggregate_depth_side"),
    "normalized_bbo_from_depth_columns": (
        ".market_data.normalize",
        "normalized_bbo_from_depth_columns",
    ),
    "normalized_depth_from_depth_columns": (
        ".market_data.normalize",
        "normalized_depth_from_depth_columns",
    ),
}


def __getattr__(name: str):
    """Load PyArrow/Numba-backed cache objects only when requested."""

    target = _COMPACT_EXPORTS.get(name)
    if target is None:
        raise AttributeError(name)
    from importlib import import_module

    module = import_module(target[0], __name__)
    value = getattr(module, target[1])
    globals()[name] = value
    return value

__all__ = (
    "AbiMismatchError",
    "ArrowDataError",
    "AssetConfig",
    "BBO_SCHEMA",
    "BBO_SCHEMA_VERSION",
    "COMPACT_BUILDER_VERSION",
    "COMPACT_ROW_ESTIMATE_BYTES",
    "COMPACT_SCHEMA_VERSION",
    "CompactBuildConfig",
    "CompactCacheBudgetError",
    "CompactCacheError",
    "CompactCacheStore",
    "CompactSource",
    "CompactValidationError",
    "TOP5_PHYSICAL_FIELDS",
    "TOP5_ROW_ESTIMATE_BYTES",
    "TOP5_SCHEMA",
    "TOP5_SCHEMA_VERSION",
    "DepthView",
    "EngineClosedError",
    "FeedLatency",
    "NativeCallError",
    "NativeLibraryError",
    "NativeLibraryNotFoundError",
    "NATIVE_ABI_VERSION",
    "OrderLatency",
    "OrderStatus",
    "OrderSubmissionError",
    "OrderType",
    "OrderView",
    "Side",
    "SlimConfigurationError",
    "SlimError",
    "SlimEngine",
    "SLIM_ENGINE_VERSION",
    "TimeInForce",
    "UnsupportedCapabilityError",
    "__version__",
    "aggregate_depth_side",
    "compact_row_estimate_bytes",
    "normalized_bbo_from_depth_columns",
    "normalized_depth_from_depth_columns",
    "profile_for_depth_levels",
    "schema_for_depth_levels",
    "schema_version_for_depth_levels",
    "top5_schema_metadata",
    "validate_compact_schema",
    "validate_depth_levels",
    "validate_top5_schema",
    "validate_top5_schema_metadata",
)
