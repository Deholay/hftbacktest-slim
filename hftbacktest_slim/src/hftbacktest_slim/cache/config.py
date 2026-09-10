"""Configuration and resource defaults for compact cache builds."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

from ..market_data.schema import (
    BBO_PROFILE,
    profile_for_depth_levels,
    schema_version_for_depth_levels,
    validate_depth_levels,
)


COMPACT_BUILDER_VERSION = 5
COMPACT_ROW_ESTIMATE_BYTES = 96
# top5_v1 has 25 eight-byte values, one uint8, and 26 nullable validity bits.
# Round the uncompressed payload up to a 64-byte boundary for Arrow/alignment
# overhead; the existing 1.20 preflight safety factor is applied separately.
_TOP5_PAYLOAD_BYTES = (25 * 8) + 1 + math.ceil(26 / 8)
TOP5_ROW_ESTIMATE_BYTES = math.ceil(_TOP5_PAYLOAD_BYTES / 64) * 64
DEFAULT_MAX_CACHE_BYTES = 200 * 1024**3
DEFAULT_MIN_FREE_BYTES = 200 * 1024**3
DEFAULT_BATCH_ROWS = 131_072


@dataclass(frozen=True)
class CompactSource:
    kind: str
    paths: tuple[Path, ...]
    symbols: tuple[str, ...]
    status_allow: tuple[str, ...] = ()
    price_only_depth_qty: float | None = None
    volume_scale: float = 1.0

    def __post_init__(self) -> None:
        if not self.kind:
            raise ValueError("compact source kind must be non-empty")
        if Path(self.kind).name != self.kind or any(
            separator in self.kind for separator in ("/", "\\")
        ):
            raise ValueError("compact source kind must not contain path separators")
        if not self.paths:
            raise ValueError("compact source requires at least one path")
        if len(set(self.symbols)) != len(self.symbols):
            raise ValueError("compact source symbols must be unique")
        if any(
            not symbol
            or Path(str(symbol)).name != str(symbol)
            or any(separator in str(symbol) for separator in ("/", "\\"))
            for symbol in self.symbols
        ):
            raise ValueError("compact source symbols must be non-empty path components")
        if not math.isfinite(float(self.volume_scale)):
            raise ValueError("volume_scale must be finite")
        if self.price_only_depth_qty is not None and not math.isfinite(
            float(self.price_only_depth_qty)
        ):
            raise ValueError("price_only_depth_qty must be finite when provided")


@dataclass(frozen=True)
class CompactBuildConfig:
    cache_root: Path
    compression: str = "lz4"
    profile: str = "bbo"
    timezone: str = "Asia/Taipei"
    session_start_ns: int | None = None
    session_end_ns: int | None = None
    base_latency_ns: int = 0
    batch_rows: int = DEFAULT_BATCH_ROWS
    max_cache_bytes: int = DEFAULT_MAX_CACHE_BYTES
    min_free_bytes: int = DEFAULT_MIN_FREE_BYTES
    rebuild: bool = False
    depth_levels: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "cache_root", Path(self.cache_root))
        depth = validate_depth_levels(self.depth_levels)
        canonical_profile = profile_for_depth_levels(depth)
        if self.profile not in {BBO_PROFILE, canonical_profile}:
            raise ValueError(
                "profile is compatibility-only; depth_levels selects bbo or top5"
            )
        # Existing callers may keep passing profile="bbo". For depth > 1 it
        # is treated as the legacy default, not as an independent selector.
        object.__setattr__(self, "profile", canonical_profile)
        if self.compression not in {"lz4", "zstd", "none"}:
            raise ValueError("compression must be lz4, zstd, or none")
        if self.batch_rows <= 0:
            raise ValueError("batch_rows must be positive")
        if self.max_cache_bytes < 0:
            raise ValueError("max_cache_bytes must be non-negative")
        if self.min_free_bytes < 0:
            raise ValueError("min_free_bytes must be non-negative")
        if (
            self.session_start_ns is not None
            and self.session_end_ns is not None
            and self.session_start_ns > self.session_end_ns
        ):
            raise ValueError("session_start_ns must not exceed session_end_ns")

    @property
    def schema_version(self) -> str:
        return schema_version_for_depth_levels(self.depth_levels)


def compact_row_estimate_bytes(depth_levels: object) -> int:
    """Return the physical-profile row estimate before the 1.20 safety factor.

    ``top5_v1`` is a fixed five-level schema, so disabled configured levels do
    not reduce this estimate.
    """

    return (
        COMPACT_ROW_ESTIMATE_BYTES
        if validate_depth_levels(depth_levels) == 1
        else TOP5_ROW_ESTIMATE_BYTES
    )


def cache_namespace_components(depth_levels: object) -> tuple[str, ...]:
    """Return safe, deterministic path components for the selected profile."""

    depth = validate_depth_levels(depth_levels)
    if depth == 1:
        return ()
    components = ("profile=top5_v1", f"depth_levels={depth}")
    if any(
        Path(component).name != component
        or any(separator in component for separator in ("/", "\\"))
        for component in components
    ):
        raise ValueError("invalid compact cache namespace component")
    return components


__all__ = (
    "COMPACT_BUILDER_VERSION",
    "COMPACT_ROW_ESTIMATE_BYTES",
    "TOP5_ROW_ESTIMATE_BYTES",
    "DEFAULT_BATCH_ROWS",
    "DEFAULT_MAX_CACHE_BYTES",
    "DEFAULT_MIN_FREE_BYTES",
    "CompactBuildConfig",
    "CompactSource",
    "cache_namespace_components",
    "compact_row_estimate_bytes",
)
