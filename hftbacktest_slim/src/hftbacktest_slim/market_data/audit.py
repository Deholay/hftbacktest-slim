"""Provider- and strategy-neutral compact partition facts."""

from __future__ import annotations

from os import PathLike
from pathlib import Path
from typing import Any

from .validation import validate_compact_partition


def compact_partition_audit(
    data_path: str | PathLike[str] | Path,
) -> dict[str, Any]:
    """Return raw compact facts without applying any strategy tick schedule."""

    path = Path(data_path)
    validation = validate_compact_partition(path)
    facts = validation.facts
    return {
        **{
            key: facts[key]
            for key in (
                "rows",
                "profile",
                "schema_version",
                "depth_levels",
                "first_exch_ts",
                "last_exch_ts",
                "raw_min_feed_latency_ns",
                "raw_max_feed_latency_ns",
                "local_timestamp_adjustment_ns",
                "min_latency_ns",
                "max_latency_ns",
                "min_price",
                "max_price",
                "trade_events",
                "non_tradable_rows",
                "depth",
            )
        },
        "depth_events": None,
        "metadata": validation.metadata,
        "schema_valid": True,
    }


__all__ = ("compact_partition_audit",)
