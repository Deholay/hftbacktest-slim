from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest

from hftbacktest_slim import (
    BBO_SCHEMA,
    TOP5_SCHEMA,
    CompactBuildConfig,
    CompactCacheBudgetError,
    CompactCacheError,
    CompactCacheStore,
    CompactSource,
)
from hftbacktest_slim.cache import builder
from hftbacktest_slim.cache.manifest import file_sha256
from hftbacktest_slim.cache.publication import (
    preflight_space,
    projected_build_space,
    projected_bytes,
)
from hftbacktest_slim.market_data import compact_partition_audit
from hftbacktest_slim.market_data.schema import top5_schema_metadata
from hftbacktest_slim.market_data.validation import validate_compact_partition


def _top5_values(depth: int) -> dict[str, list]:
    values: dict[str, list] = {
        "source_seq": [1, 2],
        "exch_ts": [100, 200],
        "local_ts_raw": [90, 205],
        "last_px": [100.0, 100.5],
        "total_volume": [10, 12],
        "tradable": [0, 1],
    }
    for side in ("bid", "ask"):
        for level in range(1, 6):
            enabled = level <= depth
            if side == "bid":
                price = 102.0 - level
            else:
                price = 99.0 + level
            values[f"{side}_px_{level}"] = [price if enabled else None, None]
            values[f"{side}_qty_{level}"] = [float(level) if enabled else None, None]
    return values


def _write_top5(
    path: Path,
    depth: int,
    *,
    values: dict[str, list] | None = None,
    metadata_updates: dict[bytes, bytes | None] | None = None,
) -> Path:
    metadata = top5_schema_metadata(depth, local_timestamp_adjustment_ns=10)
    metadata.update(
        {
            b"trade_date": b"2026-03-02",
            b"source": b"stock",
            b"symbol": b"0050",
            b"base_latency_ns": b"0",
        }
    )
    for key, value in (metadata_updates or {}).items():
        if value is None:
            metadata.pop(key, None)
        else:
            metadata[key] = value
    table = pa.Table.from_pydict(
        _top5_values(depth) if values is None else values,
        schema=TOP5_SCHEMA.with_metadata(metadata),
    )
    with path.open("wb") as sink, ipc.new_file(sink, table.schema) as writer:
        for batch in table.to_batches(max_chunksize=1):
            writer.write_batch(batch)
    return path


@pytest.mark.parametrize("depth", [2, 3, 5])
def test_valid_topn_content_and_statistics(depth: int, tmp_path: Path) -> None:
    path = _write_top5(tmp_path / f"depth-{depth}.arrow", depth)
    result = validate_compact_partition(
        path,
        expected_depth_levels=depth,
        trade_date="2026-03-02",
        source="stock",
        symbol="0050",
        require_identity_metadata=True,
    )
    assert result.facts["rows"] == 2
    assert result.facts["profile"] == "top5"
    assert result.facts["schema_version"] == "top5_v1"
    assert result.facts["depth_levels"] == depth
    assert result.facts["non_tradable_rows"] == 1
    assert result.facts["depth"]["bid"]["1"] == {
        "valid_rows": 1,
        "null_rows": 1,
    }
    assert str(depth + 1) not in result.facts["depth"]["bid"]
    assert result.facts["depth"]["complete_book_rows"] == 1
    # Row zero is locked/crossed and row one is empty; both remain valid.
    assert result.facts["depth"]["two_sided_rows"] == 1
    assert result.facts["depth"]["empty_book_rows"] == 1


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (lambda v: v["bid_qty_2"].__setitem__(0, None), "bid level 2: price without quantity"),
        (lambda v: v["ask_px_2"].__setitem__(0, None), "ask level 2: quantity without price"),
        (lambda v: v["bid_px_1"].__setitem__(0, np.nan), "finite and greater than zero"),
        (lambda v: v["ask_qty_1"].__setitem__(0, np.inf), "finite and greater than zero"),
        (lambda v: v["bid_px_1"].__setitem__(0, 0.0), "finite and greater than zero"),
        (lambda v: v["ask_qty_1"].__setitem__(0, -1.0), "finite and greater than zero"),
        (lambda v: v["bid_px_5"].__setitem__(0, 95.0), "bid level 5: disabled level"),
        (lambda v: v["bid_qty_5"].__setitem__(0, 1.0), "bid level 5: disabled level"),
        (
            lambda v: (
                v["ask_px_2"].__setitem__(0, None),
                v["ask_qty_2"].__setitem__(0, None),
            ),
            "ask level 3: level is populated after a null level",
        ),
        (lambda v: v["bid_px_2"].__setitem__(0, 102.0), "not strictly descending"),
        (lambda v: v["ask_px_2"].__setitem__(0, 99.0), "not strictly ascending"),
        (lambda v: v["bid_px_2"].__setitem__(0, 101.0), "repeated prices are invalid"),
        (lambda v: v["tradable"].__setitem__(0, 2), "outside 0 or 1"),
    ],
)
def test_topn_content_corruption_is_actionable(
    tmp_path: Path, mutate, match: str
) -> None:
    values = _top5_values(3)
    mutate(values)
    path = _write_top5(tmp_path / "bad.arrow", 3, values=values)
    with pytest.raises(CompactCacheError, match=match):
        validate_compact_partition(
            path,
            expected_depth_levels=3,
            trade_date="2026-03-02",
            source="stock",
            symbol="0050",
        )


def test_profile_metadata_disagreement_is_actionable(tmp_path: Path) -> None:
    path = _write_top5(
        tmp_path / "bad-metadata.arrow",
        3,
        metadata_updates={b"profile": b"bbo"},
    )
    with pytest.raises(CompactCacheError, match="date=2026-03-02.*profile"):
        validate_compact_partition(
            path,
            expected_depth_levels=3,
            trade_date="2026-03-02",
            source="stock",
            symbol="0050",
        )


def test_batch_validation_never_uses_read_all_and_keeps_cross_batch_state(
    tmp_path: Path,
) -> None:
    path = _write_top5(tmp_path / "batches.arrow", 3)
    from hftbacktest_slim.market_data import validation as validation_module

    original_open = ipc.open_file

    class ReaderProxy:
        def __init__(self, reader):
            self._reader = reader
            self.schema = reader.schema
            self.num_record_batches = reader.num_record_batches

        def get_batch(self, index):
            return self._reader.get_batch(index)

        def read_all(self):
            raise AssertionError("bounded validation must not call read_all")

    with patch.object(
        validation_module.ipc,
        "open_file",
        side_effect=lambda handle: ReaderProxy(original_open(handle)),
    ):
        assert validate_compact_partition(path).facts["rows"] == 2

    values = _top5_values(3)
    values["source_seq"] = [2, 1]
    bad = _write_top5(tmp_path / "cross-batch.arrow", 3, values=values)
    with pytest.raises(CompactCacheError, match="source_seq is not strictly increasing"):
        validate_compact_partition(bad)


def test_bbo_nan_missing_semantics_and_equivalent_facts(tmp_path: Path) -> None:
    metadata = {
        b"schema_version": b"bbo_v2",
        b"profile": b"bbo",
        b"depth_levels": b"1",
        b"local_timestamp_adjustment_ns": b"0",
    }
    table = pa.Table.from_pydict(
        {
            "source_seq": [1, 2, 3],
            "exch_ts": [100, 200, 300],
            "local_ts_raw": [100, 200, 300],
            "bid_px": [100.0, 100.0, np.nan],
            "ask_px": [101.0, np.nan, np.nan],
            "bid_qty": [1.0, 1.0, np.nan],
            "ask_qty": [1.0, np.nan, np.nan],
            "last_px": [100.0, 100.0, 100.0],
            "total_volume": [1, 1, 1],
            "tradable": [1, 0, 1],
        },
        schema=BBO_SCHEMA.with_metadata(metadata),
    )
    path = tmp_path / "bbo.arrow"
    with path.open("wb") as sink, ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    facts = validate_compact_partition(
        path, expected_depth_levels=1, require_identity_metadata=True
    ).facts
    assert facts["depth"]["two_sided_rows"] == 1
    assert facts["depth"]["bid_only_rows"] == 1
    assert facts["depth"]["empty_book_rows"] == 1
    assert facts["non_tradable_rows"] == 1


@pytest.mark.parametrize(
    ("bid_px", "bid_qty", "match"),
    [
        (100.0, np.nan, "bid level 1: price without quantity"),
        (np.nan, 1.0, "bid level 1: quantity without price"),
        (np.inf, 1.0, "finite and greater than zero"),
        (0.0, 1.0, "finite and greater than zero"),
        (100.0, -1.0, "finite and greater than zero"),
    ],
)
def test_bbo_content_validation_applies_paired_positive_value_rules(
    tmp_path: Path, bid_px: float, bid_qty: float, match: str
) -> None:
    schema = BBO_SCHEMA.with_metadata(
        {
            b"schema_version": b"bbo_v2",
            b"profile": b"bbo",
            b"depth_levels": b"1",
            b"local_timestamp_adjustment_ns": b"0",
        }
    )
    table = pa.Table.from_pydict(
        {
            "source_seq": [1],
            "exch_ts": [100],
            "local_ts_raw": [100],
            "bid_px": [bid_px],
            "ask_px": [101.0],
            "bid_qty": [bid_qty],
            "ask_qty": [1.0],
            "last_px": [100.0],
            "total_volume": [1],
            "tradable": [1],
        },
        schema=schema,
    )
    path = tmp_path / "bad-bbo.arrow"
    with path.open("wb") as sink, ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    with pytest.raises(CompactCacheError, match=match):
        validate_compact_partition(path, expected_depth_levels=1)


def _raw_table() -> pa.Table:
    values: dict[str, list] = {
        "symbol": ["0050", "0050"],
        "exchtime": [100, 200],
        "localtime": [100, 200],
        "status": [0, 0],
        "last_price": [100.0, 100.0],
        "total_volume": [1, 2],
    }
    for level in range(1, 6):
        values[f"bid_price{level}"] = [101.0 - level, 101.0 - level]
        values[f"ask_price{level}"] = [99.0 + level, 99.0 + level]
        values[f"bid_volume{level}"] = [1.0, 1.0]
        values[f"ask_volume{level}"] = [1.0, 1.0]
    return pa.table(values)


def _store(tmp_path: Path, *, depth: int = 3, **overrides) -> CompactCacheStore:
    values = {
        "cache_root": tmp_path / "cache",
        "depth_levels": depth,
        "batch_rows": 1,
        "max_cache_bytes": 1024**3,
        "min_free_bytes": 0,
    }
    values.update(overrides)
    return CompactCacheStore(CompactBuildConfig(**values))


def test_phase3_manifest_contract_and_deterministic_empty_statistics(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    pq.write_table(_raw_table(), raw, row_group_size=1)
    store = _store(tmp_path)
    manifest = store.build_date(
        "2026-03-02", [CompactSource("stock", (raw,), ("0050", "9999"))]
    )
    for key in (
        "aggregation_policy",
        "bid_depth_ordering",
        "ask_depth_ordering",
        "missing_level_null_policy",
        "price_only_quantity_policy_by_source",
        "volume_scale_by_source",
        "projected_source_columns",
        "source_fingerprints",
        "implementation_fingerprint",
        "compression",
        "timestamp_ordering_policy",
        "session_policy",
    ):
        assert key in manifest
    source = manifest["sources"]["stock"]
    assert source["scan_count"] == 1
    assert source["statistics_compact_read_count"] == 2
    assert source["requested_symbol_count"] == 2
    assert source["valid_symbol_count"] == 2
    assert source["empty_symbol_count"] == 1
    assert source["missing_symbol_count"] == 0
    empty = source["symbols"]["9999"]
    assert empty["rows"] == 0 and empty["empty"] is True
    assert empty["depth"]["bid"]["1"] == {"valid_rows": 0, "null_rows": 0}
    assert source["depth"]["bid"]["1"]["valid_rows"] == 2


def test_manifest_symbol_and_source_statistic_mismatches_are_rejected(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    pq.write_table(_raw_table(), raw)
    store = _store(tmp_path)
    manifest = store.build_date(
        "2026-03-02", [CompactSource("stock", (raw,), ("0050",))]
    )
    date_path = store.date_path("2026-03-02")
    source_path = date_path / "source=stock" / "manifest.json"
    top_path = date_path / "manifest.json"
    manifest["sources"]["stock"]["symbols"]["0050"]["depth"]["bid"]["1"][
        "valid_rows"
    ] += 1
    source_path.write_text(json.dumps(manifest["sources"]["stock"]), encoding="utf-8")
    top_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(CompactCacheError, match="symbol manifest depth"):
        store.validate_date("2026-03-02")

    rebuilt = _store(tmp_path, rebuild=True, compression="zstd").build_date(
        "2026-03-02", [CompactSource("stock", (raw,), ("0050",))]
    )
    date_path = store.date_path("2026-03-02")
    source_path = date_path / "source=stock" / "manifest.json"
    top_path = date_path / "manifest.json"
    rebuilt["sources"]["stock"]["depth"]["bid"]["1"]["valid_rows"] += 1
    source_path.write_text(json.dumps(rebuilt["sources"]["stock"]), encoding="utf-8")
    top_path.write_text(json.dumps(rebuilt), encoding="utf-8")
    with pytest.raises(CompactCacheError, match="source aggregate depth mismatch"):
        store.validate_date("2026-03-02")


def test_missing_phase3_metadata_invalidates_without_raw_scan(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    pq.write_table(_raw_table(), raw)
    source = CompactSource("stock", (raw,), ("0050",))
    store = _store(tmp_path)
    store.build_date("2026-03-02", [source])
    manifest_path = store.date_path("2026-03-02") / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    del payload["missing_level_null_policy"]
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with patch.object(builder, "iter_source_batches", wraps=builder.iter_source_batches) as scan:
        with pytest.raises(CompactCacheError, match="incompatible identity"):
            store.build_date("2026-03-02", [source])
    assert scan.call_count == 0


def test_corrupt_content_cannot_publish_and_raw_scan_count_stays_one(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    pq.write_table(_raw_table(), raw, row_group_size=1)
    store = _store(tmp_path)
    with patch.object(
        builder,
        "validate_compact_partition",
        side_effect=CompactCacheError("corrupt compact content"),
    ), patch.object(builder, "iter_source_batches", wraps=builder.iter_source_batches) as scan:
        with pytest.raises(CompactCacheError, match="corrupt compact content"):
            store.build_date(
                "2026-03-02", [CompactSource("stock", (raw,), ("0050",))]
            )
    assert scan.call_count == 1
    assert not store.date_path("2026-03-02").exists()
    assert not list(store.namespace_root.glob(".tmp-2026-03-02-*"))


def test_top5_audit_is_profile_aware_without_claiming_depth_events(tmp_path: Path) -> None:
    facts = compact_partition_audit(_write_top5(tmp_path / "audit.arrow", 3))
    assert facts["profile"] == "top5"
    assert facts["schema_version"] == "top5_v1"
    assert facts["depth_levels"] == 3
    assert facts["min_price"] == 99.0
    assert facts["max_price"] == 102.0
    assert facts["depth"]["bid"]["3"]["valid_rows"] == 1
    assert facts["depth_events"] is None


def test_profile_space_estimate_includes_completed_and_temporary_requirements(
    tmp_path: Path,
) -> None:
    assert projected_bytes(10, 1) == 1_152
    assert projected_build_space(10, 1) == {
        "projected_completed_bytes": 1_152,
        "largest_temporary_date_bytes": 1_152,
        "required_additional_bytes": 2_304,
    }
    assert projected_build_space(10, 2) == projected_build_space(10, 5)
    root = tmp_path / "cache"
    root.mkdir()
    (root / "other-namespace.bin").write_bytes(b"x" * 100)
    identity = {"sources": [{"files": [{"rows": 1}]}]}
    config = CompactBuildConfig(
        cache_root=root,
        depth_levels=2,
        max_cache_bytes=100 + (2 * projected_bytes(1, 2)) - 1,
        min_free_bytes=0,
    )
    with pytest.raises(CompactCacheBudgetError, match="existing=100.*largest_temporary"):
        preflight_space(root, config, identity)

    reserve = CompactBuildConfig(
        cache_root=root,
        depth_levels=2,
        max_cache_bytes=10_000,
        min_free_bytes=1,
    )
    required = projected_build_space(1, 2)["required_additional_bytes"]
    with patch(
        "hftbacktest_slim.cache.publication.shutil.disk_usage",
        return_value=SimpleNamespace(free=required),
    ):
        with pytest.raises(CompactCacheBudgetError, match="reserve"):
            preflight_space(root, reserve, identity)
