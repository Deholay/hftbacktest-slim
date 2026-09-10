from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hftbacktest_slim import (
    BBO_SCHEMA,
    BBO_SCHEMA_VERSION,
    COMPACT_SCHEMA_VERSION,
    TOP5_PHYSICAL_FIELDS,
    TOP5_ROW_ESTIMATE_BYTES,
    TOP5_SCHEMA,
    TOP5_SCHEMA_VERSION,
    CompactBuildConfig,
    CompactCacheError,
    CompactCacheStore,
    CompactSource,
    compact_row_estimate_bytes,
    profile_for_depth_levels,
    schema_for_depth_levels,
    schema_version_for_depth_levels,
    top5_schema_metadata,
    validate_compact_schema,
    validate_top5_schema,
)
from hftbacktest_slim.cache import builder
from hftbacktest_slim.cache.manifest import build_identity, canonical_sha256
from hftbacktest_slim.cache.publication import preflight_space, projected_bytes
from hftbacktest_slim.errors import ArrowDataError
from hftbacktest_slim.market_data.schema import PHYSICAL_FIELDS, SLIM_ROW_DTYPE
from hftbacktest_slim.market_data.schema import validate_schema_metadata


EXPECTED_BBO_FIELDS = (
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
EXPECTED_TOP5_NAMES = (
    "source_seq",
    "exch_ts",
    "local_ts_raw",
    *(f"bid_px_{level}" for level in range(1, 6)),
    *(f"bid_qty_{level}" for level in range(1, 6)),
    *(f"ask_px_{level}" for level in range(1, 6)),
    *(f"ask_qty_{level}" for level in range(1, 6)),
    "last_px",
    "total_volume",
    "tradable",
)


@pytest.mark.parametrize("depth", [1, 2, 3, 4, 5])
def test_compact_config_accepts_only_supported_integer_depths(
    tmp_path: Path, depth: int
) -> None:
    config = CompactBuildConfig(cache_root=tmp_path, depth_levels=depth)
    assert config.depth_levels == depth
    assert config.profile == ("bbo" if depth == 1 else "top5")


@pytest.mark.parametrize("depth", [True, False, 1.0, "1", 0, -1, 6, 100])
def test_compact_config_rejects_non_integer_or_out_of_range_depths(
    tmp_path: Path, depth: object
) -> None:
    with pytest.raises(ValueError, match="integer from 1 through 5"):
        CompactBuildConfig(cache_root=tmp_path, depth_levels=depth)  # type: ignore[arg-type]


def test_profile_is_derived_from_depth_and_legacy_bbo_input_is_compatible(
    tmp_path: Path,
) -> None:
    assert CompactBuildConfig(cache_root=tmp_path, profile="bbo").profile == "bbo"
    assert (
        CompactBuildConfig(
            cache_root=tmp_path, profile="bbo", depth_levels=3
        ).profile
        == "top5"
    )
    assert (
        CompactBuildConfig(
            cache_root=tmp_path, profile="top5", depth_levels=3
        ).profile
        == "top5"
    )
    with pytest.raises(ValueError, match="compatibility-only"):
        CompactBuildConfig(cache_root=tmp_path, profile="top5", depth_levels=1)
    positional = CompactBuildConfig(tmp_path, "zstd", "bbo", "UTC")
    assert positional.timezone == "UTC"
    assert positional.depth_levels == 1


def test_bbo_schema_version_and_native_dtype_remain_exactly_unchanged() -> None:
    assert BBO_SCHEMA_VERSION == COMPACT_SCHEMA_VERSION == "bbo_v2"
    assert PHYSICAL_FIELDS == EXPECTED_BBO_FIELDS
    assert tuple(BBO_SCHEMA.names) == tuple(name for name, _ in EXPECTED_BBO_FIELDS)
    assert [(field.type, field.nullable) for field in BBO_SCHEMA] == [
        (data_type, True) for _, data_type in EXPECTED_BBO_FIELDS
    ]
    assert SLIM_ROW_DTYPE.names == tuple(BBO_SCHEMA.names)
    assert SLIM_ROW_DTYPE.itemsize == 80
    assert [SLIM_ROW_DTYPE.fields[name][1] for name in BBO_SCHEMA.names] == [
        *range(0, 72, 8),
        72,
    ]


def test_existing_bbo_metadata_contract_remains_compatible() -> None:
    schema = BBO_SCHEMA.with_metadata(
        {
            b"schema_version": b"bbo_v2",
            b"local_timestamp_adjustment_ns": b"0",
        }
    )
    assert validate_schema_metadata(schema) == {
        "schema_version": "bbo_v2",
        "local_timestamp_adjustment_ns": "0",
    }
    validate_compact_schema(schema, expected_depth_levels=1)

    conflicting = schema.with_metadata(
        {
            **(schema.metadata or {}),
            b"profile": b"top5",
        }
    )
    with pytest.raises(ArrowDataError, match="profile"):
        validate_compact_schema(conflicting, expected_depth_levels=1)


def test_top5_schema_has_exact_fixed_order_types_and_nullability() -> None:
    assert TOP5_SCHEMA_VERSION == "top5_v1"
    assert tuple(TOP5_SCHEMA.names) == EXPECTED_TOP5_NAMES
    assert tuple(name for name, _ in TOP5_PHYSICAL_FIELDS) == EXPECTED_TOP5_NAMES
    expected_types = (
        pa.uint64(),
        pa.int64(),
        pa.int64(),
        *(pa.float64() for _ in range(20)),
        pa.float64(),
        pa.int64(),
        pa.uint8(),
    )
    assert tuple(field.type for field in TOP5_SCHEMA) == expected_types
    assert all(field.nullable for field in TOP5_SCHEMA)
    validate_top5_schema(TOP5_SCHEMA)


@pytest.mark.parametrize("depth", [2, 3, 5])
def test_top5_metadata_validates_for_each_selected_depth(depth: int) -> None:
    schema = TOP5_SCHEMA.with_metadata(
        top5_schema_metadata(depth, local_timestamp_adjustment_ns=17)
    )
    metadata = validate_compact_schema(schema, expected_depth_levels=depth)
    assert metadata["schema_version"] == "top5_v1"
    assert metadata["profile"] == "top5"
    assert metadata["depth_levels"] == str(depth)
    assert metadata["local_timestamp_adjustment_ns"] == "17"


@pytest.mark.parametrize(
    ("key", "value", "match"),
    [
        ("depth_levels", None, "depth_levels"),
        ("depth_levels", "1", "2 through 5"),
        ("depth_levels", "6", "2 through 5"),
        ("depth_levels", "2.0", "invalid depth_levels"),
        ("profile", "bbo", "profile"),
        ("schema_version", "bbo_v2", "fields/order"),
        ("aggregation_policy", "other", "aggregation_policy"),
        ("bid_depth_ordering", None, "bid_depth_ordering"),
        ("ask_depth_ordering", "descending", "ask_depth_ordering"),
        ("local_timestamp_adjustment_ns", "1.5", "local_timestamp"),
    ],
)
def test_top5_metadata_rejects_missing_malformed_or_conflicting_values(
    key: str, value: str | None, match: str
) -> None:
    metadata = top5_schema_metadata(2, local_timestamp_adjustment_ns=0)
    encoded_key = key.encode()
    if value is None:
        metadata.pop(encoded_key)
    else:
        metadata[encoded_key] = value.encode()
    with pytest.raises(ArrowDataError, match=match):
        validate_compact_schema(
            TOP5_SCHEMA.with_metadata(metadata), expected_depth_levels=2
        )


def test_top5_validation_rejects_field_order_type_and_nullability() -> None:
    metadata = top5_schema_metadata(2, local_timestamp_adjustment_ns=0)
    reordered = pa.schema(
        [TOP5_SCHEMA.field(1), TOP5_SCHEMA.field(0), *list(TOP5_SCHEMA)[2:]],
        metadata=metadata,
    )
    wrong_type_fields = list(TOP5_SCHEMA)
    wrong_type_fields[3] = pa.field("bid_px_1", pa.float32(), nullable=True)
    wrong_type = pa.schema(wrong_type_fields, metadata=metadata)
    nonnullable_fields = list(TOP5_SCHEMA)
    nonnullable_fields[3] = pa.field("bid_px_1", pa.float64(), nullable=False)
    nonnullable = pa.schema(nonnullable_fields, metadata=metadata)
    for schema in (reordered, wrong_type, nonnullable):
        with pytest.raises(ArrowDataError):
            validate_compact_schema(schema, expected_depth_levels=2)


def test_profile_and_schema_selection_is_depth_driven() -> None:
    assert profile_for_depth_levels(1) == "bbo"
    assert schema_version_for_depth_levels(1) == "bbo_v2"
    assert schema_for_depth_levels(1) is BBO_SCHEMA
    for depth in (2, 3, 4, 5):
        assert profile_for_depth_levels(depth) == "top5"
        assert schema_version_for_depth_levels(depth) == "top5_v1"
        assert schema_for_depth_levels(depth) is TOP5_SCHEMA


def test_build_identity_and_checksum_are_distinct_for_each_depth(tmp_path: Path) -> None:
    raw = tmp_path / "source.parquet"
    pq.write_table(pa.table({"symbol": ["0050"]}), raw)
    source = CompactSource("stock", (raw,), ("0050",))
    identities = {
        depth: build_identity(
            "2026-03-02",
            [source],
            CompactBuildConfig(cache_root=tmp_path / "cache", depth_levels=depth),
        )
        for depth in (1, 2, 3, 5)
    }
    assert identities[1]["profile"] == "bbo"
    assert identities[1]["schema_version"] == "bbo_v2"
    assert all(identities[depth]["profile"] == "top5" for depth in (2, 3, 5))
    assert all(
        identities[depth]["depth_levels"] == depth for depth in (1, 2, 3, 5)
    )
    assert len({canonical_sha256(value) for value in identities.values()}) == 4


def test_default_and_topn_date_paths_use_noncolliding_namespaces(
    tmp_path: Path,
) -> None:
    root = tmp_path / "cache"
    paths = {
        depth: CompactCacheStore(
            CompactBuildConfig(cache_root=root, depth_levels=depth)
        ).date_path("2026-03-02")
        for depth in (1, 2, 3, 5)
    }
    assert paths[1] == root / "date=20260302"
    for depth in (2, 3, 5):
        assert paths[depth] == (
            root
            / "profile=top5_v1"
            / f"depth_levels={depth}"
            / "date=20260302"
        )
    assert len(set(paths.values())) == 4


@pytest.mark.parametrize("depth", [2, 3, 4, 5])
def test_topn_build_fails_before_source_batch_scanning(
    tmp_path: Path, depth: int
) -> None:
    store = CompactCacheStore(
        CompactBuildConfig(cache_root=tmp_path / "cache", depth_levels=depth)
    )
    source = CompactSource("stock", (tmp_path / "not-read.parquet",), ("0050",))
    with patch.object(builder, "iter_source_batches") as scan:
        with pytest.raises(CompactCacheError, match="population is not implemented"):
            store.build_date("2026-03-02", [source])
    scan.assert_not_called()
    assert not store.namespace_root.exists()


def test_resource_estimates_are_profile_aware_and_keep_bbo_baseline() -> None:
    assert compact_row_estimate_bytes(1) == 96
    assert TOP5_ROW_ESTIMATE_BYTES == 256
    assert compact_row_estimate_bytes(2) == TOP5_ROW_ESTIMATE_BYTES
    assert compact_row_estimate_bytes(5) == TOP5_ROW_ESTIMATE_BYTES
    assert projected_bytes(10, 1) == 1_152
    assert projected_bytes(10, 2) == 3_072


def test_preflight_probes_selected_namespace_but_counts_overall_cache(
    tmp_path: Path,
) -> None:
    root = tmp_path / "cache"
    namespace = root / "profile=top5_v1" / "depth_levels=2"
    namespace.mkdir(parents=True)
    (root / "other-profile.bin").write_bytes(b"x" * 100)
    identity = {"sources": [{"files": [{"rows": 1}]}]}
    config = CompactBuildConfig(
        cache_root=root,
        depth_levels=2,
        max_cache_bytes=10_000,
        min_free_bytes=0,
    )
    with patch(
        "hftbacktest_slim.cache.publication.shutil.disk_usage",
        return_value=SimpleNamespace(free=10_000),
    ) as disk_usage:
        preflight_space(root, config, identity, namespace_root=namespace)
    disk_usage.assert_called_once_with(namespace)

    too_small = CompactBuildConfig(
        cache_root=root,
        depth_levels=2,
        max_cache_bytes=projected_bytes(1, 2),
        min_free_bytes=0,
    )
    with pytest.raises(CompactCacheError, match="existing=100"):
        preflight_space(root, too_small, identity, namespace_root=namespace)
