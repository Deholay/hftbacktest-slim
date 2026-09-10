from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from hftbacktest_slim import (
    ArrowDataError,
    AssetConfig,
    Side,
    SlimEngine,
    TimeInForce,
    BBO_SCHEMA,
    TOP5_SCHEMA,
    top5_schema_metadata,
)
from hftbacktest_slim.engine.arrow_reader import SLIM_ROW_DTYPE, _read_rows, read_rows


def _values(depth: int) -> dict[str, list]:
    values: dict[str, list] = {
        "source_seq": [0, 1, 2],
        "exch_ts": [100, 200, 300],
        "local_ts_raw": [90, 210, 320],
        "last_px": [77.925, 78.025, 78.125],
        "total_volume": [10, 11, 12],
        "tradable": [1, 0, 1],
    }
    for side in ("bid", "ask"):
        for level in range(1, 6):
            enabled = level <= depth
            if side == "bid":
                prices = [77.95 - 0.05 * level, 78.05 - 0.05 * level, None]
            else:
                prices = [77.90 + 0.05 * level, 78.00 + 0.05 * level, None]
            values[f"{side}_px_{level}"] = prices if enabled else [None] * 3
            values[f"{side}_qty_{level}"] = (
                [level + 0.25, level + 0.5, None] if enabled else [None] * 3
            )
    return values


def _write(
    path: Path,
    depth: int,
    *,
    values: dict[str, list] | None = None,
    metadata_updates: dict[bytes, bytes | None] | None = None,
) -> Path:
    metadata = top5_schema_metadata(depth, local_timestamp_adjustment_ns=10)
    metadata.update(
        {b"trade_date": b"2026-03-02", b"source": b"stock", b"symbol": b"0050"}
    )
    for key, value in (metadata_updates or {}).items():
        if value is None:
            metadata.pop(key, None)
        else:
            metadata[key] = value
    table = pa.Table.from_pydict(
        _values(depth) if values is None else values,
        schema=TOP5_SCHEMA.with_metadata(metadata),
    )
    with path.open("wb") as sink, ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return path


@pytest.mark.parametrize("depth", [2, 3, 5])
def test_reader_projects_exact_level_one_and_preserves_identity(
    tmp_path: Path, depth: int
) -> None:
    loaded = read_rows(_write(tmp_path / f"n{depth}.arrow", depth))
    assert loaded.schema_version == "top5_v1"
    assert loaded.profile == "top5"
    assert loaded.depth_levels == depth
    assert loaded.local_timestamp_adjustment_ns == 10
    assert loaded.rows.dtype == SLIM_ROW_DTYPE
    assert loaded.rows["source_seq"].tolist() == [0, 1, 2]
    np.testing.assert_allclose(loaded.rows["bid_px"][:2], [77.90, 78.00])
    np.testing.assert_allclose(loaded.rows["ask_px"][:2], [77.95, 78.05])
    np.testing.assert_allclose(loaded.rows["bid_qty"][:2], [1.25, 1.5])
    np.testing.assert_allclose(loaded.rows["ask_qty"][:2], [1.25, 1.5])
    assert np.isnan(loaded.rows["bid_px"][2])
    assert np.isnan(loaded.rows["bid_qty"][2])
    assert loaded.rows["tradable"].tolist() == [1, 0, 1]
    assert SLIM_ROW_DTYPE.itemsize == 80 and SLIM_ROW_DTYPE.isalignedstruct


def test_reader_empty_top5_and_tuple_bridge(tmp_path: Path) -> None:
    values = {name: [] for name in TOP5_SCHEMA.names}
    path = _write(tmp_path / "empty.arrow", 3, values=values)
    loaded = read_rows(path)
    rows, adjustment = _read_rows(path)
    assert loaded.rows.shape == rows.shape == (0,)
    assert rows.dtype == SLIM_ROW_DTYPE
    assert adjustment == 10


def test_bbo_and_topn_level_one_project_to_identical_native_rows(tmp_path: Path) -> None:
    top5_path = _write(tmp_path / "top5.arrow", 5)
    top5 = read_rows(top5_path)
    values = _values(5)
    bbo = pa.Table.from_pydict(
        {
            "source_seq": values["source_seq"],
            "exch_ts": values["exch_ts"],
            "local_ts_raw": values["local_ts_raw"],
            "bid_px": values["bid_px_1"],
            "ask_px": values["ask_px_1"],
            "bid_qty": values["bid_qty_1"],
            "ask_qty": values["ask_qty_1"],
            "last_px": values["last_px"],
            "total_volume": values["total_volume"],
            "tradable": values["tradable"],
        },
        schema=BBO_SCHEMA,
    ).replace_schema_metadata(
        {b"schema_version": b"bbo_v2", b"local_timestamp_adjustment_ns": b"10"}
    )
    bbo_path = tmp_path / "bbo.arrow"
    with bbo_path.open("wb") as sink, ipc.new_file(sink, bbo.schema) as writer:
        writer.write_table(bbo)
    projected_bbo = read_rows(bbo_path)
    for name in SLIM_ROW_DTYPE.names or ():
        if projected_bbo.rows[name].dtype.kind == "f":
            np.testing.assert_allclose(
                projected_bbo.rows[name], top5.rows[name], equal_nan=True
            )
        else:
            np.testing.assert_equal(projected_bbo.rows[name], top5.rows[name])


@pytest.mark.parametrize(
    ("updates", "match"),
    [
        ({b"profile": None}, "profile"),
        ({b"depth_levels": None}, "depth_levels"),
        ({b"depth_levels": b"1"}, "2 through 5"),
        ({b"schema_version": b"bbo_v2"}, "fields/order"),
    ],
)
def test_reader_rejects_invalid_top5_identity(
    tmp_path: Path, updates: dict[bytes, bytes | None], match: str
) -> None:
    path = _write(tmp_path / "bad-metadata.arrow", 3, metadata_updates=updates)
    with pytest.raises(ArrowDataError, match=match):
        read_rows(path)


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (lambda v: v["bid_qty_1"].__setitem__(0, None), "price without quantity"),
        (lambda v: v["ask_px_2"].__setitem__(0, 77.90), "strictly ascending"),
        (lambda v: v["tradable"].__setitem__(0, None), "tradable contains null"),
        (lambda v: v["tradable"].__setitem__(0, 2), "outside 0 or 1"),
    ],
)
def test_reader_rejects_corrupt_top5_content(tmp_path: Path, mutate, match: str) -> None:
    values = _values(3)
    mutate(values)
    path = _write(tmp_path / "bad-content.arrow", 3, values=values)
    with pytest.raises(ArrowDataError, match=match):
        read_rows(path)


def test_slim_top5_uses_only_level_one_and_displayed_size_does_not_cap_fill(
    tmp_path: Path, native_library_path: Path
) -> None:
    left = _write(tmp_path / "left.arrow", 3)
    right = _write(tmp_path / "right.arrow", 3)
    assets = [
        AssetConfig("left", left, 0.05),
        AssetConfig("right", right, 0.05),
    ]
    with SlimEngine(assets, library_path=native_library_path) as engine:
        assert engine.advance(10)
        depth = engine.depth(0)
        assert depth.best_ask == pytest.approx(77.95)
        assert depth.best_ask_quantity == pytest.approx(1.25)
        engine.submit_order(
            asset_no=0,
            order_id=1,
            side=Side.BUY,
            price=77.95,
            quantity=10.0,
            time_in_force=TimeInForce.FOK,
        )
        assert engine.wait_order_response(0, 1, 1)
        order = engine.order(0, 1)
        assert order is not None
        assert order.execution_price == pytest.approx(77.95)
        assert order.execution_quantity == pytest.approx(10.0)
