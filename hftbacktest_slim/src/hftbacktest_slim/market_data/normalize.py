"""Provider-neutral Top-5 cleanup, aggregation, ordering, and selection."""

from __future__ import annotations

import numpy as np
from numba import njit


@njit(cache=True)
def _aggregate_depth_side(
    prices: np.ndarray,
    quantities: np.ndarray,
    row: int,
    volume_scale: float,
    price_only_depth_qty: float,
    use_price_only_depth_qty: bool,
    ascending: bool,
    work_prices: np.ndarray,
    work_quantities: np.ndarray,
) -> int:
    count = 0
    for level in range(prices.shape[1]):
        px = prices[row, level]
        qty = quantities[row, level]
        if (not np.isfinite(qty)) and use_price_only_depth_qty and np.isfinite(px) and px > 0.0:
            qty = price_only_depth_qty
        qty *= volume_scale
        if not (np.isfinite(px) and px > 0.0 and np.isfinite(qty) and qty > 0.0):
            continue

        found = -1
        for index in range(count):
            if work_prices[index] == px:
                found = index
                break
        if found >= 0:
            work_quantities[found] += qty
        else:
            work_prices[count] = px
            work_quantities[count] = qty
            count += 1

    # Top-5 is tiny; insertion sort avoids a temporary allocation for each row.
    for index in range(1, count):
        px = work_prices[index]
        qty = work_quantities[index]
        cursor = index - 1
        while cursor >= 0 and (
            (ascending and work_prices[cursor] > px)
            or ((not ascending) and work_prices[cursor] < px)
        ):
            work_prices[cursor + 1] = work_prices[cursor]
            work_quantities[cursor + 1] = work_quantities[cursor]
            cursor -= 1
        work_prices[cursor + 1] = px
        work_quantities[cursor + 1] = qty
    return count


@njit(cache=True)
def _normalized_depth_from_depth_columns_impl(
    prices: np.ndarray,
    quantities: np.ndarray,
    volume_scale: float,
    price_only_depth_qty: float,
    use_price_only_depth_qty: bool,
    bid: bool,
    depth_levels: int,
) -> tuple[np.ndarray, np.ndarray]:
    selected_prices = np.full(
        (prices.shape[0], depth_levels), np.nan, dtype=np.float64
    )
    selected_quantities = np.full(
        (prices.shape[0], depth_levels), np.nan, dtype=np.float64
    )
    work_prices = np.empty(prices.shape[1], dtype=np.float64)
    work_quantities = np.empty(prices.shape[1], dtype=np.float64)
    for row in range(prices.shape[0]):
        count = _aggregate_depth_side(
            prices,
            quantities,
            row,
            volume_scale,
            price_only_depth_qty,
            use_price_only_depth_qty,
            not bid,
            work_prices,
            work_quantities,
        )
        selected = 0
        for level in range(count):
            qty = work_quantities[level]
            if not (np.isfinite(qty) and qty > 0.0):
                continue
            selected_prices[row, selected] = work_prices[level]
            selected_quantities[row, selected] = qty
            selected += 1
            if selected == depth_levels:
                break
    return selected_prices, selected_quantities


def _validated_depth_inputs(
    prices: np.ndarray,
    quantities: np.ndarray,
    depth_levels: object,
) -> tuple[np.ndarray, np.ndarray, int]:
    if isinstance(depth_levels, bool) or not isinstance(depth_levels, int):
        raise ValueError("depth_levels must be an integer from 1 through 5")
    if not 1 <= depth_levels <= 5:
        raise ValueError("depth_levels must be an integer from 1 through 5")
    price_values = np.asarray(prices, dtype=np.float64)
    quantity_values = np.asarray(quantities, dtype=np.float64)
    if price_values.ndim != 2 or quantity_values.ndim != 2:
        raise ValueError("prices and quantities must be two-dimensional arrays")
    if price_values.shape != quantity_values.shape:
        raise ValueError("prices and quantities must have the same shape")
    if price_values.shape[1] > 5:
        raise ValueError("prices and quantities may contain at most five source levels")
    return (
        np.ascontiguousarray(price_values),
        np.ascontiguousarray(quantity_values),
        depth_levels,
    )


def normalized_depth_from_depth_columns(
    prices: np.ndarray,
    quantities: np.ndarray,
    volume_scale: float,
    price_only_depth_qty: float,
    use_price_only_depth_qty: bool,
    bid: bool,
    depth_levels: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Return the first Top-N distinct normalized levels as float64 matrices.

    Missing levels are represented by NaN internally. The compact Arrow
    builder converts those paired missing values to explicit Arrow nulls.
    """

    price_values, quantity_values, depth = _validated_depth_inputs(
        prices, quantities, depth_levels
    )
    return _normalized_depth_from_depth_columns_impl(
        price_values,
        quantity_values,
        volume_scale,
        price_only_depth_qty,
        use_price_only_depth_qty,
        bid,
        depth,
    )


def normalized_bbo_from_depth_columns(
    prices: np.ndarray,
    quantities: np.ndarray,
    volume_scale: float,
    price_only_depth_qty: float,
    use_price_only_depth_qty: bool,
    bid: bool,
) -> tuple[np.ndarray, np.ndarray]:
    """Return normalized best prices and quantities using the Top-N kernel."""

    selected_prices, selected_quantities = normalized_depth_from_depth_columns(
        prices,
        quantities,
        volume_scale,
        price_only_depth_qty,
        use_price_only_depth_qty,
        bid,
        1,
    )
    return selected_prices[:, 0], selected_quantities[:, 0]


__all__ = (
    "normalized_bbo_from_depth_columns",
    "normalized_depth_from_depth_columns",
)
