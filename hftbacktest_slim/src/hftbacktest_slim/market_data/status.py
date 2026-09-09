"""Packed Taiwan market-data status decoding shared by cache consumers."""

from __future__ import annotations

from typing import Any


TWSE_TRIAL_STATUS_MASK = 1 << 23


def decode_twse_status(status: int) -> dict[str, int]:
    """Decode ``StockDepthV.status`` on little-endian GCC-style layouts."""

    data_flag = status & 0xFF
    limit_flag = (status >> 8) & 0xFF
    data_status = (status >> 16) & 0xFF
    return {
        "raw_status": status,
        "data_flag": data_flag,
        "disclosure_tag": data_flag & 0b00000001,
        "ask_level": (data_flag >> 1) & 0b00000111,
        "bid_level": (data_flag >> 4) & 0b00000111,
        "is_traded": (data_flag >> 7) & 0b00000001,
        "limit_flag": limit_flag,
        "price_tag": limit_flag & 0b00000011,
        "best_ask": (limit_flag >> 2) & 0b00000011,
        "best_bid": (limit_flag >> 4) & 0b00000011,
        "limit_tag": (limit_flag >> 6) & 0b00000011,
        "data_status": data_status,
        "reserve": data_status & 0b00000011,
        "close_tag": (data_status >> 2) & 0b00000001,
        "open_tag": (data_status >> 3) & 0b00000001,
        "match_tag": (data_status >> 4) & 0b00000001,
        "close_delay_tag": (data_status >> 5) & 0b00000001,
        "open_delay_tag": (data_status >> 6) & 0b00000001,
        "trial_status_tag": (data_status >> 7) & 0b00000001,
    }


def decode_taifex_status(status: int) -> dict[str, int]:
    """Decode ``FutureOrderbook.status`` into its four flag bytes."""

    return {
        "raw_status": status,
        "build_type": status & 0xFF,
        "match_flag": (status >> 8) & 0xFF,
        "orderbook_action": (status >> 16) & 0xFF,
        "continuous_flag": (status >> 24) & 0xFF,
    }


def is_twse_trial_status(status: int) -> bool:
    """Return whether the packed TWSE status marks a trial-match quote."""

    return bool(int(status) & TWSE_TRIAL_STATUS_MASK)


def twse_trial_status_mask(values: Any) -> Any:
    """Vectorize :func:`is_twse_trial_status` over integer-like values."""

    import numpy as np

    status = np.asarray(values, dtype=np.uint32)
    return (status & np.uint32(TWSE_TRIAL_STATUS_MASK)) != 0


def expand_twse_status_columns(
    df: Any,
    status_col: str = "status",
    prefix: str = "status_",
) -> Any:
    """Return a pandas-compatible frame with decoded TWSE status columns."""

    import numpy as np

    s = df[status_col].to_numpy(dtype=np.uint32, copy=False)
    decoded = {
        f"{prefix}{key}": value
        for key, value in {
            "raw_status": s,
            "data_flag": (s & np.uint32(0xFF)).astype(np.uint8),
            "disclosure_tag": (s & np.uint32(0b00000001)).astype(np.uint8),
            "ask_level": ((s >> np.uint32(1)) & np.uint32(0b00000111)).astype(np.uint8),
            "bid_level": ((s >> np.uint32(4)) & np.uint32(0b00000111)).astype(np.uint8),
            "is_traded": ((s >> np.uint32(7)) & np.uint32(0b00000001)).astype(np.uint8),
            "limit_flag": ((s >> np.uint32(8)) & np.uint32(0xFF)).astype(np.uint8),
            "price_tag": ((s >> np.uint32(8)) & np.uint32(0b00000011)).astype(np.uint8),
            "best_ask": ((s >> np.uint32(10)) & np.uint32(0b00000011)).astype(np.uint8),
            "best_bid": ((s >> np.uint32(12)) & np.uint32(0b00000011)).astype(np.uint8),
            "limit_tag": ((s >> np.uint32(14)) & np.uint32(0b00000011)).astype(np.uint8),
            "data_status": ((s >> np.uint32(16)) & np.uint32(0xFF)).astype(np.uint8),
            "reserve": ((s >> np.uint32(16)) & np.uint32(0b00000011)).astype(np.uint8),
            "close_tag": ((s >> np.uint32(18)) & np.uint32(0b00000001)).astype(np.uint8),
            "open_tag": ((s >> np.uint32(19)) & np.uint32(0b00000001)).astype(np.uint8),
            "match_tag": ((s >> np.uint32(20)) & np.uint32(0b00000001)).astype(np.uint8),
            "close_delay_tag": ((s >> np.uint32(21)) & np.uint32(0b00000001)).astype(np.uint8),
            "open_delay_tag": ((s >> np.uint32(22)) & np.uint32(0b00000001)).astype(np.uint8),
            "trial_status_tag": ((s >> np.uint32(23)) & np.uint32(0b00000001)).astype(np.uint8),
        }.items()
    }
    return df.assign(**decoded)


def expand_taifex_status_columns(
    df: Any,
    status_col: str = "status",
    prefix: str = "status_",
) -> Any:
    """Return a pandas-compatible frame with decoded TAIFEX status columns."""

    import numpy as np

    s = df[status_col].to_numpy(dtype=np.uint32, copy=False)
    return df.assign(
        **{
            f"{prefix}raw_status": s,
            f"{prefix}build_type": (s & np.uint32(0xFF)).astype(np.uint8),
            f"{prefix}match_flag": ((s >> np.uint32(8)) & np.uint32(0xFF)).astype(np.uint8),
            f"{prefix}orderbook_action": ((s >> np.uint32(16)) & np.uint32(0xFF)).astype(np.uint8),
            f"{prefix}continuous_flag": ((s >> np.uint32(24)) & np.uint32(0xFF)).astype(np.uint8),
        }
    )


__all__ = (
    "TWSE_TRIAL_STATUS_MASK",
    "decode_taifex_status",
    "decode_twse_status",
    "expand_taifex_status_columns",
    "expand_twse_status_columns",
    "is_twse_trial_status",
    "twse_trial_status_mask",
)
