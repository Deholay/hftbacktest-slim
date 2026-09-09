"""Package-owned compact market-data contracts and transformations."""

from .audit import compact_partition_audit
from .normalize import normalized_bbo_from_depth_columns
from .schema import (
    BBO_SCHEMA,
    COMPACT_SCHEMA_VERSION,
    PROJECTED_COLUMNS,
    SLIM_ROW_DTYPE,
)
from .status import (
    TWSE_TRIAL_STATUS_MASK,
    decode_taifex_status,
    decode_twse_status,
    expand_taifex_status_columns,
    expand_twse_status_columns,
    is_twse_trial_status,
    twse_trial_status_mask,
)

__all__ = (
    "BBO_SCHEMA",
    "COMPACT_SCHEMA_VERSION",
    "PROJECTED_COLUMNS",
    "SLIM_ROW_DTYPE",
    "TWSE_TRIAL_STATUS_MASK",
    "compact_partition_audit",
    "decode_taifex_status",
    "decode_twse_status",
    "expand_taifex_status_columns",
    "expand_twse_status_columns",
    "is_twse_trial_status",
    "normalized_bbo_from_depth_columns",
    "twse_trial_status_mask",
)
