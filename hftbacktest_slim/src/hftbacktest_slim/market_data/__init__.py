"""Package-owned compact market-data contracts and transformations."""

from .audit import compact_partition_audit
from .normalize import (
    normalized_bbo_from_depth_columns,
    normalized_depth_from_depth_columns,
)
from .schema import (
    BBO_SCHEMA_VERSION,
    BBO_SCHEMA,
    COMPACT_SCHEMA_VERSION,
    PROJECTED_COLUMNS,
    SLIM_ROW_DTYPE,
    TOP5_PHYSICAL_FIELDS,
    TOP5_SCHEMA,
    TOP5_SCHEMA_VERSION,
    profile_for_depth_levels,
    schema_for_depth_levels,
    schema_version_for_depth_levels,
    top5_schema_metadata,
    validate_compact_schema,
    validate_depth_levels,
    validate_top5_schema,
    validate_top5_schema_metadata,
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
from .validation import validate_compact_table

__all__ = (
    "BBO_SCHEMA",
    "BBO_SCHEMA_VERSION",
    "COMPACT_SCHEMA_VERSION",
    "PROJECTED_COLUMNS",
    "SLIM_ROW_DTYPE",
    "TOP5_PHYSICAL_FIELDS",
    "TOP5_SCHEMA",
    "TOP5_SCHEMA_VERSION",
    "TWSE_TRIAL_STATUS_MASK",
    "compact_partition_audit",
    "decode_taifex_status",
    "decode_twse_status",
    "expand_taifex_status_columns",
    "expand_twse_status_columns",
    "is_twse_trial_status",
    "normalized_bbo_from_depth_columns",
    "normalized_depth_from_depth_columns",
    "profile_for_depth_levels",
    "schema_for_depth_levels",
    "schema_version_for_depth_levels",
    "top5_schema_metadata",
    "twse_trial_status_mask",
    "validate_compact_schema",
    "validate_compact_table",
    "validate_depth_levels",
    "validate_top5_schema",
    "validate_top5_schema_metadata",
)
