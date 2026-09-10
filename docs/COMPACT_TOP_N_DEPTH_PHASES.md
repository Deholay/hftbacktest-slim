# Configurable Compact Top-N Depth

## Status and scope

This document defines the staged implementation of configurable symmetric
compact market depth. Phase 0 and Phase 1 froze BBO behavior and introduced
configuration, physical schema, metadata, identity, namespace, CLI, and
resource-estimation contracts. Phase 2 populates normalized Top-N Arrow rows.
Phase 3 adds batch-wise content validation, exact depth statistics, conservative
manifest invalidation, and completed-plus-temporary disk preflight accounting,
without changing replay or matching behavior.

The one user-facing selector is `depth_levels`, exposed on the command line as
`--compact-depth-levels`. It accepts only Python integers from 1 through 5;
booleans, floats, strings, zero, negatives, and values above 5 are invalid.
Bid and ask always use the same value:

| `depth_levels` | Selected normalized levels |
| ---: | --- |
| 1 | `a1`, `b1` |
| 2 | `a1-a2`, `b1-b2` |
| 3 | `a1-a3`, `b1-b3` |
| 4 | `a1-a4`, `b1-b4` |
| 5 | `a1-a5`, `b1-b5` |

The retained `profile="bbo"` configuration argument is compatibility input
for existing callers, not a second user choice. The canonical profile is
derived from depth: level 1 selects `bbo`; levels 2 through 5 select `top5`.

## Normalized distinct levels

A depth level is the Nth distinct normalized price, not the Nth raw field or
order. For each side and source row:

1. Reject non-finite or non-positive price and quantity values. The existing
   explicit price-only quantity policy remains the only declared exception.
2. Aggregate quantities at repeated prices.
3. Sort bids by descending price and asks by ascending price.
4. Select the first N distinct levels.

Phase 2 reuses one provider-neutral normalization implementation for BBO and
Top-N and preserves decimal prices.

## Versioned physical profiles

`depth_levels=1` continues to use `bbo_v2`. `COMPACT_SCHEMA_VERSION` remains a
compatibility alias for `bbo_v2`; `BBO_SCHEMA`, `PHYSICAL_FIELDS`, and
`SLIM_ROW_DTYPE` retain their exact existing contracts. The default cache path
also remains:

```text
cache_root/date=YYYYMMDD
```

`depth_levels=2..5` selects the separately versioned `top5_v1` profile. Its
physical schema always contains all five levels in this exact order:

```text
source_seq      uint64
exch_ts         int64
local_ts_raw    int64
bid_px_1        float64
bid_px_2        float64
bid_px_3        float64
bid_px_4        float64
bid_px_5        float64
bid_qty_1       float64
bid_qty_2       float64
bid_qty_3       float64
bid_qty_4       float64
bid_qty_5       float64
ask_px_1        float64
ask_px_2        float64
ask_px_3        float64
ask_px_4        float64
ask_px_5        float64
ask_qty_1       float64
ask_qty_2       float64
ask_qty_3       float64
ask_qty_4       float64
ask_qty_5       float64
last_px         float64
total_volume    int64
tradable        uint8
```

Every field is nullable, and in particular every depth price and quantity is
nullable `float64`. Phase 2 populates levels 1 through N and writes Arrow nulls
for unavailable levels and all fields above N. A fixed physical schema makes
files predictable while the selected N remains part of metadata and cache
identity.

## Metadata, identity, and namespace

Every `top5_v1` Arrow partition requires these exact metadata identities:

```text
schema_version=top5_v1
profile=top5
depth_levels=N
local_timestamp_adjustment_ns=<integer>
aggregation_policy=valid_positive_distinct_price_sum_qty_v1
bid_depth_ordering=price_descending_v1
ask_depth_ordering=price_ascending_v1
```

Validation fails closed on missing, malformed, conflicting, or unsupported
values and on any physical field-name, order, type, or nullability difference.
The BBO validator continues to accept the current `bbo_v2` metadata contract;
new optional BBO profile/depth metadata may only declare `bbo` and `1`.

The build identity and checksum include schema version, canonical profile, and
`depth_levels`. Thus N=2, N=3, and N=5 are not interchangeable. Their date
namespaces are deterministic and separate:

```text
cache_root/profile=top5_v1/depth_levels=N/date=YYYYMMDD
```

All path components are validated. Temporary construction, validation, and
atomic date publication happen inside the selected namespace. The cache-size
limit still accounts for the complete configured cache root, so namespaces do
not create independent budgets.

Manifests must record the selected schema version, canonical profile,
`depth_levels`, builder version, complete build identity/checksum, source
identity, projected fields, normalization implementation, per-source scan
counts, per-symbol facts, timestamp correction/order state, and completion
state. Unknown identity metadata is never a cache hit.

## One-scan and resource contracts

Top-N population retains the existing successful-cold-date
invariant: at most one raw stock scan and one raw futures scan, projected
streaming batches only, no whole-day collection, no raw scan inside symbol or
pair loops, and no worker access to raw daily sources.

Preflight estimates are profile-aware and deliberately ignore compression.
The BBO estimate remains 96 bytes per source row before the existing 1.20
safety factor. The fixed Top-5 payload is 201 bytes across 25 eight-byte fields
and one byte field; allowing for 26 nullable validity bits, alignment, and
Arrow overhead gives a conservative 256 bytes per row before the same 1.20
factor. Overall cache-budget and filesystem-free-space checks remain in force.

## Matching boundary and deferred work

The slim engine remains an immediate, crossing, no-partial-fill BBO matcher.
`top5_v1` is a data profile, not authorization to reinterpret `DepthView`, the
native `BboRow`/`BboView`, the C ABI, queue behavior, displayed-size behavior,
or futures/spot strategy decisions. Phase 2 removes the package cache-build
guard and supports streaming `top5_v1` publication and profile-aware
`read_symbol()` tables. Full-market execution fails before cache construction
for `depth_levels>1` because its engine readers remain BBO-only.

Phase 3 is the final implemented phase in this change. Later phases must
separately implement multi-level reference-HBT reconstruction, runtime readers,
and any future depth-sensitive execution mode. Native engine or matching
changes require a separate semantic version, ABI review where layouts change,
regression baseline, and parity gate.
