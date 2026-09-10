"""Public compact-cache build, validation, reuse, and read operations."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Sequence

import pyarrow as pa
import pyarrow.ipc as ipc

from ..errors import CompactCacheError, CompactValidationError
from ..market_data.ordering import validate_order_sidecar
from ..market_data.schema import PROJECTED_COLUMNS
from ..market_data.validation import (
    aggregate_depth_statistics,
    validate_compact_partition,
)
from .builder import build_source, source_dirname
from .config import (
    COMPACT_BUILDER_VERSION,
    CompactBuildConfig,
    CompactSource,
    cache_namespace_components,
)
from .manifest import (
    build_identity,
    canonical_sha256,
    file_sha256,
    implementation_fingerprint,
    implementation_paths,
    source_identity,
)
from .publication import (
    cleanup_incomplete_date,
    create_temporary_date,
    preflight_space,
    publish_date_atomically,
    write_json,
)


class CompactCacheStore:
    """Versioned, conservative compact cache rooted on one filesystem."""

    def __init__(self, config: CompactBuildConfig):
        self.config = config
        self.root = Path(config.cache_root)

    @property
    def namespace_root(self) -> Path:
        path = self.root
        for component in cache_namespace_components(self.config.depth_levels):
            path /= component
        return path

    def date_path(self, trade_date: str) -> Path:
        validate_date_value(trade_date)
        return self.namespace_root / f"date={trade_date.replace('-', '')}"

    def build_date(
        self,
        trade_date: str,
        sources: Sequence[CompactSource],
    ) -> dict[str, Any]:
        validate_date_value(trade_date)
        _validate_source_request(sources)
        expected = self._identity(trade_date, sources)
        final = self.date_path(trade_date)
        if final.exists():
            try:
                current = self.validate_date(trade_date)
            except CompactCacheError:
                current = None
            if current is not None and current.get("identity_sha256") == canonical_sha256(
                expected
            ):
                return {
                    **current,
                    "cache_state": "hit",
                    "build_invocation_scan_count": 0,
                }
            if not self.config.rebuild:
                raise CompactCacheError(
                    "compact date exists with an incompatible identity; use rebuild "
                    f"or a new root: {final}"
                )

        preflight_space(
            self.root,
            self.config,
            expected,
            namespace_root=self.namespace_root,
        )
        temp = create_temporary_date(self.namespace_root, trade_date)
        started = time.perf_counter()
        try:
            source_manifests: dict[str, Any] = {}
            for source in sources:
                if source.kind in source_manifests:
                    raise CompactCacheError(f"duplicate compact source kind: {source.kind}")
                source_manifests[source.kind] = build_source(
                    temp,
                    trade_date,
                    source,
                    self.config,
                )
            all_symbols = {
                f"{kind}/{symbol}": details
                for kind, source_details in source_manifests.items()
                for symbol, details in source_details["symbols"].items()
            }
            payload = {
                "schema_version": self.config.schema_version,
                "builder_version": COMPACT_BUILDER_VERSION,
                "profile": self.config.profile,
                "depth_levels": self.config.depth_levels,
                "trade_date": trade_date,
                "build_complete": True,
                "identity": expected,
                "identity_sha256": canonical_sha256(expected),
                "sources": source_manifests,
                "aggregation_policy": expected["aggregation_policy"],
                "bid_depth_ordering": expected["bid_depth_ordering"],
                "ask_depth_ordering": expected["ask_depth_ordering"],
                "missing_level_null_policy": expected["missing_level_null_policy"],
                "price_only_quantity_policy": expected["price_only_quantity_policy"],
                "price_only_quantity_policy_by_source": {
                    source["kind"]: source["price_only_quantity_policy"]
                    for source in expected["sources"]
                },
                "volume_scale_by_source": {
                    source["kind"]: source["volume_scale"]
                    for source in expected["sources"]
                },
                "projected_source_columns": expected["projected_source_columns"],
                "source_fingerprints": expected["source_fingerprints"],
                "implementation_fingerprint": expected["implementation_sha256"],
                "compression": expected["compression"],
                "timestamp_ordering_policy": expected["timestamp_ordering_policy"],
                "session_policy": expected["session_policy"],
                "output_rows": sum(
                    int(details["output_rows"])
                    for details in source_manifests.values()
                ),
                "output_bytes": sum(
                    int(details["output_bytes"])
                    for details in source_manifests.values()
                ),
                "non_tradable_rows": sum(
                    int(details["non_tradable_rows"])
                    for details in source_manifests.values()
                ),
                "depth": aggregate_depth_statistics(
                    all_symbols, self.config.depth_levels
                ),
                "elapsed_seconds": time.perf_counter() - started,
            }
            # Validate every closed partition/source manifest first. The date
            # manifest is the final staged write and is required for reuse.
            self._validate_sources(
                temp, trade_date, source_manifests, identity=expected
            )
            write_json(temp / "manifest.json", payload)
            publish_date_atomically(
                root=self.namespace_root,
                temp=temp,
                final=final,
                validate_staged=lambda path: self._validate_path(path, trade_date),
            )
        except Exception:
            if temp.exists():
                cleanup_incomplete_date(self.namespace_root, temp, trade_date)
            raise
        validated = self.validate_date(trade_date)
        return {
            **validated,
            "cache_state": "miss",
            "build_invocation_scan_count": sum(
                int(source.get("scan_count", 0))
                for source in validated.get("sources", {}).values()
            ),
        }

    def validate_date(self, trade_date: str) -> dict[str, Any]:
        return self._validate_path(self.date_path(trade_date), trade_date)

    def read_symbol(self, trade_date: str, source: str, symbol: str) -> pa.Table:
        manifest = self.validate_date(trade_date)
        details = manifest["sources"].get(source, {}).get("symbols", {}).get(str(symbol))
        if details is None or details.get("status") != "valid":
            raise CompactCacheError(
                f"symbol not present in compact cache: {trade_date}/{source}/{symbol}"
            )
        path = self.date_path(trade_date) / source_dirname(source) / details["file"]
        try:
            with pa.memory_map(str(path), "r") as handle:
                return ipc.open_file(handle).read_all()
        except (OSError, pa.ArrowException) as exc:
            raise CompactCacheError(f"failed to read compact symbol: {path}") from exc

    def _identity(
        self,
        trade_date: str,
        sources: Sequence[CompactSource],
    ) -> dict[str, Any]:
        return build_identity(trade_date, sources, self.config)

    def _validate_path(self, path: Path, trade_date: str) -> dict[str, Any]:
        try:
            payload = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CompactCacheError(f"invalid or incomplete compact date: {path}") from exc
        if payload.get("schema_version") != self.config.schema_version:
            raise CompactCacheError(f"unsupported compact schema version: {path}")
        if payload.get("builder_version") != COMPACT_BUILDER_VERSION:
            raise CompactCacheError(f"unsupported compact builder version: {path}")
        if payload.get("build_complete") is not True:
            raise CompactCacheError(f"unsupported or incomplete compact date: {path}")
        if payload.get("trade_date") != trade_date:
            raise CompactCacheError(f"compact date identity mismatch: {path}")
        if payload.get("profile") != self.config.profile:
            raise CompactCacheError(f"compact date profile mismatch: {path}")
        if payload.get("depth_levels") != self.config.depth_levels:
            raise CompactCacheError(f"compact date depth-level mismatch: {path}")
        identity = payload.get("identity")
        if not isinstance(identity, dict):
            raise CompactCacheError(f"compact identity metadata is missing: {path}")
        if identity.get("schema_version") != self.config.schema_version:
            raise CompactCacheError(f"compact identity schema mismatch: {path}")
        if identity.get("builder_version") != COMPACT_BUILDER_VERSION:
            raise CompactCacheError(f"compact identity builder mismatch: {path}")
        if identity.get("profile") != self.config.profile:
            raise CompactCacheError(f"compact identity profile mismatch: {path}")
        if identity.get("depth_levels") != self.config.depth_levels:
            raise CompactCacheError(f"compact identity depth-level mismatch: {path}")
        expected_manifest_contract = {
            "aggregation_policy": identity.get("aggregation_policy"),
            "bid_depth_ordering": identity.get("bid_depth_ordering"),
            "ask_depth_ordering": identity.get("ask_depth_ordering"),
            "missing_level_null_policy": identity.get("missing_level_null_policy"),
            "price_only_quantity_policy": identity.get("price_only_quantity_policy"),
            "price_only_quantity_policy_by_source": {
                item.get("kind"): item.get("price_only_quantity_policy")
                for item in identity.get("sources", [])
                if isinstance(item, dict)
            },
            "volume_scale_by_source": {
                item.get("kind"): item.get("volume_scale")
                for item in identity.get("sources", [])
                if isinstance(item, dict)
            },
            "projected_source_columns": identity.get("projected_source_columns"),
            "source_fingerprints": identity.get("source_fingerprints"),
            "implementation_fingerprint": identity.get("implementation_sha256"),
            "compression": identity.get("compression"),
            "timestamp_ordering_policy": identity.get("timestamp_ordering_policy"),
            "session_policy": identity.get("session_policy"),
        }
        for key, expected_value in expected_manifest_contract.items():
            if expected_value is None or payload.get(key) != expected_value:
                raise CompactCacheError(
                    f"compact Phase 3 manifest contract mismatch for {key}: {path}"
                )
        current_implementation = implementation_fingerprint()
        if identity.get("implementation_sha256") != current_implementation:
            raise CompactCacheError(f"compact implementation identity mismatch: {path}")
        if identity.get("builder_sha256") != current_implementation:
            raise CompactCacheError(f"compact builder identity mismatch: {path}")
        package_root = Path(__file__).resolve().parents[1]
        current_paths = [
            item.relative_to(package_root).as_posix() for item in implementation_paths()
        ]
        if identity.get("implementation_paths") != current_paths:
            raise CompactCacheError(f"compact implementation path identity mismatch: {path}")
        normalize_sha = file_sha256(package_root / "market_data" / "normalize.py")
        if identity.get("top5_implementation_sha256") != normalize_sha:
            raise CompactCacheError(f"compact normalization identity mismatch: {path}")
        if identity.get("projected_columns") != list(PROJECTED_COLUMNS):
            raise CompactCacheError(f"compact projected-column identity mismatch: {path}")
        self._validate_raw_source_identities(identity, path)
        if payload.get("identity_sha256") != canonical_sha256(identity):
            raise CompactCacheError(f"compact identity checksum mismatch: {path}")
        sources = payload.get("sources")
        if not isinstance(sources, dict):
            raise CompactCacheError(f"compact source manifests are missing: {path}")
        identity_sources = {
            item.get("kind"): item
            for item in identity.get("sources", [])
            if isinstance(item, dict)
        }
        if set(identity_sources) != set(sources):
            raise CompactValidationError(
                f"date={trade_date}: source manifest set does not match cache identity"
            )
        for kind, source_details in sources.items():
            source_identity_details = identity_sources[kind]
            expected_source_facts = {
                "scan_count": len(source_identity_details["files"]),
                "input_rows": sum(
                    int(item["rows"]) for item in source_identity_details["files"]
                ),
                "input_bytes": sum(
                    int(item["bytes"]) for item in source_identity_details["files"]
                ),
                "price_only_quantity_policy": {
                    key: value
                    for key, value in source_identity_details[
                        "price_only_quantity_policy"
                    ].items()
                    if key != "identity"
                },
                "volume_scale": source_identity_details["volume_scale"],
            }
            for key, value in expected_source_facts.items():
                if source_details.get(key) != value:
                    raise CompactValidationError(
                        f"date={trade_date} source={kind}: source manifest {key} "
                        "does not match cache identity"
                    )
            if set(source_details.get("symbols", {})) != set(
                source_identity_details["symbols"]
            ):
                raise CompactValidationError(
                    f"date={trade_date} source={kind}: requested symbol universe mismatch"
                )
        self._validate_sources(path, trade_date, sources, identity=identity)
        all_symbols = {
            f"{kind}/{symbol}": details
            for kind, source_details in sources.items()
            for symbol, details in source_details["symbols"].items()
        }
        date_facts = {
            "output_rows": sum(int(item["output_rows"]) for item in sources.values()),
            "output_bytes": sum(int(item["output_bytes"]) for item in sources.values()),
            "non_tradable_rows": sum(
                int(item["non_tradable_rows"]) for item in sources.values()
            ),
            "depth": aggregate_depth_statistics(
                all_symbols, self.config.depth_levels
            ),
        }
        for key, observed in date_facts.items():
            if payload.get(key) != observed:
                raise CompactValidationError(
                    f"date={trade_date}: date manifest {key} does not match source facts"
                )
        return payload

    def _validate_raw_source_identities(
        self,
        identity: dict[str, Any],
        cache_path: Path,
    ) -> None:
        sources = identity.get("sources")
        if not isinstance(sources, list):
            raise CompactCacheError(f"compact raw-source identity is missing: {cache_path}")
        for source in sources:
            if not isinstance(source, dict) or not isinstance(source.get("files"), list):
                raise CompactCacheError(
                    f"compact raw-source file identity is missing: {cache_path}"
                )
            for saved in source["files"]:
                if not isinstance(saved, dict) or not isinstance(saved.get("path"), str):
                    raise CompactCacheError(
                        f"compact raw-source path identity is missing: {cache_path}"
                    )
                try:
                    current = source_identity(Path(saved["path"]))
                except (OSError, ValueError, pa.ArrowException) as exc:
                    raise CompactCacheError(
                        f"compact raw source cannot be validated: {saved.get('path')}"
                    ) from exc
                if current != saved:
                    raise CompactCacheError(
                        f"compact raw-source identity changed: {saved['path']}"
                    )

    def _validate_sources(
        self,
        path: Path,
        trade_date: str,
        sources: dict[str, Any],
        *,
        identity: dict[str, Any],
    ) -> None:
        for source, source_details in sources.items():
            _safe_component(source, "source")
            source_manifest_path = path / source_dirname(source) / "manifest.json"
            try:
                source_manifest = json.loads(
                    source_manifest_path.read_text(encoding="utf-8")
                )
            except (OSError, json.JSONDecodeError) as exc:
                raise CompactCacheError(
                    f"missing compact source manifest: {source_manifest_path}"
                ) from exc
            if source_manifest != source_details:
                raise CompactCacheError(
                    f"compact source manifest mismatch: {source_manifest_path}"
                )
            symbols = source_details.get("symbols")
            if not isinstance(symbols, dict):
                raise CompactCacheError(
                    f"compact symbol manifest is missing: {source_manifest_path}"
                )
            if source_details.get("kind") != source:
                raise CompactValidationError(
                    f"date={trade_date} source={source}: source manifest kind mismatch"
                )
            for key, expected in (
                ("profile", self.config.profile),
                ("schema_version", self.config.schema_version),
                ("depth_levels", self.config.depth_levels),
            ):
                if source_details.get(key) != expected:
                    raise CompactValidationError(
                        f"date={trade_date} source={source}: source manifest {key} mismatch"
                    )
            for symbol, details in symbols.items():
                _safe_component(symbol, "symbol")
                if details.get("status") == "missing":
                    continue
                if details.get("status") != "valid":
                    raise CompactCacheError(
                        f"unknown compact symbol status: {source}/{symbol}"
                    )
                filename = _safe_component(details.get("file"), "symbol file")
                file_path = path / source_dirname(source) / filename
                self._validate_symbol(
                    file_path,
                    trade_date=trade_date,
                    source=source,
                    symbol=symbol,
                    details=details,
                    expected_base_latency_ns=int(identity["base_latency_ns"]),
                    expected_compression=str(identity["compression"]),
                )
            valid = [
                item for item in symbols.values() if item.get("status") == "valid"
            ]
            missing = [
                item for item in symbols.values() if item.get("status") == "missing"
            ]
            empty = [item for item in valid if item.get("empty") is True]
            output_bytes = sum(
                int(item.get("bytes", 0))
                + sum(
                    int(sidecar.get("bytes", 0))
                    for sidecar in item.get("sidecars", {}).values()
                )
                for item in valid
            )
            observed = {
                "requested_symbol_count": len(symbols),
                "valid_symbol_count": len(valid),
                "empty_symbol_count": len(empty),
                "missing_symbol_count": len(missing),
                "empty_symbols": sorted(
                    symbol
                    for symbol, item in symbols.items()
                    if item.get("status") == "valid" and item.get("empty") is True
                ),
                "missing_symbols": sorted(
                    symbol
                    for symbol, item in symbols.items()
                    if item.get("status") == "missing"
                ),
                "output_rows": sum(int(item.get("rows", 0)) for item in valid),
                "output_bytes": output_bytes,
                "non_tradable_rows": sum(
                    int(item.get("non_tradable_rows", 0)) for item in valid
                ),
                "depth": aggregate_depth_statistics(symbols, self.config.depth_levels),
            }
            for key, value in observed.items():
                if source_details.get(key) != value:
                    raise CompactValidationError(
                        f"date={trade_date} source={source}: source aggregate {key} mismatch"
                    )
            if source_details.get("statistics_compact_read_count") != len(valid):
                raise CompactValidationError(
                    f"date={trade_date} source={source}: compact statistics read "
                    "count mismatch"
                )

    def _validate_symbol(
        self,
        file_path: Path,
        *,
        trade_date: str,
        source: str,
        symbol: str,
        details: dict[str, Any],
        expected_base_latency_ns: int,
        expected_compression: str,
    ) -> None:
        if not file_path.is_file() or file_path.stat().st_size != details.get("bytes"):
            raise CompactCacheError(f"missing or changed compact symbol: {file_path}")
        if file_sha256(file_path) != details.get("sha256"):
            raise CompactCacheError(f"compact symbol checksum mismatch: {file_path}")
        validation = validate_compact_partition(
            file_path,
            expected_depth_levels=self.config.depth_levels,
            trade_date=trade_date,
            source=source,
            symbol=symbol,
            require_identity_metadata=True,
        )
        metadata = validation.metadata
        for key, expected in (
            ("trade_date", trade_date),
            ("source", source),
            ("symbol", symbol),
            ("base_latency_ns", str(expected_base_latency_ns)),
            ("compression", expected_compression),
            ("exchange_ordering", "exch_ts,source_seq"),
            ("local_ordering", "corrected_local_ts,source_seq"),
        ):
            if metadata.get(key) != expected:
                raise CompactCacheError(
                    f"compact symbol metadata mismatch for {key}: {file_path}"
                )
        if (validation.facts["rows"] == 0) != (details.get("empty") is True):
            raise CompactValidationError(
                f"date={trade_date} source={source} symbol={symbol} "
                f"file={file_path.name}: empty marker does not match Arrow contents"
            )
        for key, observed in validation.facts.items():
            if key in {"min_latency_ns", "max_latency_ns", "trade_events"}:
                continue
            if details.get(key) != observed:
                raise CompactValidationError(
                    f"date={trade_date} source={source} symbol={symbol} file={file_path.name}: "
                    f"partition fact mismatch; symbol manifest {key} does not match Arrow contents"
                )
        ordering = validation.ordering
        for key in (
            "raw_min_feed_latency_ns",
            "local_timestamp_adjustment_ns",
            "exchange_ordered",
            "local_ordered",
            "requires_dual_order",
        ):
            if details.get(key) != ordering[key]:
                raise CompactCacheError(
                    f"compact ordering metadata mismatch for {key}: {file_path}"
                )
        expected_sidecars = {
            f"{kind}_order": order
            for kind, order in (
                ("exchange", ordering["exchange_order"]),
                ("local", ordering["local_order"]),
            )
            if order is not None
        }
        sidecars = details.get("sidecars", {})
        if set(sidecars) != set(expected_sidecars):
            raise CompactCacheError(f"compact sidecar set mismatch: {file_path}")
        for key, expected_order in expected_sidecars.items():
            sidecar = sidecars[key]
            sidecar_name = _safe_component(sidecar.get("file"), "sidecar file")
            sidecar_path = file_path.parent / sidecar_name
            if (
                not sidecar_path.is_file()
                or sidecar_path.stat().st_size != sidecar.get("bytes")
            ):
                raise CompactCacheError(
                    f"missing or changed compact sidecar: {sidecar_path}"
                )
            if file_sha256(sidecar_path) != sidecar.get("sha256"):
                raise CompactCacheError(
                    f"compact sidecar checksum mismatch: {sidecar_path}"
                )
            validate_order_sidecar(
                sidecar_path,
                expected_order=expected_order,
                expected_details=sidecar,
            )

def validate_date_value(value: str) -> None:
    try:
        time.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"trade_date must be YYYY-MM-DD: {value!r}") from exc


def _safe_component(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or Path(value).name != value or any(
        separator in value for separator in ("/", "\\")
    ):
        raise CompactCacheError(f"invalid compact {label} path component: {value!r}")
    return value


def _validate_source_request(sources: Sequence[CompactSource]) -> None:
    kinds = [source.kind for source in sources]
    if len(kinds) != len(set(kinds)):
        raise CompactCacheError("compact source kinds must be unique per date build")
    paths = [Path(path).resolve() for source in sources for path in source.paths]
    if len(paths) != len(set(paths)):
        raise CompactCacheError(
            "each physical compact source path may be scanned at most once per date build"
        )


__all__ = ("CompactCacheStore",)
