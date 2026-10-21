"""Shared ingestion manifest validation; no storage or transformation imports."""

from datetime import datetime, timezone


def validate_cycle_manifest(manifest: dict) -> None:
    if not isinstance(manifest, dict):
        raise TypeError("Cycle manifest must be a JSON object")
    if "schema_version" in manifest:
        version = manifest["schema_version"]
        if (
            isinstance(version, bool)
            or not isinstance(version, int)
            or version not in (1, 2)
        ):
            raise ValueError("Cycle manifest has an unsupported schema_version")
        required_fields = {
            "status",
            "buses",
            "trams",
            "failed_sources",
            "empty_sources",
            "ingested_at",
            "cycle_started_at",
            "cycle_slot_start",
        }
        missing_fields = required_fields - manifest.keys()
        if missing_fields:
            raise ValueError(
                f"Cycle manifest is missing required fields: {sorted(missing_fields)}"
            )
        for field in (
            "ingested_at",
            "cycle_started_at",
            "cycle_slot_start",
        ):
            try:
                timestamp = datetime.fromisoformat(manifest[field])
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Cycle manifest has an invalid {field}") from exc
            if timestamp.tzinfo is None:
                raise ValueError(f"Cycle manifest {field} must be timezone-aware")
    if manifest.get("schema_version") == 2:
        interval = manifest.get("poll_interval_seconds")
        if isinstance(interval, bool) or not isinstance(interval, int) or interval <= 0:
            raise ValueError("Cycle manifest has invalid poll_interval_seconds")
        sources = manifest.get("source_objects")
        if not isinstance(sources, dict) or set(sources) != {"bus", "tram"}:
            raise ValueError("Cycle manifest must identify both source_objects")
        slot = datetime.fromisoformat(manifest["cycle_slot_start"]).astimezone(
            timezone.utc
        )
        if slot.microsecond or int(slot.timestamp()) % interval:
            raise ValueError("Cycle manifest slot conflicts with poll_interval_seconds")
        partition = f"date={slot:%Y-%m-%d}/hour={slot:%H}/"
        for label, counter in (("bus", "buses"), ("tram", "trams")):
            source = sources[label]
            if manifest[counter] == 0:
                if source is not None:
                    raise ValueError(
                        "Cycle manifest source_objects conflict with source counts"
                    )
                continue
            if not isinstance(source, dict):
                raise TypeError(
                    "Cycle manifest source_objects must identify nonempty sources"
                )
            key, size = source.get("key"), source.get("size")
            prefix = f"vehicle_type={label}/{partition}"
            if (
                not isinstance(key, str)
                or not key.startswith(prefix)
                or not key.endswith(".jsonl")
                or "/" in key[len(prefix) :]
            ):
                raise ValueError("Cycle manifest has invalid source_objects key")
            if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
                raise ValueError("Cycle manifest has invalid source_objects size")
            if "etag" in source and (
                not isinstance(source["etag"], str) or not source["etag"]
            ):
                raise ValueError("Cycle manifest has invalid source_objects etag")
    status = manifest.get("status")
    if status not in ("complete", "partial"):
        raise ValueError("Cycle manifest has an invalid status")

    has_buses = "buses" in manifest
    has_trams = "trams" in manifest
    if has_buses != has_trams:
        raise ValueError("Cycle manifest must contain both source counters")

    if has_buses:
        for field in ("buses", "trams"):
            value = manifest[field]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"Cycle manifest has an invalid {field} count")

        expected_status = (
            "complete" if manifest["buses"] > 0 and manifest["trams"] > 0 else "partial"
        )
        if status != expected_status:
            raise ValueError("Cycle manifest status conflicts with source counts")

    has_failed = "failed_sources" in manifest
    has_empty = "empty_sources" in manifest
    if not has_failed and not has_empty:
        return
    if has_failed != has_empty:
        raise ValueError("Cycle manifest must contain both sources lists")

    for field in ("failed_sources", "empty_sources"):
        values = manifest[field]
        if not isinstance(values, list) or any(
            not isinstance(source, str) or source not in ("bus", "tram")
            for source in values
        ):
            raise ValueError(f"Cycle manifest has invalid {field}")
        if len(values) != len(set(values)):
            raise ValueError(f"Cycle manifest has duplicate {field}")

    failed = set(manifest["failed_sources"])
    empty = set(manifest["empty_sources"])
    if failed & empty:
        raise ValueError("Cycle manifest sources lists overlap")

    if has_buses:
        expected_missing = {
            source
            for source, field in (("bus", "buses"), ("tram", "trams"))
            if manifest[field] == 0
        }
        if failed | empty != expected_missing:
            raise ValueError("Cycle manifest sources conflict with source counts")
    elif status == "complete" and (failed or empty):
        raise ValueError("Cycle manifest sources conflict with complete status")
