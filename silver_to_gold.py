"""Build the hourly Gold tables from one Silver partition.

Contract: docs/gold-data-contract.md
"""

import argparse
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone

import duckdb
from botocore.exceptions import BotoCoreError, ClientError

from bronze_to_silver import (
    MIN_COMPLETE_CYCLE_RATIO,
    MIN_CYCLE_COVERAGE_RATIO,
    QUARANTINE_REJECTION_REASONS,
    _parse_s3_uri,
    _s3_client,
    copy_s3_object,
    delete_s3_object,
    get_connection,
    read_completeness_marker,
    resolve_silver_artifacts,
    s3_object_exists,
    validate_partition,
    validate_silver_schema,
)

logger = logging.getLogger(__name__)

COMPLETENESS_STATUSES = ("complete", "incomplete", "unknown")
VEHICLE_TYPES = ("bus", "tram")

TSTZ = "TIMESTAMP WITH TIME ZONE"
REASON_LIST = "STRUCT(reason VARCHAR, records BIGINT)[]"

# GOLD_TABLES defines the complete set of tables in a Gold publication.
# The commit manifest is written only after all tables are staged, validated,
# and copied to versioned paths (see docs/gold-data-contract.md).
GOLD_TABLES = (
    "vehicle_activity_hourly",
    "hourly_line_metrics",
    "hourly_network_metrics",
    "hourly_data_quality",
)

GOLD_SCHEMAS = {
    "hourly_network_metrics": [
        ("date", "DATE"),
        ("hour", "TINYINT"),
        ("active_vehicles", "BIGINT"),
        ("active_bus_vehicles", "BIGINT"),
        ("active_tram_vehicles", "BIGINT"),
        ("position_events", "BIGINT"),
        ("active_lines", "BIGINT"),
        ("active_brigades", "BIGINT"),
        ("silver_records", "BIGINT"),
        ("first_gps_time_utc", TSTZ),
        ("last_gps_time_utc", TSTZ),
        ("data_completeness_status", "VARCHAR"),
        ("is_final", "BOOLEAN"),
        ("source_max_ingested_at", TSTZ),
        ("pipeline_run_at", TSTZ),
    ],
    "hourly_line_metrics": [
        ("date", "DATE"),
        ("hour", "TINYINT"),
        ("vehicle_type", "VARCHAR"),
        ("line", "VARCHAR"),
        ("active_vehicles", "BIGINT"),
        ("position_events", "BIGINT"),
        ("active_brigades", "BIGINT"),
        ("first_gps_time_utc", TSTZ),
        ("last_gps_time_utc", TSTZ),
        ("data_completeness_status", "VARCHAR"),
        ("is_final", "BOOLEAN"),
        ("source_max_ingested_at", TSTZ),
        ("pipeline_run_at", TSTZ),
    ],
    "vehicle_activity_hourly": [
        ("date", "DATE"),
        ("hour", "TINYINT"),
        ("vehicle_type", "VARCHAR"),
        ("vehicle_number", "VARCHAR"),
        ("position_events", "BIGINT"),
        ("unique_lines", "BIGINT"),
        ("observed_lines", "VARCHAR[]"),
        ("first_gps_time_utc", TSTZ),
        ("last_gps_time_utc", TSTZ),
        ("active_seconds", "BIGINT"),
        ("first_lat", "DOUBLE"),
        ("first_lon", "DOUBLE"),
        ("last_lat", "DOUBLE"),
        ("last_lon", "DOUBLE"),
        ("min_lat", "DOUBLE"),
        ("max_lat", "DOUBLE"),
        ("min_lon", "DOUBLE"),
        ("max_lon", "DOUBLE"),
        ("data_completeness_status", "VARCHAR"),
        ("is_final", "BOOLEAN"),
        ("source_max_ingested_at", TSTZ),
        ("pipeline_run_at", TSTZ),
    ],
    "hourly_data_quality": [
        ("date", "DATE"),
        ("hour", "TINYINT"),
        ("silver_partition_status", "VARCHAR"),
        ("data_completeness_status", "VARCHAR"),
        ("manifest_cycles", "BIGINT"),
        ("complete_manifest_cycles", "BIGINT"),
        ("partial_manifest_cycles", "BIGINT"),
        ("complete_cycle_ratio", "DOUBLE"),
        ("expected_slots", "BIGINT"),
        ("observed_slots", "BIGINT"),
        ("complete_slots", "BIGINT"),
        ("slot_coverage_ratio", "DOUBLE"),
        ("complete_slot_ratio", "DOUBLE"),
        ("bronze_records", "BIGINT"),
        ("silver_position_events", "BIGINT"),
        ("quarantined_records", "BIGINT"),
        ("rejection_pct", "DOUBLE"),
        ("duplicate_records", "BIGINT"),
        ("integrity_rejection_pct", "DOUBLE"),
        ("quarantined_by_reason", REASON_LIST),
        ("safe_for_dashboard", "BOOLEAN"),
        ("is_final", "BOOLEAN"),
        ("source_max_ingested_at", TSTZ),
        ("pipeline_run_at", TSTZ),
    ],
}

UNKNOWN_LINE = "unknown"
# Rejections that are routine for the ZTM feed (old GPS fixes, repeated rows) and
# therefore do not make an hour unsafe; everything else counts as an integrity issue.
ROUTINE_REJECTION_REASONS = ("duplicate", "stale_gps_time")
MAX_INTEGRITY_REJECTION_PCT = 5.0
# Late Bronze data may still change an hour for this long after it closes.
FINALITY_GRACE = timedelta(minutes=15)

GOLD_KEYS = {
    "hourly_network_metrics": ("date", "hour"),
    "hourly_line_metrics": ("date", "hour", "vehicle_type", "line"),
    "vehicle_activity_hourly": ("date", "hour", "vehicle_type", "vehicle_number"),
    "hourly_data_quality": ("date", "hour"),
}


class SilverDataMissingError(FileNotFoundError):
    """Raised when the Silver partition for the hour does not exist."""


def partition_uris(date_str: str, hour_str: str) -> dict:
    date_str, hour_str = validate_partition(date_str, hour_str)
    partition = f"date={date_str}/hour={hour_str}"
    publication_id = uuid.uuid4().hex

    silver_artifacts = resolve_silver_artifacts(date_str, hour_str)

    return {
        "silver": silver_artifacts["data"],
        "quarantine": silver_artifacts["quarantine"],
        "gold": {
            table: (
                f"s3://gold/_versions/{partition}/{publication_id}/{table}/data.parquet"
            )
            for table in GOLD_TABLES
        },
        "staging": {
            table: (
                f"s3://gold/_staging/{partition}/{publication_id}/"
                f"{table}.parquet.pending"
            )
            for table in GOLD_TABLES
        },
        "publication_id": publication_id,
        "partition": partition,
        "manifest_uri": publication_manifest_uri(date_str, hour_str),
    }


def publication_manifest_uri(date_str: str, hour_str: str) -> str:
    partition = f"date={date_str}/hour={hour_str}"
    return f"s3://gold/_commits/{partition}/manifest.json"


def diagnostic_manifest_uri(date_str: str, hour_str: str) -> str:
    partition = f"date={date_str}/hour={hour_str}"
    return f"s3://gold/_diagnostics/{partition}/manifest.json"


def write_publication_manifest(manifest_uri: str, manifest: dict) -> None:
    """Atomically replace the single S3 commit manifest."""
    bucket, key = _parse_s3_uri(manifest_uri)
    _s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(manifest, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def write_diagnostic_manifest(manifest_uri: str, manifest: dict) -> None:
    """Write a diagnostic manifest without replacing the full Gold commit."""
    bucket, key = _parse_s3_uri(manifest_uri)
    _s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(manifest, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def _non_negative_int(marker: dict, field: str):
    value = marker.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"Completeness marker has invalid {field}: {value!r}")
    return value


def load_completeness(date_str: str, hour_str: str, s3_client=None) -> dict:
    """Read and validate the Silver completeness marker.

    A missing marker yields status 'unknown' with unknown (None) counters, which
    is distinct from a real marker that reports zero cycles.
    """
    marker = read_completeness_marker(date_str, hour_str, s3_client)
    status = marker.get("status")
    if status not in COMPLETENESS_STATUSES:
        raise ValueError(f"Completeness marker has invalid status: {status!r}")
    for field, expected in (("date", date_str), ("hour", hour_str)):
        if field in marker and str(marker[field]) != expected:
            raise ValueError(
                f"Completeness marker {field} {marker[field]!r} does not match "
                f"partition {expected!r}"
            )

    cycles = _non_negative_int(marker, "cycles")
    complete_cycles = _non_negative_int(marker, "complete_cycles")
    if (cycles is None) != (complete_cycles is None):
        raise ValueError("Completeness marker must have both cycles counters or none")
    if cycles is not None and complete_cycles > cycles:
        raise ValueError("Completeness marker has complete_cycles > cycles")
    expected_cycles = _non_negative_int(marker, "expected_cycles")
    observed_slots = _non_negative_int(marker, "observed_slots")
    complete_slots = _non_negative_int(marker, "complete_slots")

    if (observed_slots is None) != (complete_slots is None):
        raise ValueError("Completeness marker must have both slot counters or none")

    if observed_slots is not None:
        if expected_cycles is None:
            raise ValueError("Slot counters require expected_cycles")
        if complete_slots > observed_slots:
            raise ValueError("Completeness marker has complete_slots > observed_slots")
        if observed_slots > expected_cycles:
            raise ValueError("Completeness marker has observed_slots > expected_cycles")
        if cycles is None or observed_slots > cycles:
            raise ValueError("Slot counters exceed known manifest count")
        if complete_slots > complete_cycles:
            raise ValueError("Completeness marker has complete_slots > complete_cycles")
        if status == "complete" and (
            expected_cycles == 0
            or observed_slots / expected_cycles < MIN_CYCLE_COVERAGE_RATIO
        ):
            raise ValueError("Complete marker does not meet the minimum slot coverage")
        if status == "complete" and (
            observed_slots == 0
            or complete_slots / observed_slots < MIN_COMPLETE_CYCLE_RATIO
        ):
            raise ValueError(
                "Complete marker does not meet the minimum complete slot ratio"
            )
    return {
        "status": status,
        "cycles": cycles,
        "complete_cycles": complete_cycles,
        "expected_cycles": expected_cycles,
        "observed_slots": observed_slots,
        "complete_slots": complete_slots,
        "slot_coverage_ratio": (
            observed_slots / expected_cycles
            if observed_slots is not None and expected_cycles > 0
            else None
        ),
        "complete_slot_ratio": (
            complete_slots / observed_slots
            if observed_slots is not None and observed_slots > 0
            else None
        ),
    }


def _sql_bigint(value) -> str:
    return "CAST(NULL AS BIGINT)" if value is None else f"CAST({int(value)} AS BIGINT)"


def is_hour_final(date_str: str, hour_str: str, run_at: datetime) -> bool:
    """An hour is final only once it has closed and the grace period has passed."""
    hour_start = datetime.strptime(f"{date_str} {hour_str}", "%Y-%m-%d %H").replace(
        tzinfo=timezone.utc
    )
    return run_at >= hour_start + timedelta(hours=1) + FINALITY_GRACE


def _common_columns(date_str: str, hour_str: str, status: str, run_at: datetime):
    final = "TRUE" if is_hour_final(date_str, hour_str, run_at) else "FALSE"
    return (
        f"DATE '{date_str}' AS date, CAST({int(hour_str)} AS TINYINT) AS hour",
        (
            f"'{status}' AS data_completeness_status, "
            f"{final} AS is_final, "
            "(SELECT MAX(ingested_at) FROM silver_hour) AS source_max_ingested_at, "
            f"CAST('{run_at.isoformat()}' AS TIMESTAMPTZ) AS pipeline_run_at"
        ),
    )


def load_silver_hour(con, silver_uri: str) -> int:
    """Validate and load the Silver partition into the temp table silver_hour."""
    validate_silver_schema(con, silver_uri)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE silver_hour AS
        SELECT
            vehicle_type,
            CAST(vehicle_number AS VARCHAR) AS vehicle_number,
            NULLIF(TRIM(CAST(line AS VARCHAR)), '') AS line,
            NULLIF(TRIM(CAST(brigade AS VARCHAR)), '') AS brigade,
            lat,
            lon,
            gps_time_utc,
            ingested_at
        FROM read_parquet('{silver_uri}', hive_partitioning = false)
        """
    )
    rows, bad_types, bad_vehicles, bad_times, bad_coords, duplicates = con.execute(
        """
        SELECT
            COUNT(*),
            COUNT(*) FILTER (
                WHERE vehicle_type IS NULL
                   OR vehicle_type NOT IN ('bus', 'tram')
            ),
            COUNT(*) FILTER (
                WHERE vehicle_number IS NULL OR TRIM(vehicle_number) = ''
            ),
            COUNT(*) FILTER (
                WHERE gps_time_utc IS NULL
                   OR ingested_at IS NULL
                   OR gps_time_utc > ingested_at
                   OR ingested_at - gps_time_utc > INTERVAL 5 MINUTE
            ),
            COUNT(*) FILTER (
                WHERE lat IS NULL
                   OR lon IS NULL
                   OR lat NOT BETWEEN 51.9 AND 52.6
                   OR lon NOT BETWEEN 20.5 AND 21.6
            ),
            COUNT(*) - (
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT vehicle_type, vehicle_number, gps_time_utc
                    FROM silver_hour
                )
            )
        FROM silver_hour
        """
    ).fetchone()
    problems = {
        "unexpected_vehicle_type": bad_types,
        "missing_vehicle_number": bad_vehicles,
        "invalid_required_timestamp": bad_times,
        "invalid_coordinates": bad_coords,
        "duplicate_observations": duplicates,
    }
    problems = {name: count for name, count in problems.items() if count}
    if problems:
        raise ValueError(
            f"Silver content violates the Silver contract ({problems}); "
            "refusing to build Gold."
        )
    return rows


def load_quarantine_hour(con, quarantine_uri: str) -> None:
    try:
        con.execute(
            "CREATE OR REPLACE TEMP TABLE quarantine_hour AS "
            f"SELECT rejection_reason FROM read_parquet('{quarantine_uri}', "
            "hive_partitioning = false)"
        )
    except duckdb.Error as exc:
        raise ValueError(
            f"Could not read quarantine partition {quarantine_uri}: {exc}"
        ) from exc

    reason_type = con.execute("DESCRIBE quarantine_hour").fetchone()[1]
    if reason_type != "VARCHAR":
        raise ValueError("Quarantine rejection_reason must have type VARCHAR")
    unknown_reasons = con.execute(
        """
        SELECT COUNT(*)
        FROM quarantine_hour
        WHERE rejection_reason NOT IN (SELECT unnest(?))
        """,
        [list(QUARANTINE_REJECTION_REASONS)],
    ).fetchone()[0]
    if unknown_reasons:
        raise ValueError("Quarantine contains an unknown rejection_reason")
    invalid_reasons = con.execute(
        """
        SELECT COUNT(*)
        FROM quarantine_hour
        WHERE rejection_reason IS NULL
           OR TRIM(CAST(rejection_reason AS VARCHAR)) = ''
        """
    ).fetchone()[0]
    if invalid_reasons:
        raise ValueError("Quarantine contains missing or empty rejection_reason")


def build_metric_tables(
    con, date_str: str, hour_str: str, status: str, run_at: datetime
) -> None:
    head, tail = _common_columns(date_str, hour_str, status, run_at)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE hourly_network_metrics AS
        SELECT
            {head},
            COUNT(DISTINCT (vehicle_type, vehicle_number)) AS active_vehicles,
            COUNT(DISTINCT vehicle_number) FILTER (WHERE vehicle_type = 'bus')
                AS active_bus_vehicles,
            COUNT(DISTINCT vehicle_number) FILTER (WHERE vehicle_type = 'tram')
                AS active_tram_vehicles,
            COUNT(*) AS position_events,
            COUNT(DISTINCT (vehicle_type, line)) FILTER (WHERE line IS NOT NULL)
                AS active_lines,
            COUNT(DISTINCT (vehicle_type, line, brigade))
                FILTER (WHERE line IS NOT NULL AND brigade IS NOT NULL)
                AS active_brigades,
            COUNT(*) AS silver_records,
            MIN(gps_time_utc) AS first_gps_time_utc,
            MAX(gps_time_utc) AS last_gps_time_utc,
            {tail}
        FROM silver_hour
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE hourly_line_metrics AS
        SELECT
            {head},
            vehicle_type,
            COALESCE(line, '{UNKNOWN_LINE}') AS line,
            COUNT(DISTINCT vehicle_number) AS active_vehicles,
            COUNT(*) AS position_events,
            COUNT(DISTINCT brigade) AS active_brigades,
            MIN(gps_time_utc) AS first_gps_time_utc,
            MAX(gps_time_utc) AS last_gps_time_utc,
            {tail}
        FROM silver_hour
        GROUP BY vehicle_type, COALESCE(line, '{UNKNOWN_LINE}')
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE vehicle_activity_hourly AS
        SELECT
            {head},
            vehicle_type,
            vehicle_number,
            COUNT(*) AS position_events,
            COUNT(DISTINCT line) AS unique_lines,
            COALESCE(
                list_sort(list_distinct(list(line) FILTER (WHERE line IS NOT NULL))),
                CAST([] AS VARCHAR[])
            ) AS observed_lines,
            MIN(gps_time_utc) AS first_gps_time_utc,
            MAX(gps_time_utc) AS last_gps_time_utc,
            CAST(date_diff('second', MIN(gps_time_utc), MAX(gps_time_utc)) AS BIGINT)
                AS active_seconds,
            arg_min(lat, gps_time_utc) AS first_lat,
            arg_min(lon, gps_time_utc) AS first_lon,
            arg_max(lat, gps_time_utc) AS last_lat,
            arg_max(lon, gps_time_utc) AS last_lon,
            MIN(lat) AS min_lat,
            MAX(lat) AS max_lat,
            MIN(lon) AS min_lon,
            MAX(lon) AS max_lon,
            {tail}
        FROM silver_hour
        GROUP BY vehicle_type, vehicle_number
        """
    )


def build_quality_table(
    con,
    date_str: str,
    hour_str: str,
    completeness: dict,
    silver_rows,
    quarantine_present: bool,
    run_at: datetime,
) -> None:
    head, _ = _common_columns(date_str, hour_str, completeness["status"], run_at)
    cycles = completeness["cycles"]
    complete_cycles = completeness["complete_cycles"]
    ratio = (
        f"CAST({complete_cycles} AS DOUBLE) / {cycles}"
        if cycles
        else "CAST(NULL AS DOUBLE)"
    )
    if quarantine_present:
        quarantined = "(SELECT COUNT(*) FROM quarantine_hour)"
        duplicates = (
            "(SELECT COUNT(*) FROM quarantine_hour "
            "WHERE rejection_reason = 'duplicate')"
        )
        routine = ", ".join(f"'{r}'" for r in ROUTINE_REJECTION_REASONS)
        integrity = (
            "(SELECT COUNT(*) FROM quarantine_hour "
            f"WHERE rejection_reason NOT IN ({routine}))"
        )
        by_reason = (
            "COALESCE((SELECT list(struct_pack(reason := rejection_reason, "
            "records := records) ORDER BY rejection_reason) FROM ("
            "SELECT rejection_reason, COUNT(*) AS records FROM quarantine_hour "
            f"GROUP BY rejection_reason)), CAST([] AS {REASON_LIST}))"
        )
    else:
        quarantined = duplicates = integrity = "CAST(NULL AS BIGINT)"
        by_reason = f"CAST(NULL AS {REASON_LIST})"
    # Bronze = Silver + quarantine by construction (bronze_to_silver verifies it).
    if silver_rows is not None and quarantine_present:
        bronze = f"{int(silver_rows)} + {quarantined}"
    else:
        bronze = "CAST(NULL AS BIGINT)"
    partial = (
        _sql_bigint(cycles - complete_cycles)
        if cycles is not None
        else _sql_bigint(None)
    )
    final = "TRUE" if is_hour_final(date_str, hour_str, run_at) else "FALSE"
    source_max = (
        "(SELECT MAX(ingested_at) FROM silver_hour)"
        if silver_rows is not None
        else f"CAST(NULL AS {TSTZ})"
    )
    safe = (
        f"{'TRUE' if completeness['status'] == 'complete' else 'FALSE'} "
        f"AND {final} "
        f"AND {'TRUE' if silver_rows else 'FALSE'} "
        "AND integrity_rejection_pct IS NOT NULL "
        f"AND integrity_rejection_pct <= {MAX_INTEGRITY_REJECTION_PCT}"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE hourly_data_quality AS
        WITH counts AS (
            SELECT
                CAST({bronze} AS BIGINT) AS bronze_records,
                CAST({quarantined} AS BIGINT) AS quarantined_records,
                CAST({duplicates} AS BIGINT) AS duplicate_records,
                CAST({integrity} AS BIGINT) AS integrity_records
        ),
        pcts AS (
            SELECT
                *,
                CASE WHEN bronze_records > 0
                    THEN 100.0 * quarantined_records / bronze_records END
                    AS rejection_pct,
                CASE WHEN bronze_records > 0
                    THEN 100.0 * integrity_records / bronze_records END
                    AS integrity_rejection_pct
            FROM counts
        )
        SELECT
            {head},
            '{"present" if silver_rows is not None else "missing"}'
                AS silver_partition_status,
            '{completeness["status"]}' AS data_completeness_status,
            {_sql_bigint(cycles)} AS manifest_cycles,
            {_sql_bigint(complete_cycles)} AS complete_manifest_cycles,
            {partial} AS partial_manifest_cycles,
            {ratio} AS complete_cycle_ratio,
            {_sql_bigint(completeness.get("expected_cycles"))} AS expected_slots,
            {_sql_bigint(completeness.get("observed_slots"))} AS observed_slots,
            {_sql_bigint(completeness.get("complete_slots"))} AS complete_slots,
            CAST({_sql_bigint(completeness.get("observed_slots"))} AS DOUBLE)
                / NULLIF({_sql_bigint(completeness.get("expected_cycles"))}, 0)
                AS slot_coverage_ratio,
            CAST({_sql_bigint(completeness.get("complete_slots"))} AS DOUBLE)
                / NULLIF({_sql_bigint(completeness.get("observed_slots"))}, 0)
                AS complete_slot_ratio,
            bronze_records,
            {_sql_bigint(silver_rows)} AS silver_position_events,
            quarantined_records,
            CAST(rejection_pct AS DOUBLE) AS rejection_pct,
            duplicate_records,
            CAST(integrity_rejection_pct AS DOUBLE) AS integrity_rejection_pct,
            CAST({by_reason} AS {REASON_LIST}) AS quarantined_by_reason,
            COALESCE({safe}, FALSE) AS safe_for_dashboard,
            {final} AS is_final,
            CAST({source_max} AS {TSTZ}) AS source_max_ingested_at,
            CAST('{run_at.isoformat()}' AS TIMESTAMPTZ) AS pipeline_run_at
        FROM pcts
        """
    )


def validate_gold_tables(con, tables) -> None:
    """Check key uniqueness and cross-table consistency before publishing."""
    for table in tables:
        keys = ", ".join(GOLD_KEYS[table])
        total, distinct = con.execute(
            f"SELECT COUNT(*), (SELECT COUNT(*) FROM "
            f"(SELECT DISTINCT {keys} FROM {table})) FROM {table}"
        ).fetchone()
        if total != distinct:
            raise ValueError(
                f"Gold table {table} violates its unique key ({keys}): "
                f"{total} rows, {distinct} distinct keys"
            )
    if "hourly_network_metrics" not in tables:
        return

    network = con.execute(
        "SELECT active_vehicles, active_bus_vehicles + active_tram_vehicles, "
        "position_events FROM hourly_network_metrics"
    ).fetchall()
    if len(network) != 1:
        raise ValueError(f"hourly_network_metrics must have 1 row, got {len(network)}")
    active, by_type, events = network[0]
    vehicle_rows, vehicle_events = con.execute(
        "SELECT COUNT(*), COALESCE(SUM(position_events), 0) "
        "FROM vehicle_activity_hourly"
    ).fetchone()
    line_events = con.execute(
        "SELECT COALESCE(SUM(position_events), 0) FROM hourly_line_metrics"
    ).fetchone()[0]
    checks = {
        "bus + tram vehicles != active vehicles": by_type != active,
        "vehicle rows != active vehicles": vehicle_rows != active,
        "vehicle events != position events": int(vehicle_events) != events,
        "line events != position events": int(line_events) != events,
    }
    failed = [name for name, bad in checks.items() if bad]
    if failed:
        raise ValueError(f"Gold consistency checks failed: {failed}")


def _normalize_type(type_name: str) -> str:
    return type_name.replace(" ", "").upper()


def validate_staged_file(con, table: str, staged_uri: str, expected_rows: int):
    try:
        schema = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{staged_uri}', "
            "hive_partitioning = false)"
        ).fetchall()
    except duckdb.Error as exc:
        raise ValueError(f"Gold validation failed for {table}: {exc}") from exc
    actual = [(name, _normalize_type(str(kind))) for name, kind, *_ in schema]
    expected = [(name, _normalize_type(kind)) for name, kind in GOLD_SCHEMAS[table]]
    if actual != expected:
        raise ValueError(
            f"Gold schema validation failed for {table}: expected {expected}, "
            f"got {actual}"
        )
    rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{staged_uri}', hive_partitioning = false)"
    ).fetchone()[0]
    if rows != expected_rows:
        raise ValueError(
            f"Gold validation failed for {table}: staged file has {rows} rows, "
            f"expected {expected_rows}; refusing to publish."
        )


def publish_tables(con, tables, uris: dict) -> dict:
    """Stage and validate all files before committing a complete Gold publication."""
    staging = uris["staging"]
    row_counts = {}

    try:
        # 1. Write every table to staging and validate it before publishing.
        for table in tables:
            order = ", ".join(GOLD_KEYS[table])
            row_counts[table] = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[
                0
            ]

            con.execute(
                f"COPY (SELECT * FROM {table} ORDER BY {order}) "
                f"TO '{staging[table]}' (FORMAT parquet, OVERWRITE_OR_IGNORE true)"
            )
            validate_staged_file(con, table, staging[table], row_counts[table])

        # 2. Copy validated files to unique, versioned destinations.
        for table in tables:
            copy_s3_object(staging[table], uris["gold"][table])

        # 3. Commit only a complete publication.
        if tuple(tables) == GOLD_TABLES:
            is_final = con.execute(
                "SELECT is_final FROM hourly_data_quality"
            ).fetchone()[0]
            manifest = {
                "publication_id": uris["publication_id"],
                "partition": uris["partition"],
                "source_silver_uri": uris["silver"],
                "source_silver_etag": uris.get("source_silver_etag"),
                "is_final": is_final,
                "tables": {
                    table: {
                        "uri": uris["gold"][table],
                        "rows": row_counts[table],
                    }
                    for table in tables
                },
            }
            write_publication_manifest(uris["manifest_uri"], manifest)

    finally:
        for table in tables:
            try:
                delete_s3_object(staging[table])
            except (BotoCoreError, ClientError) as exc:
                logger.warning(
                    "Could not remove temporary Gold object %s: %s",
                    staging[table],
                    exc,
                )

    return row_counts


def build_gold_hour(con, date_str, hour_str, run_at=None, s3_client=None) -> dict:
    """Build and publish all Gold tables for one UTC hour; safe to re-run."""
    date_str, hour_str = validate_partition(date_str, hour_str)
    run_at = (run_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    uris = partition_uris(date_str, hour_str)

    completeness = load_completeness(date_str, hour_str, s3_client)
    silver_present = s3_object_exists(uris["silver"])
    quarantine_present = s3_object_exists(uris["quarantine"])
    legacy_key = f"vehicle_positions/{uris['partition']}/data.parquet"
    if silver_present and uris["silver"] == f"s3://silver/{legacy_key}":
        source_client = s3_client or _s3_client()
        source_metadata = source_client.head_object(
            Bucket="silver",
            Key=legacy_key,
        )
        source_etag = source_metadata.get("ETag")
        if not isinstance(source_etag, str) or not source_etag:
            raise ValueError("Legacy Silver object is missing a valid ETag")
        uris["source_silver_etag"] = source_etag

    silver_rows = load_silver_hour(con, uris["silver"]) if silver_present else None
    if quarantine_present:
        load_quarantine_hour(con, uris["quarantine"])

    build_quality_table(
        con,
        date_str,
        hour_str,
        completeness,
        silver_rows,
        quarantine_present,
        run_at,
    )

    if not silver_rows:
        # Publish diagnostics separately; never replace the full Gold commit.
        diagnostic_tables = ("hourly_data_quality",)
        row_counts = publish_tables(con, diagnostic_tables, uris)
        diagnostic_manifest = {
            "publication_id": uris["publication_id"],
            "partition": uris["partition"],
            "status": "silver_empty" if silver_present else "silver_missing",
            "tables": {
                table: {
                    "uri": uris["gold"][table],
                    "rows": row_counts[table],
                }
                for table in diagnostic_tables
            },
        }
        write_diagnostic_manifest(
            diagnostic_manifest_uri(date_str, hour_str),
            diagnostic_manifest,
        )
        if silver_present:
            raise ValueError(
                f"Silver partition is empty for {date_str} hour {hour_str}"
            )
        raise SilverDataMissingError(
            f"No Silver partition for {date_str} hour {hour_str}; "
            "published a data-quality record only."
        )

    build_metric_tables(con, date_str, hour_str, completeness["status"], run_at)
    validate_gold_tables(con, GOLD_TABLES)
    row_counts = publish_tables(con, GOLD_TABLES, uris)

    if completeness["status"] != "complete":
        logger.warning(
            "%s hour %s Gold published with completeness '%s'; metrics must not be "
            "treated as reliable.",
            date_str,
            hour_str,
            completeness["status"],
        )
    if not is_hour_final(date_str, hour_str, run_at):
        logger.warning(
            "%s hour %s is not final yet (still open to late data); "
            "it is published with is_final = false and safe_for_dashboard = false.",
            date_str,
            hour_str,
        )
    logger.info(
        "%s hour %s Gold published: status=%s, final=%s, rows=%s",
        date_str,
        hour_str,
        completeness["status"],
        is_hour_final(date_str, hour_str, run_at),
        row_counts,
    )
    return {"status": completeness["status"], "rows": row_counts}


def main():
    previous_hour = datetime.now(timezone.utc) - timedelta(hours=1)

    parser = argparse.ArgumentParser(description="Silver -> gold for one hour")
    parser.add_argument("--date", default=previous_hour.strftime("%Y-%m-%d"))
    parser.add_argument("--hour", default=previous_hour.strftime("%H"))
    args = parser.parse_args()

    con = get_connection()
    close_error = None
    try:
        build_gold_hour(con, args.date, args.hour)
    finally:
        try:
            con.close()
        except duckdb.Error as exc:
            close_error = exc
            logger.error("Could not close Silver-to-Gold CLI connection")

    if close_error is not None:
        raise close_error


if __name__ == "__main__":
    main()
