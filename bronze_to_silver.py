import argparse
import json
import logging
import math
import os
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse

import boto3
import duckdb
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

logging.Formatter.converter = time.gmtime
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
load_dotenv()

EXPECTED_SILVER_COLUMNS = [
    "vehicle_type",
    "vehicle_number",
    "line",
    "brigade",
    "lat",
    "lon",
    "gps_time_utc",
    "ingested_at",
]

EXPECTED_SILVER_TYPES = {
    "lat": "DOUBLE",
    "lon": "DOUBLE",
    "gps_time_utc": "TIMESTAMP WITH TIME ZONE",
    "ingested_at": "TIMESTAMP WITH TIME ZONE",
}


class BronzeDataMissingError(FileNotFoundError):
    """Raised when an hourly Bronze partition contains no files."""


QUARANTINE_REJECTION_REASONS = (
    "invalid_coordinates",
    "missing_vehicle_number",
    "ambiguous_local_time",
    "nonexistent_local_time",
    "missing_or_invalid_timestamp",
    "future_gps_time",
    "stale_gps_time",
    "duplicate",
)

BRONZE_JSON_COLUMNS = {
    "vehicle_type": "VARCHAR",
    "VehicleNumber": "VARCHAR",
    "Lines": "VARCHAR",
    "Brigade": "VARCHAR",
    "Lat": "VARCHAR",
    "Lon": "VARCHAR",
    "Time": "VARCHAR",
    "_ingested_at": "VARCHAR",
}

BRONZE_SOURCE_CTE = """
WITH source AS (
    SELECT *
    FROM read_json_auto(
        '{bronze_path}',
        columns = {bronze_columns},
        format = 'newline_delimited',
        hive_partitioning = true
    )
),
""".replace(
    "{bronze_columns}",
    repr(BRONZE_JSON_COLUMNS).replace("{", "{{").replace("}", "}}"),
)

BASE_CTES = (
    BRONZE_SOURCE_CTE
    + """
typed_base AS (
    SELECT
        vehicle_type,
        VehicleNumber AS vehicle_number,
        Lines         AS line,
        Brigade       AS brigade,
        TRY_CAST(Lat AS DOUBLE) AS lat,
        TRY_CAST(Lon AS DOUBLE) AS lon,
        TRY_CAST("Time" AS TIMESTAMP) AS gps_time_local,
        TRY_CAST(_ingested_at AS TIMESTAMPTZ) AS ingested_at
    FROM source
),
typed AS (
    SELECT
        *,
        TRY_CAST(gps_time_local AT TIME ZONE 'Europe/Warsaw' AS TIMESTAMPTZ)
            AS gps_time_utc,
        CASE
            WHEN gps_time_local IS NULL THEN NULL
            ELSE
                CAST(
                    timezone(
                        'Europe/Warsaw',
                        (gps_time_local AT TIME ZONE 'UTC') - INTERVAL 1 HOUR
                    ) = gps_time_local AS INTEGER
                )
                + CAST(
                    timezone(
                        'Europe/Warsaw',
                        (gps_time_local AT TIME ZONE 'UTC') - INTERVAL 2 HOUR
                    ) = gps_time_local AS INTEGER
                )
        END AS local_time_candidate_count
    FROM typed_base
),
valid_coordinates AS (
    SELECT *
    FROM typed
    WHERE lat BETWEEN 51.9 AND 52.6
    AND lon BETWEEN 20.5 AND 21.6
),
valid_vehicle AS (
    SELECT *
    FROM valid_coordinates
    WHERE vehicle_number IS NOT NULL
      AND TRIM(CAST(vehicle_number AS VARCHAR)) <> ''
),
valid_gps_time AS (
    SELECT *
    FROM valid_vehicle
    WHERE gps_time_utc IS NOT NULL
    AND ingested_at IS NOT NULL
    AND local_time_candidate_count = 1
    AND ingested_at - gps_time_utc >= INTERVAL 0 SECOND
    AND ingested_at - gps_time_utc <= INTERVAL 5 MINUTE
),
ranked_valid_gps_time AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY vehicle_type, vehicle_number, gps_time_utc
            ORDER BY ingested_at DESC, lat, lon, line, brigade
        ) AS row_num
    FROM valid_gps_time
),
deduplicated AS (
    SELECT
        vehicle_type,
        vehicle_number,
        line,
        brigade,
        lat,
        lon,
        gps_time_utc,
        ingested_at
    FROM ranked_valid_gps_time
    WHERE row_num = 1
)
"""
)
TRANSFORM_SQL = BASE_CTES + "SELECT * FROM deduplicated"

REJECTED_SQL = (
    BASE_CTES
    + """
, rejected_records AS (
    SELECT
        vehicle_type, vehicle_number, line, brigade, lat, lon,
        gps_time_utc, ingested_at, 'invalid_coordinates' AS rejection_reason
    FROM typed
    WHERE lat IS NULL OR lon IS NULL
       OR lat NOT BETWEEN 51.9 AND 52.6
       OR lon NOT BETWEEN 20.5 AND 21.6

    UNION ALL

    SELECT
        vehicle_type, vehicle_number, line, brigade, lat, lon,
        gps_time_utc, ingested_at, 'missing_vehicle_number' AS rejection_reason
    FROM valid_coordinates
    WHERE vehicle_number IS NULL
       OR TRIM(CAST(vehicle_number AS VARCHAR)) = ''

    UNION ALL

    SELECT
        vehicle_type, vehicle_number, line, brigade, lat, lon,
        gps_time_utc, ingested_at,
        CASE
            WHEN local_time_candidate_count > 1 THEN 'ambiguous_local_time'
            WHEN local_time_candidate_count = 0 THEN 'nonexistent_local_time'
            WHEN gps_time_utc IS NULL OR ingested_at IS NULL
                THEN 'missing_or_invalid_timestamp'
            WHEN gps_time_utc > ingested_at THEN 'future_gps_time'
            ELSE 'stale_gps_time'
        END AS rejection_reason
    FROM valid_vehicle
    WHERE gps_time_utc IS NULL OR ingested_at IS NULL
       OR local_time_candidate_count <> 1
       OR ingested_at - gps_time_utc < INTERVAL 0 SECOND
       OR ingested_at - gps_time_utc > INTERVAL 5 MINUTE

    UNION ALL

    SELECT
        vehicle_type, vehicle_number, line, brigade, lat, lon,
        gps_time_utc, ingested_at, 'duplicate' AS rejection_reason
    FROM ranked_valid_gps_time
    WHERE row_num > 1
)
SELECT * FROM rejected_records
"""
)

QUALITY_SQL = (
    BASE_CTES
    + """
SELECT
    (SELECT COUNT(*) FROM source) AS bronze_count,
    (SELECT COUNT(*) FROM valid_coordinates) AS after_coordinates_count,
    (SELECT COUNT(*) FROM valid_vehicle) AS after_vehicle_count,
    (SELECT COUNT(*) FROM valid_gps_time) AS after_time_count,
    (
        SELECT COUNT(*)
        FROM valid_vehicle
        WHERE local_time_candidate_count = 1
          AND gps_time_utc IS NOT NULL
          AND ingested_at IS NOT NULL
          AND gps_time_utc > ingested_at
    ) AS future_gps_time_count,
    (
        SELECT COUNT(*)
        FROM valid_vehicle
        WHERE local_time_candidate_count = 1
          AND gps_time_utc IS NOT NULL
          AND ingested_at IS NOT NULL
          AND ingested_at - gps_time_utc > INTERVAL 5 MINUTE
    ) AS stale_gps_time_count,
    (
        SELECT COUNT(*)
        FROM valid_vehicle
        WHERE local_time_candidate_count = 1
          AND gps_time_utc IS NOT NULL
          AND ingested_at IS NOT NULL
          AND ingested_at - gps_time_utc > INTERVAL 5 MINUTE
        AND ingested_at - gps_time_utc <= INTERVAL 10 MINUTE
    ) AS stale_5_to_10_minutes_count,
    (
        SELECT COUNT(*)
        FROM valid_vehicle
        WHERE local_time_candidate_count = 1
          AND gps_time_utc IS NOT NULL
          AND ingested_at IS NOT NULL
          AND ingested_at - gps_time_utc > INTERVAL 10 MINUTE
        AND ingested_at - gps_time_utc <= INTERVAL 30 MINUTE
    ) AS stale_10_to_30_minutes_count,
    (
        SELECT COUNT(*)
        FROM valid_vehicle
        WHERE local_time_candidate_count = 1
          AND gps_time_utc IS NOT NULL
          AND ingested_at IS NOT NULL
          AND ingested_at - gps_time_utc > INTERVAL 30 MINUTE
    ) AS stale_over_30_minutes_count,
    (
        SELECT COUNT(*)
        FROM valid_vehicle
        WHERE gps_time_utc IS NULL OR ingested_at IS NULL
           OR local_time_candidate_count <> 1
    ) AS invalid_gps_time_count,
    (SELECT COUNT(*) FROM deduplicated) AS silver_count
"""
)


def parse_s3_endpoint(url: str) -> tuple[str, bool]:
    parsed = urlparse(url if "://" in url else f"http://{url}")
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(f"Invalid S3_ENDPOINT_URL: {url!r}")
    return parsed.netloc, parsed.scheme == "https"


def get_connection():
    user = os.getenv("MINIO_USER")
    password = os.getenv("MINIO_PASSWORD")
    if not user or not password:
        raise RuntimeError("MINIO_USER and MINIO_PASSWORD must be set")
    endpoint, use_ssl = parse_s3_endpoint(
        os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
    )

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET TimeZone = 'UTC';")

    # DuckDB 1.2 does not accept bound parameters in CREATE SECRET.
    def literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    con.execute(
        f"CREATE SECRET minio (TYPE s3, KEY_ID {literal(user)}, "
        f"SECRET {literal(password)}, ENDPOINT {literal(endpoint)}, "
        f"URL_STYLE 'path', USE_SSL {'true' if use_ssl else 'false'})"
    )
    return con


def validate_partition(date_str: str, hour_str: str) -> tuple[str, str]:
    try:
        parsed_date = date.fromisoformat(date_str)
    except ValueError as exc:
        raise ValueError("date must use YYYY-MM-DD format") from exc

    if parsed_date.strftime("%Y-%m-%d") != date_str:
        raise ValueError("date must use YYYY-MM-DD format")

    if not hour_str.isascii() or not hour_str.isdigit() or len(hour_str) not in (1, 2):
        raise ValueError("hour must be between 0 and 23")

    hour = int(hour_str)
    if not 0 <= hour <= 23:
        raise ValueError("hour must be between 0 and 23")

    return date_str, f"{hour:02d}"


def validate_silver_schema(con, silver_path):
    try:
        schema = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{silver_path}', hive_partitioning = false)"
        ).fetchall()
    except duckdb.Error as exc:
        raise ValueError(
            f"Silver schema validation failed for {silver_path}: {exc}"
        ) from exc

    actual_types = {column[0]: str(column[1]) for column in schema}
    missing = [
        column for column in EXPECTED_SILVER_COLUMNS if column not in actual_types
    ]
    if missing:
        raise ValueError(
            f"Silver schema validation failed for {silver_path}: missing columns {missing}"
        )
    unexpected = [
        column for column in actual_types if column not in EXPECTED_SILVER_COLUMNS
    ]
    if unexpected:
        raise ValueError(
            f"Silver schema validation failed for {silver_path}: "
            f"unexpected columns {unexpected}"
        )

    wrong_types = {
        column: actual_types[column]
        for column, expected in EXPECTED_SILVER_TYPES.items()
        if actual_types[column] != expected
    }
    if wrong_types:
        raise ValueError(
            f"Silver schema validation failed for {silver_path}: "
            f"unexpected column types {wrong_types}"
        )


def _parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    parsed_uri = urlparse(s3_uri)
    if parsed_uri.scheme != "s3" or not parsed_uri.netloc or not parsed_uri.path:
        raise ValueError(f"Expected an s3:// URI, received: {s3_uri}")
    return parsed_uri.netloc, parsed_uri.path.lstrip("/")


def resolve_silver_artifacts(
    date_str: str, hour_str: str, s3_client=None
) -> dict[str, str]:
    """Resolve active Silver artifacts, falling back to legacy paths."""
    date_str, hour_str = validate_partition(date_str, hour_str)
    s3_client = s3_client or _s3_client()
    partition = f"date={date_str}/hour={hour_str}"
    manifest_key = f"vehicle_positions/{partition}/_active.json"

    try:
        response = s3_client.get_object(
            Bucket="silver",
            Key=manifest_key,
        )
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if error_code in ("NoSuchKey", "404", "NotFound") or status == 404:
            return {
                "data": f"s3://silver/vehicle_positions/{partition}/data.parquet",
                "quarantine": (
                    f"s3://silver/quarantine/vehicle_positions/{partition}/data.parquet"
                ),
                "completeness": (
                    f"s3://silver/vehicle_positions/{partition}/_completeness.json"
                ),
            }
        raise

    try:
        manifest = json.loads(response["Body"].read().decode("utf-8"))
    except (KeyError, TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Active Silver manifest is not valid JSON") from exc

    if not isinstance(manifest, dict):
        raise TypeError("Active Silver manifest must be a JSON object")
    if manifest.get("status") != "published":
        raise ValueError("Active Silver manifest has invalid status")
    if manifest.get("partition") != partition:
        raise ValueError("Active Silver manifest references another partition")

    publication_id = manifest.get("publication_id")
    artifacts = manifest.get("artifacts")
    if (
        not isinstance(publication_id, str)
        or len(publication_id) != 32
        or any(char not in "0123456789abcdef" for char in publication_id)
        or not isinstance(artifacts, dict)
    ):
        raise ValueError("Active Silver manifest is missing publication metadata")

    expected_artifacts = {
        "data": (f"s3://silver/_versions/{partition}/{publication_id}/data.parquet"),
        "quarantine": (
            f"s3://silver/_versions/{partition}/{publication_id}/quarantine.parquet"
        ),
        "completeness": (
            f"s3://silver/_versions/{partition}/{publication_id}/_completeness.json"
        ),
    }

    resolved = {}
    for name, expected_uri in expected_artifacts.items():
        uri = artifacts.get(name)
        if uri != expected_uri:
            raise ValueError(f"Active Silver manifest has invalid {name} artifact URI")
        resolved[name] = uri

    return resolved


def write_silver_active_manifest(
    date_str: str,
    hour_str: str,
    publication_id: str,
    artifacts: dict[str, str],
    source_inputs: list[dict] | None = None,
) -> None:
    """Publish a prepared Silver version by replacing its active manifest."""
    date_str, hour_str = validate_partition(date_str, hour_str)
    partition = f"date={date_str}/hour={hour_str}"

    if (
        not isinstance(publication_id, str)
        or len(publication_id) != 32
        or any(char not in "0123456789abcdef" for char in publication_id)
    ):
        raise ValueError("Invalid Silver publication ID")

    expected_artifacts = {
        "data": (f"s3://silver/_versions/{partition}/{publication_id}/data.parquet"),
        "quarantine": (
            f"s3://silver/_versions/{partition}/{publication_id}/quarantine.parquet"
        ),
        "completeness": (
            f"s3://silver/_versions/{partition}/{publication_id}/_completeness.json"
        ),
    }

    for name, expected_uri in expected_artifacts.items():
        if artifacts.get(name) != expected_uri:
            raise ValueError(f"Invalid Silver {name} artifact URI")

    manifest = {
        "publication_id": publication_id,
        "partition": partition,
        "status": "published",
        "artifacts": artifacts,
    }
    if source_inputs is not None:
        validate_source_inputs(source_inputs, date_str, hour_str)
        manifest["source_inputs"] = source_inputs
    _s3_client().put_object(
        Bucket="silver",
        Key=f"vehicle_positions/{partition}/_active.json",
        Body=json.dumps(manifest, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_USER"),
        aws_secret_access_key=os.getenv("MINIO_PASSWORD"),
    )


def delete_s3_object(s3_uri: str) -> None:
    bucket, key = _parse_s3_uri(s3_uri)
    _s3_client().delete_object(Bucket=bucket, Key=key)


def s3_object_exists(s3_uri: str) -> bool:
    bucket, key = _parse_s3_uri(s3_uri)
    try:
        _s3_client().head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 404 or exc.response.get("Error", {}).get("Code") in {
            "404",
            "NoSuchKey",
            "NotFound",
        }:
            return False
        raise
    return True


def copy_s3_object(source_uri: str, destination_uri: str) -> None:
    """Server-side copy: the destination is replaced in a single operation."""
    source_bucket, source_key = _parse_s3_uri(source_uri)
    destination_bucket, destination_key = _parse_s3_uri(destination_uri)
    _s3_client().copy_object(
        Bucket=destination_bucket,
        Key=destination_key,
        CopySource={"Bucket": source_bucket, "Key": source_key},
    )


def sql_from_materialized_input(template: str) -> str:
    """Use the captured input table while preserving transformation rules."""
    if not template.startswith(BRONZE_SOURCE_CTE):
        raise ValueError("SQL template must start with the Bronze source CTE")

    source_cte = "WITH source AS (SELECT * FROM bronze_input),\n"
    return source_cte + template[len(BRONZE_SOURCE_CTE) :]


def materialize_bronze_inputs(con, files: list[str]) -> None:
    """Read selected files once using the explicit Bronze input schema."""
    if not files:
        raise BronzeDataMissingError("No Bronze files selected for processing")

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE bronze_input AS
        SELECT *
        FROM read_json_auto(
            ?,
            columns = ?,
            format = 'newline_delimited',
            hive_partitioning = true
        )
        """,
        [files, BRONZE_JSON_COLUMNS],
    )


def validate_source_inputs(
    source_inputs: list[dict],
    date_str: str,
    hour_str: str,
) -> None:
    date_str, hour_str = validate_partition(date_str, hour_str)
    partition = f"date={date_str}/hour={hour_str}"

    if not isinstance(source_inputs, list):
        raise TypeError("Silver source_inputs must be a list")

    allowed_inputs = (
        (f"vehicle_type=bus/{partition}/", ".jsonl"),
        (f"vehicle_type=tram/{partition}/", ".jsonl"),
        (f"_manifests/{partition}/", ".json"),
    )
    seen_keys: set[str] = set()

    for item in source_inputs:
        if not isinstance(item, dict):
            raise TypeError("Silver source_inputs items must be JSON objects")

        input_key = item.get("key")
        if not isinstance(input_key, str) or not any(
            input_key.startswith(prefix)
            and input_key.endswith(suffix)
            and len(input_key) > len(prefix) + len(suffix)
            for prefix, suffix in allowed_inputs
        ):
            raise ValueError("Silver source_inputs has an invalid key")

        if input_key in seen_keys:
            raise ValueError("Silver source_inputs has a duplicate key")
        seen_keys.add(input_key)

        size = item.get("size")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ValueError("Silver source_inputs has an invalid size")

        etag = item.get("etag")
        if not isinstance(etag, str) or not etag.strip():
            raise ValueError("Silver source_inputs has an invalid etag")

        try:
            modified_at = datetime.fromisoformat(item.get("last_modified"))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Silver source_inputs has an invalid last_modified"
            ) from exc
        if modified_at.tzinfo is None:
            raise ValueError(
                "Silver source_inputs last_modified must be timezone-aware"
            )


def read_silver_source_inputs(
    date_str: str, hour_str: str, s3_client=None
) -> list[dict] | None:
    """Read recorded inputs; None means a legacy publication."""
    date_str, hour_str = validate_partition(date_str, hour_str)
    s3_client = s3_client or _s3_client()
    partition = f"date={date_str}/hour={hour_str}"
    key = f"vehicle_positions/{partition}/_active.json"

    try:
        response = s3_client.get_object(Bucket="silver", Key=key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if error_code in ("NoSuchKey", "404", "NotFound") or status == 404:
            return None
        raise

    manifest = json.loads(response["Body"].read())
    if not isinstance(manifest, dict):
        raise TypeError("Active Silver manifest must be a JSON object")
    if manifest.get("partition") != partition:
        raise ValueError("Active Silver manifest references another partition")
    if "source_inputs" not in manifest:
        return None

    source_inputs = manifest["source_inputs"]
    validate_source_inputs(source_inputs, date_str, hour_str)
    return source_inputs


def capture_bronze_inputs(date_str: str, hour_str: str, s3_client=None) -> list[dict]:
    """List data and manifest objects identifying one hour's input."""
    date_str, hour_str = validate_partition(date_str, hour_str)
    s3_client = s3_client or _s3_client()
    partition = f"date={date_str}/hour={hour_str}/"
    prefixes = (
        (f"vehicle_type=bus/{partition}", ".jsonl"),
        (f"vehicle_type=tram/{partition}", ".jsonl"),
        (f"_manifests/{partition}", ".json"),
    )
    inputs = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for prefix, suffix in prefixes:
        for page in paginator.paginate(Bucket="bronze", Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(suffix):
                    continue

                inputs.append(
                    {
                        "key": key,
                        "etag": obj.get("ETag"),
                        "size": obj.get("Size"),
                        "last_modified": obj["LastModified"]
                        .astimezone(timezone.utc)
                        .isoformat(),
                    }
                )

    return sorted(inputs, key=lambda obj: obj["key"])


def is_before_first_bronze_hour(date_str: str, hour_str: str, s3_client=None) -> bool:
    """True when no Bronze data exists at or before this hour (pre-ingestion period)."""
    date_str, hour_str = validate_partition(date_str, hour_str)
    s3_client = s3_client or _s3_client()
    earliest = None
    for label in ("bus", "tram"):
        # Keys sort by date then hour, so the first key is the earliest hour.
        response = s3_client.list_objects_v2(
            Bucket="bronze", Prefix=f"vehicle_type={label}/", MaxKeys=1
        )
        for obj in response.get("Contents", []):
            parts = dict(p.split("=", 1) for p in obj["Key"].split("/")[1:3])
            candidate = (parts["date"], parts["hour"])
            earliest = candidate if earliest is None else min(earliest, candidate)
    return earliest is None or (date_str, hour_str) < earliest


MIN_COMPLETE_CYCLE_RATIO = 0.9
MIN_CYCLE_COVERAGE_RATIO = 0.95


def completeness_thresholds(
    poll_interval_seconds: int | None = None,
) -> tuple[int, int]:
    """Return expected and minimum required ingestion cycles for one hour."""
    if poll_interval_seconds is None:
        poll_interval_seconds = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))

    if poll_interval_seconds <= 0:
        raise ValueError("POLL_INTERVAL_SECONDS must be a positive integer")

    expected_cycles = math.ceil(3600 / poll_interval_seconds)
    minimum_required_cycles = math.ceil(expected_cycles * MIN_CYCLE_COVERAGE_RATIO)
    return expected_cycles, minimum_required_cycles


def expected_slots_elapsed(
    date_str: str,
    hour_str: str,
    now_utc: datetime,
    poll_interval_seconds: int,
) -> int:
    if poll_interval_seconds <= 0:
        raise ValueError("POLL_INTERVAL_SECONDS must be a positive integer")

    hour_start = datetime.fromisoformat(f"{date_str}T{hour_str}:00:00+00:00")
    hour_end = hour_start + timedelta(hours=1)
    now_utc = now_utc.astimezone(timezone.utc)

    if now_utc < hour_start:
        return 0

    start_epoch = int(hour_start.timestamp())
    end_epoch = int(hour_end.timestamp())

    first_slot = (start_epoch + poll_interval_seconds - 1) // poll_interval_seconds
    last_second = min(math.floor(now_utc.timestamp()), end_epoch - 1)
    last_slot = last_second // poll_interval_seconds

    return max(0, last_slot - first_slot + 1)


def validate_cycle_manifest(manifest: dict) -> None:
    if "schema_version" in manifest:
        version = manifest["schema_version"]
        if isinstance(version, bool) or not isinstance(version, int) or version != 1:
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


def assess_completeness(
    date_str: str,
    hour_str: str,
    s3_client=None,
    poll_interval_seconds: int | None = None,
    now_utc: datetime | None = None,
    source_inputs: list[dict] | None = None,
) -> dict:
    """Summarize ingestion cycle manifests for an hour (bus + tram both fetched)."""
    s3_client = s3_client or _s3_client()
    prefix = f"_manifests/date={date_str}/hour={hour_str}/"
    if source_inputs is None:
        keys = []
        for page in s3_client.get_paginator("list_objects_v2").paginate(
            Bucket="bronze", Prefix=prefix
        ):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
    else:
        keys = [
            obj["key"]
            for obj in source_inputs
            if obj["key"].startswith(prefix) and obj["key"].endswith(".json")
        ]

    complete = 0
    slot_statuses: dict[str, list[bool]] = {}
    has_slot_metadata = False
    for key in keys:
        body = s3_client.get_object(Bucket="bronze", Key=key)["Body"].read()
        manifest = json.loads(body)
        if not isinstance(manifest, dict):
            raise TypeError("Cycle manifest must be a JSON object")
        validate_cycle_manifest(manifest)

        if manifest.get("status") == "complete":
            complete += 1

        if "cycle_slot_start" in manifest:
            has_slot_metadata = True

        slot_start = manifest.get("cycle_slot_start")
        if isinstance(slot_start, str) and slot_start:
            slot_statuses.setdefault(slot_start, []).append(
                manifest.get("status") == "complete"
            )

    total = len(keys)
    if poll_interval_seconds is None:
        poll_interval_seconds = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))

    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    elif now_utc.tzinfo is None:
        raise ValueError("now_utc must be timezone-aware")

    expected_cycles = expected_slots_elapsed(
        date_str,
        hour_str,
        now_utc,
        poll_interval_seconds,
    )

    minimum_required_cycles = math.ceil(expected_cycles * MIN_CYCLE_COVERAGE_RATIO)

    if has_slot_metadata:
        expected_prefix = f"{date_str}T{hour_str}:"
        valid_slots: dict[datetime, bool] = {}

        for slot_start, statuses in slot_statuses.items():
            try:
                parsed_slot = datetime.fromisoformat(slot_start)
            except ValueError:
                continue
            if parsed_slot.tzinfo is None:
                continue

            parsed_slot = parsed_slot.astimezone(timezone.utc)
            if parsed_slot > now_utc:
                continue
            if (
                parsed_slot.microsecond != 0
                or int(parsed_slot.timestamp()) % poll_interval_seconds != 0
            ):
                continue

            if parsed_slot.strftime("%Y-%m-%dT%H:") == expected_prefix:
                valid_slots[parsed_slot] = valid_slots.get(parsed_slot, False) or any(
                    statuses
                )

        assessed_cycles = len(valid_slots)
        assessed_complete = sum(valid_slots.values())
    else:
        assessed_cycles = total
        assessed_complete = complete

    missing_cycles = max(0, expected_cycles - assessed_cycles)

    if total == 0:
        status = "unknown"
    elif (
        expected_cycles - missing_cycles < minimum_required_cycles
        or assessed_cycles == 0
        or assessed_complete / assessed_cycles < MIN_COMPLETE_CYCLE_RATIO
    ):
        status = "incomplete"
    else:
        status = "complete"
    return {
        "date": date_str,
        "hour": hour_str,
        "status": status,
        "cycles": total,
        "complete_cycles": complete,
        "partial_cycles": total - complete,
        "observed_slots": assessed_cycles if has_slot_metadata else None,
        "complete_slots": assessed_complete if has_slot_metadata else None,
        "slot_coverage_ratio": (
            assessed_cycles / expected_cycles
            if has_slot_metadata and expected_cycles > 0
            else None
        ),
        "complete_slot_ratio": (
            assessed_complete / assessed_cycles
            if has_slot_metadata and assessed_cycles > 0
            else None
        ),
        "missing_cycles": missing_cycles,
        "min_complete_ratio": MIN_COMPLETE_CYCLE_RATIO,
        "expected_cycles": expected_cycles,
        "minimum_required_cycles": minimum_required_cycles,
    }


def write_completeness_marker(completeness: dict, silver_dir: str) -> None:
    bucket, key = _parse_s3_uri(f"{silver_dir}/_completeness.json")
    _s3_client().put_object(
        Bucket=bucket, Key=key, Body=json.dumps(completeness).encode("utf-8")
    )


def read_completeness_marker(date_str: str, hour_str: str, s3_client=None) -> dict:
    """Read the active completeness marker, with legacy-path fallback."""
    date_str, hour_str = validate_partition(date_str, hour_str)
    s3_client = s3_client or _s3_client()

    artifacts = resolve_silver_artifacts(date_str, hour_str, s3_client)
    bucket, key = _parse_s3_uri(artifacts["completeness"])

    try:
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if error_code in ("NoSuchKey", "404", "NotFound") or status == 404:
            return {"status": "unknown"}
        raise

    marker = json.loads(body)
    if not isinstance(marker, dict):
        raise TypeError("Completeness marker must be a JSON object")
    return marker


def is_hour_complete(date_str: str, hour_str: str, s3_client=None) -> bool:
    """Gold/dashboard consumers should only aggregate hours where this is True."""
    marker = read_completeness_marker(date_str, hour_str, s3_client)
    return marker.get("status") == "complete"


def validate_output_quality(
    date_str: str, hour_str: str, bronze_count: int, silver_count: int
):
    if bronze_count > 0 and silver_count == 0:
        raise ValueError(
            f"No valid Silver rows produced for {date_str} hour {hour_str} "
            f"despite {bronze_count} bronze records. Check transformation filters."
        )


def transform_hour(con, date_str, hour_str, overwrite=True):
    date_str, hour_str = validate_partition(date_str, hour_str)
    partition = f"date={date_str}/hour={hour_str}"
    bronze_path = f"s3://bronze/vehicle_type=*/date={date_str}/hour={hour_str}/*.jsonl"

    publication_id = uuid.uuid4().hex
    version_dir = f"s3://silver/_versions/{partition}/{publication_id}"
    staging_dir = f"s3://silver/_staging/{partition}/{publication_id}"

    temp_file = f"{staging_dir}/data.parquet.pending"
    rejected_temp_file = f"{staging_dir}/quarantine.parquet.pending"

    output_file = f"{version_dir}/data.parquet"
    rejected_file = f"{version_dir}/quarantine.parquet"

    artifacts = {
        "data": output_file,
        "quarantine": rejected_file,
        "completeness": f"{version_dir}/_completeness.json",
    }

    # glob() does not accept bound parameters in DuckDB 1.2.x.
    # The partition values are validated before building this path.
    bronze_file = con.execute(
        f"SELECT file FROM glob('{bronze_path}') LIMIT 1"
    ).fetchone()
    if bronze_file is None:
        raise BronzeDataMissingError(
            f"No bronze files found for {date_str} hour {hour_str}; "
            "refusing to report success for an empty partition."
        )

    source_inputs = capture_bronze_inputs(date_str, hour_str)
    data_files = [
        f"s3://bronze/{obj['key']}"
        for obj in source_inputs
        if obj["key"].endswith(".jsonl")
    ]
    materialize_bronze_inputs(con, data_files)
    quality_sql = sql_from_materialized_input(QUALITY_SQL)
    (
        before,
        after_coordinates,
        after_vehicle,
        after_time,
        future_gps_time,
        stale_gps_time,
        stale_5_to_10,
        stale_10_to_30,
        stale_over_30,
        invalid_gps_time,
        after,
    ) = con.execute(quality_sql).fetchone()
    validate_output_quality(date_str, hour_str, before, after)

    sql = sql_from_materialized_input(TRANSFORM_SQL)
    rejected_sql = sql_from_materialized_input(REJECTED_SQL)
    rejected_count = before - after

    if not overwrite:
        active_artifacts = resolve_silver_artifacts(date_str, hour_str)
        if any(
            s3_object_exists(path)
            for path in (
                active_artifacts["data"],
                active_artifacts["quarantine"],
            )
        ):
            raise FileExistsError(
                f"Silver output already exists: {active_artifacts['data']}"
            )

    try:
        con.execute(
            f"COPY ({rejected_sql}) TO '{rejected_temp_file}' "
            "(FORMAT parquet, OVERWRITE_OR_IGNORE true)"
        )
        rejected_output_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{rejected_temp_file}')"
        ).fetchone()[0]
        if rejected_output_count != rejected_count:
            raise ValueError(
                f"Rejected-record validation failed for {date_str} hour "
                f"{hour_str}: temporary output has {rejected_output_count} "
                f"rows, expected {rejected_count}; refusing to publish."
            )

        con.execute(
            f"COPY ({sql}) TO '{temp_file}' (FORMAT parquet, OVERWRITE_OR_IGNORE true)"
        )
        validate_silver_schema(con, temp_file)

        row_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{temp_file}')"
        ).fetchone()[0]
        if row_count != after:
            raise ValueError(
                f"Silver validation failed for {date_str} hour {hour_str}: "
                f"temporary output has {row_count} rows, expected {after}; "
                "refusing to publish."
            )

        completeness = assess_completeness(
            date_str, hour_str, source_inputs=source_inputs
        )

        # Prepare the complete new version before switching the active pointer.
        copy_s3_object(rejected_temp_file, rejected_file)
        copy_s3_object(temp_file, output_file)
        write_completeness_marker(completeness, version_dir)

        # This is the publication point: readers now resolve the new version.
        write_silver_active_manifest(
            date_str,
            hour_str,
            publication_id,
            artifacts,
            source_inputs=source_inputs,
        )

        if completeness["status"] != "complete":
            logger.warning(
                "%s hour %s Silver completeness is %s: %s/%s ingestion "
                "cycles had both bus and tram data; business metrics may "
                "be understated.",
                date_str,
                hour_str,
                completeness["status"],
                completeness["complete_cycles"],
                completeness["cycles"],
            )
    finally:
        for temp_path in (temp_file, rejected_temp_file):
            try:
                delete_s3_object(temp_path)
            except (BotoCoreError, ClientError) as exc:
                logger.warning(
                    "Could not remove temporary Silver object %s: %s",
                    temp_path,
                    exc,
                )

    rejected_coordinates = before - after_coordinates
    rejected_vehicle = after_coordinates - after_vehicle
    rejected_gps_time = after_vehicle - after_time
    duplicates = after_time - after
    dropped_pct = (before - after) / before if before else 0

    logger.info(
        f"{date_str} hour {hour_str}: "
        f"bronze={before}, "
        f"rejected_coordinates={rejected_coordinates}, "
        f"rejected_vehicle_number={rejected_vehicle}, "
        f"rejected_gps_time={rejected_gps_time} "
        f"(future={future_gps_time}, stale_total={stale_gps_time}, "
        f"stale_5_to_10={stale_5_to_10}, "
        f"stale_10_to_30={stale_10_to_30}, "
        f"stale_over_30={stale_over_30}, "
        f"invalid_or_ambiguous={invalid_gps_time}), "
        f"duplicates={duplicates}, "
        f"quarantined={rejected_count}, "
        f"silver={after}, "
        f"dropped={dropped_pct:.1%} -> {output_file}"
    )


def main():
    # default: previous full hour in UTC
    previous_hour = datetime.now(timezone.utc) - timedelta(hours=1)

    parser = argparse.ArgumentParser(description="Bronze -> silver for one hour")
    parser.add_argument("--date", default=previous_hour.strftime("%Y-%m-%d"))
    parser.add_argument("--hour", default=previous_hour.strftime("%H"))
    args = parser.parse_args()

    con = get_connection()
    try:
        transform_hour(con, args.date, args.hour)
    finally:
        con.close()


if __name__ == "__main__":
    main()
