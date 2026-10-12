import argparse
import json
import logging
import os
import os
import time
import uuid
from contextlib import contextmanager
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


class SilverHourLockedError(RuntimeError):
    """Raised when another process is publishing this Silver hour."""


@contextmanager
def silver_hour_lock(date_str: str, hour_str: str, s3_client=None):
    """Fail-closed lock preventing overlapping Silver publication for one hour."""
    client = s3_client or _s3_client()
    key = f"_locks/vehicle_positions/date={date_str}/hour={hour_str}/writer.json"
    token = uuid.uuid4().hex
    try:
        client.put_object(
            Bucket="silver",
            Key=key,
            Body=json.dumps({"token": token}).encode("utf-8"),
            IfNoneMatch="*",
        )
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = exc.response.get("Error", {}).get("Code")
        if status in (409, 412) or code in ("PreconditionFailed", "ConditionalRequestConflict"):
            raise SilverHourLockedError(
                f"Silver hour {date_str} {hour_str} is already being published; "
                "retry after the active writer finishes."
            ) from exc
        raise

    try:
        yield
    finally:
        response = client.get_object(Bucket="silver", Key=key)
        current = json.loads(response["Body"].read())
        if current.get("token") != token:
            raise RuntimeError(
                f"Silver lock ownership changed for {date_str} hour {hour_str}; "
                "refusing to remove another writer's lock."
            )
        client.delete_object(Bucket="silver", Key=key, IfMatch=response["ETag"])


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


BASE_CTES = """
WITH source AS (
    SELECT *
    FROM read_json_auto('{bronze_path}', hive_partitioning = true)
),
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


def assess_completeness(date_str: str, hour_str: str, s3_client=None) -> dict:
    """Summarize ingestion cycle manifests for an hour (bus + tram both fetched)."""
    s3_client = s3_client or _s3_client()
    prefix = f"_manifests/date={date_str}/hour={hour_str}/"
    keys = []
    for page in s3_client.get_paginator("list_objects_v2").paginate(
        Bucket="bronze", Prefix=prefix
    ):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))

    complete = 0
    for key in keys:
        body = s3_client.get_object(Bucket="bronze", Key=key)["Body"].read()
        if json.loads(body).get("status") == "complete":
            complete += 1

    total = len(keys)
    if total == 0:
        status = "unknown"
    elif complete / total >= MIN_COMPLETE_CYCLE_RATIO:
        status = "complete"
    else:
        status = "incomplete"
    return {
        "date": date_str,
        "hour": hour_str,
        "status": status,
        "cycles": total,
        "complete_cycles": complete,
        "partial_cycles": total - complete,
        "min_complete_ratio": MIN_COMPLETE_CYCLE_RATIO,
    }


def write_completeness_marker(completeness: dict, silver_dir: str) -> None:
    bucket, key = _parse_s3_uri(f"{silver_dir}/_completeness.json")
    _s3_client().put_object(
        Bucket=bucket, Key=key, Body=json.dumps(completeness).encode("utf-8")
    )


def read_completeness_marker(date_str: str, hour_str: str, s3_client=None) -> dict:
    """Return the hour's marker, or status 'unknown' when it is missing."""
    date_str, hour_str = validate_partition(date_str, hour_str)
    s3_client = s3_client or _s3_client()
    key = f"vehicle_positions/date={date_str}/hour={hour_str}/_completeness.json"
    try:
        body = s3_client.get_object(Bucket="silver", Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return {"status": "unknown"}
        raise
    return json.loads(body)


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
    bronze_path = f"s3://bronze/vehicle_type=*/date={date_str}/hour={hour_str}/*.jsonl"
    silver_dir = f"s3://silver/vehicle_positions/date={date_str}/hour={hour_str}"
    output_file = f"{silver_dir}/data.parquet"
    temp_file = f"{silver_dir}/_staging/{uuid.uuid4()}.parquet.pending"
    rejected_file = f"s3://silver/quarantine/vehicle_positions/date={date_str}/hour={hour_str}/data.parquet"
    rejected_temp_file = (
        f"{silver_dir}/_staging/{uuid.uuid4()}.rejected.parquet.pending"
    )

    # glob() does not accept bound parameters in duckdb 1.2.x; the path comes from a validated partition
    bronze_file = con.execute(
        f"SELECT file FROM glob('{bronze_path}') LIMIT 1"
    ).fetchone()
    if bronze_file is None:
        raise BronzeDataMissingError(
            f"No bronze files found for {date_str} hour {hour_str}; "
            "refusing to report success for an empty partition."
        )

    quality_sql = QUALITY_SQL.format(bronze_path=bronze_path)
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

    sql = TRANSFORM_SQL.format(bronze_path=bronze_path)
    rejected_sql = REJECTED_SQL.format(bronze_path=bronze_path)
    rejected_count = before - after
    if not overwrite and (
        s3_object_exists(output_file) or s3_object_exists(rejected_file)
    ):
        raise FileExistsError(f"Silver output already exists: {output_file}")
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
                f"Rejected-record validation failed for {date_str} hour {hour_str}: "
                f"temporary output has {rejected_output_count} rows, expected "
                f"{rejected_count}; refusing to publish."
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

        # Serialize the commit phase so concurrent regular/late-data runs cannot
        # interleave quarantine, Silver and completeness-marker writes.
        with silver_hour_lock(date_str, hour_str):
            # A marker certifies the current Silver + quarantine objects. Remove
            # the previous marker before replacing either object, then write a fresh
            # marker only after completeness assessment succeeds.
            delete_s3_object(f"{silver_dir}/_completeness.json")
            copy_s3_object(rejected_temp_file, rejected_file)
            copy_s3_object(temp_file, output_file)
            completeness = assess_completeness(date_str, hour_str)
            write_completeness_marker(completeness, silver_dir)
            if completeness["status"] != "complete":
                logger.warning(
                    "%s hour %s Silver completeness is %s: %s/%s ingestion cycles had "
                    "both bus and tram data; business metrics may be understated.",
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
                    "Could not remove temporary Silver object %s: %s", temp_path, exc
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
