import argparse
import logging
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


BASE_CTES = """
WITH source AS (
    SELECT *
    FROM read_json_auto('{bronze_path}', hive_partitioning = true)
),
typed AS (
    SELECT
        vehicle_type,
        VehicleNumber AS vehicle_number,
        Lines         AS line,
        Brigade       AS brigade,
        TRY_CAST(Lat AS DOUBLE) AS lat,
        TRY_CAST(Lon AS DOUBLE) AS lon,
        TRY_CAST(TRY_CAST("Time" AS TIMESTAMP) AT TIME ZONE 'Europe/Warsaw' AS TIMESTAMPTZ)
            AS gps_time_utc,
        TRY_CAST(_ingested_at AS TIMESTAMPTZ) AS ingested_at
    FROM source
),
valid_coordinates AS (
    SELECT *
    FROM typed
    WHERE lat BETWEEN 51.9 AND 52.6
    AND lon BETWEEN 20.5 AND 21.6
),
valid_gps_time AS (
    SELECT *
    FROM valid_coordinates
    WHERE ingested_at - gps_time_utc >= INTERVAL 0 SECOND
    AND ingested_at - gps_time_utc <= INTERVAL 5 MINUTE
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
    FROM (
        SELECT
            vehicle_type,
            vehicle_number,
            line,
            brigade,
            lat,
            lon,
            gps_time_utc,
            ingested_at,
            ROW_NUMBER() OVER (
                PARTITION BY vehicle_type, vehicle_number, gps_time_utc
                ORDER BY ingested_at, lat, lon, line, brigade
            ) AS row_num
        FROM valid_gps_time
    )
    WHERE row_num = 1
)
"""

TRANSFORM_SQL = BASE_CTES + "SELECT * FROM deduplicated"

QUALITY_SQL = (
    BASE_CTES
    + """
SELECT
    (SELECT COUNT(*) FROM source) AS bronze_count,
    (SELECT COUNT(*) FROM valid_coordinates) AS after_coordinates_count,
    (SELECT COUNT(*) FROM valid_gps_time) AS after_time_count,
    (
        SELECT COUNT(*)
        FROM valid_coordinates
        WHERE gps_time_utc > ingested_at
    ) AS future_gps_time_count,
    (
        SELECT COUNT(*)
        FROM valid_coordinates
        WHERE ingested_at - gps_time_utc > INTERVAL 5 MINUTE
    ) AS stale_gps_time_count,
    (
        SELECT COUNT(*)
        FROM valid_coordinates
        WHERE ingested_at - gps_time_utc > INTERVAL 5 MINUTE
        AND ingested_at - gps_time_utc <= INTERVAL 10 MINUTE
    ) AS stale_5_to_10_minutes_count,
    (
        SELECT COUNT(*)
        FROM valid_coordinates
        WHERE ingested_at - gps_time_utc > INTERVAL 10 MINUTE
        AND ingested_at - gps_time_utc <= INTERVAL 30 MINUTE
    ) AS stale_10_to_30_minutes_count,
    (
        SELECT COUNT(*)
        FROM valid_coordinates
        WHERE ingested_at - gps_time_utc > INTERVAL 30 MINUTE
    ) AS stale_over_30_minutes_count,
    (
        SELECT COUNT(*)
        FROM valid_coordinates
        WHERE gps_time_utc IS NULL OR ingested_at IS NULL
    ) AS missing_gps_time_count,
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
    con.execute(
        "CREATE SECRET minio (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, "
        "URL_STYLE 'path', USE_SSL ?)",
        [user, password, endpoint, use_ssl],
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
            f"DESCRIBE SELECT * FROM read_parquet('{silver_path}')"
        ).fetchall()
    except Exception as exc:
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


def validate_output_quality(
    date_str: str, hour_str: str, bronze_count: int, silver_count: int
):
    if bronze_count > 0 and silver_count == 0:
        raise ValueError(
            f"No valid Silver rows produced for {date_str} hour {hour_str} "
            f"despite {bronze_count} bronze records. Check transformation filters."
        )


def check_bronze_freshness(
    con,
    date_str: str,
    hour_str: str,
    max_age_minutes: int = 90,
    now_utc: datetime | None = None,
):
    date_str, hour_str = validate_partition(date_str, hour_str)
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    partition_dt = datetime.strptime(
        f"{date_str} {hour_str}:00:00",
        "%Y-%m-%d %H:%M:%S",
    ).replace(tzinfo=timezone.utc)
    age_minutes = (now_utc - partition_dt).total_seconds() / 60

    if age_minutes < 0:
        raise ValueError(
            f"Bronze partition {date_str} hour {hour_str} is in the future; "
            "cannot validate freshness for a future hour."
        )

    if age_minutes > max_age_minutes:
        raise ValueError(
            f"Bronze partition {date_str} hour {hour_str} is stale: "
            f"age {age_minutes:.0f} minutes exceeds the {max_age_minutes}-minute threshold."
        )

    bronze_path = f"s3://bronze/vehicle_type=*/date={date_str}/hour={hour_str}/*.jsonl"
    try:
        files = con.execute(
            "SELECT file FROM glob(?) LIMIT 1", [bronze_path]
        ).fetchall()
    except duckdb.Error as exc:
        raise RuntimeError(
            f"Bronze file enumeration failed for {date_str} hour {hour_str} at {bronze_path}: {exc}"
        ) from exc

    if not files:
        raise BronzeDataMissingError(
            f"No bronze files found for {date_str} hour {hour_str}; "
            "data freshness check failed."
        )

    return {
        "date": date_str,
        "hour": hour_str,
        "age_minutes": round(age_minutes, 2),
        "file_count": len(files),
    }


def transform_hour(con, date_str, hour_str, overwrite=True):
    date_str, hour_str = validate_partition(date_str, hour_str)
    bronze_path = f"s3://bronze/vehicle_type=*/date={date_str}/hour={hour_str}/*.jsonl"
    silver_dir = f"s3://silver/vehicle_positions/date={date_str}/hour={hour_str}"
    output_file = f"{silver_dir}/data.parquet"
    temp_file = f"{silver_dir}/_staging/{uuid.uuid4()}.parquet.pending"

    bronze_file = con.execute(
        "SELECT file FROM glob(?) LIMIT 1",
        [bronze_path],
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
        after_time,
        future_gps_time,
        stale_gps_time,
        stale_5_to_10,
        stale_10_to_30,
        stale_over_30,
        missing_gps_time,
        after,
    ) = con.execute(quality_sql).fetchone()
    validate_output_quality(date_str, hour_str, before, after)

    sql = TRANSFORM_SQL.format(bronze_path=bronze_path)
    try:
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

        if not overwrite and s3_object_exists(output_file):
            raise FileExistsError(f"Silver output already exists: {output_file}")
        copy_s3_object(temp_file, output_file)
    finally:
        try:
            delete_s3_object(temp_file)
        except (BotoCoreError, ClientError) as exc:
            logger.warning(
                "Could not remove temporary Silver object %s: %s", temp_file, exc
            )

    rejected_coordinates = before - after_coordinates
    rejected_gps_time = after_coordinates - after_time
    duplicates = after_time - after
    dropped_pct = (before - after) / before if before else 0

    logger.info(
        f"{date_str} hour {hour_str}: "
        f"bronze={before}, "
        f"rejected_coordinates={rejected_coordinates}, "
        f"rejected_gps_time={rejected_gps_time} "
        f"(future={future_gps_time}, stale_total={stale_gps_time}, "
        f"stale_5_to_10={stale_5_to_10}, "
        f"stale_10_to_30={stale_10_to_30}, "
        f"stale_over_30={stale_over_30}, "
        f"missing={missing_gps_time}), "
        f"duplicates={duplicates}, "
        f"silver={after}, "
        f"dropped={dropped_pct:.1%} → {output_file}"
    )


def main():
    # domyślnie: poprzednia pełna godzina w UTC
    previous_hour = datetime.now(timezone.utc) - timedelta(hours=1)

    parser = argparse.ArgumentParser(description="Bronze → silver for one hour")
    parser.add_argument("--date", default=previous_hour.strftime("%Y-%m-%d"))
    parser.add_argument("--hour", default=previous_hour.strftime("%H"))
    args = parser.parse_args()

    con = get_connection()
    transform_hour(con, args.date, args.hour)


if __name__ == "__main__":
    main()
