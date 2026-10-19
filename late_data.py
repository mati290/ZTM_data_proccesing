"""Repair recent closed hours and finalize Gold publications.

Within the correction window (default 24h), compare current Bronze data
and manifest metadata with the inputs recorded by Silver. Legacy Silver
publications use modification times as a fallback.

Rebuild Silver only when required. Repair missing or outdated Gold and
finalize eligible Gold publications without rebuilding fresh Silver.
Project DAGs serialize writes through a shared Airflow pool.
"""

import argparse
import json
import logging
from datetime import datetime, timedelta, timezone

import bronze_to_silver
import silver_to_gold
from bronze_to_silver import (
    ClientError,
    _parse_s3_uri,
    resolve_silver_artifacts,
    validate_partition,
)

logger = logging.getLogger(__name__)

CORRECTION_WINDOW_HOURS = 24
VEHICLE_TYPES = ("bus", "tram")


def _max_last_modified(s3_client, bucket, prefix):
    latest = None
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if latest is None or obj["LastModified"] > latest:
                latest = obj["LastModified"]
    return latest


def is_hour_stale(date_str, hour_str, s3_client) -> bool:
    date_str, hour_str = validate_partition(date_str, hour_str)

    bronze_latest = None
    for label in VEHICLE_TYPES:
        modified = _max_last_modified(
            s3_client,
            "bronze",
            f"vehicle_type={label}/date={date_str}/hour={hour_str}/",
        )
        if modified is not None and (bronze_latest is None or modified > bronze_latest):
            bronze_latest = modified

    if bronze_latest is None:
        return False

    try:
        artifacts = resolve_silver_artifacts(date_str, hour_str, s3_client=s3_client)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if error_code in ("NoSuchKey", "404", "NotFound") or status == 404:
            return True
        raise

    required_artifacts = (
        artifacts["data"],
        artifacts["quarantine"],
        artifacts["completeness"],
    )

    for uri in required_artifacts:
        bucket, key = _parse_s3_uri(uri)
        if bucket != "silver":
            raise ValueError(f"Unexpected Silver artifact bucket: {bucket}")

        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=key,
        )
        if not any(obj.get("Key") == key for obj in response.get("Contents", [])):
            return True
    recorded_inputs = bronze_to_silver.read_silver_source_inputs(
        date_str, hour_str, s3_client
    )
    if recorded_inputs is not None:
        current_inputs = bronze_to_silver.capture_bronze_inputs(
            date_str, hour_str, s3_client
        )
        return current_inputs != recorded_inputs
    data_bucket, data_key = _parse_s3_uri(artifacts["data"])
    silver_latest = _max_last_modified(
        s3_client,
        data_bucket,
        data_key,
    )

    return silver_latest is None or bronze_latest > silver_latest


def is_gold_stale(
    date_str: str, hour_str: str, s3_client, now: datetime | None = None
) -> bool:
    """Check whether Gold references the currently active Silver data."""
    date_str, hour_str = validate_partition(date_str, hour_str)
    artifacts = resolve_silver_artifacts(date_str, hour_str, s3_client=s3_client)
    manifest_uri = silver_to_gold.publication_manifest_uri(date_str, hour_str)
    bucket, key = _parse_s3_uri(manifest_uri)

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if error_code in ("NoSuchKey", "404", "NotFound") or status == 404:
            return True
        raise

    manifest = json.loads(response["Body"].read())
    if not isinstance(manifest, dict):
        raise TypeError("Gold publication manifest must be a JSON object")

    expected_partition = f"date={date_str}/hour={hour_str}"
    if manifest.get("partition") != expected_partition:
        raise ValueError("Gold publication manifest references another partition")

    if manifest.get("source_silver_uri") != artifacts["data"]:
        return True
    legacy_key = f"vehicle_positions/{expected_partition}/data.parquet"
    if artifacts["data"] == f"s3://silver/{legacy_key}":
        recorded_etag = manifest.get("source_silver_etag")
        if not isinstance(recorded_etag, str) or not recorded_etag:
            return True

        try:
            source_metadata = s3_client.head_object(
                Bucket="silver",
                Key=legacy_key,
            )
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if error_code in ("NoSuchKey", "404", "NotFound") or status == 404:
                return True
            raise

        current_etag = source_metadata.get("ETag")
        if not isinstance(current_etag, str) or not current_etag:
            raise ValueError("Legacy Silver object is missing a valid ETag")
        if recorded_etag != current_etag:
            return True
    tables = manifest.get("tables")
    if not isinstance(tables, dict):
        return True
    if set(tables) != set(silver_to_gold.GOLD_TABLES):
        return True
    publication_id = manifest.get("publication_id")
    if (
        not isinstance(publication_id, str)
        or len(publication_id) != 32
        or any(char not in "0123456789abcdef" for char in publication_id)
    ):
        return True

    for table in silver_to_gold.GOLD_TABLES:
        metadata = tables[table]
        expected_key = (
            f"_versions/{expected_partition}/{publication_id}/{table}/data.parquet"
        )
        if (
            not isinstance(metadata, dict)
            or metadata.get("uri") != f"s3://gold/{expected_key}"
        ):
            return True

        try:
            s3_client.head_object(Bucket="gold", Key=expected_key)
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if error_code in ("NoSuchKey", "404", "NotFound") or status == 404:
                return True
            raise
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    return manifest.get("is_final") is not True and silver_to_gold.is_hour_final(
        date_str, hour_str, now
    )


def find_stale_hours(
    now=None, window_hours=CORRECTION_WINDOW_HOURS, s3_client=None
) -> list[tuple[str, str]]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    s3_client = s3_client or bronze_to_silver._s3_client()
    current_hour = now.replace(minute=0, second=0, microsecond=0)

    stale = []
    for offset in range(window_hours, 0, -1):
        hour_start = current_hour - timedelta(hours=offset)
        date_str, hour_str = hour_start.strftime("%Y-%m-%d"), hour_start.strftime("%H")
        if is_hour_stale(date_str, hour_str, s3_client):
            stale.append((date_str, hour_str))
            continue

        artifacts = resolve_silver_artifacts(date_str, hour_str, s3_client=s3_client)
        bucket, key = _parse_s3_uri(artifacts["data"])
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
        silver_exists = any(
            obj.get("Key") == key for obj in response.get("Contents", [])
        )

        if silver_exists and is_gold_stale(date_str, hour_str, s3_client, now=now):
            stale.append((date_str, hour_str))
    return stale


def recompute_hour(con, date_str, hour_str, run_at=None, s3_client=None):
    s3_client = s3_client or bronze_to_silver._s3_client()
    if is_hour_stale(date_str, hour_str, s3_client):
        bronze_to_silver.transform_hour(con, date_str, hour_str, overwrite=True)
    return silver_to_gold.build_gold_hour(con, date_str, hour_str, run_at)


def reprocess_late_data(
    now=None, window_hours=CORRECTION_WINDOW_HOURS, s3_client=None, con=None
) -> dict:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(timezone.utc)
    stale = find_stale_hours(now, window_hours, s3_client)
    logger.info("Hours to recompute in the last %sh: %s", window_hours, stale)
    if not stale:
        return {"recomputed": [], "failed": []}

    owns_connection = con is None
    con = con or silver_to_gold.get_connection()
    recomputed, failed = [], []
    try:
        for date_str, hour_str in stale:
            try:
                result = recompute_hour(
                    con, date_str, hour_str, run_at=now, s3_client=s3_client
                )
                recomputed.append(f"{date_str} {hour_str}")
                logger.info(
                    "Recomputed %s hour %s: status=%s",
                    date_str,
                    hour_str,
                    result["status"],
                )
            except Exception:
                logger.exception("Recompute failed for %s hour %s", date_str, hour_str)
                failed.append(f"{date_str} {hour_str}")
    finally:
        if owns_connection:
            con.close()

    if failed:
        raise RuntimeError(f"Late-data recompute failed for hours: {failed}")
    return {"recomputed": recomputed, "failed": failed}


def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Recompute stale recent hours")
    parser.add_argument("--window-hours", type=int, default=CORRECTION_WINDOW_HOURS)
    args = parser.parse_args()
    print(reprocess_late_data(window_hours=args.window_hours))


if __name__ == "__main__":
    main()
