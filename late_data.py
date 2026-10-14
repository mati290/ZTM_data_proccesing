"""Late-data correction: recomputes Silver and Gold for recent hours that changed.

Policy:
  * only closed hours inside the correction window (default 24h) are considered;
  * an hour is stale when Bronze was written after the Silver partition was published,
    or when Silver is missing although Bronze exists (this also repairs gaps);
  * hours older than the window are frozen;
  * the current (open) hour is never touched - the regular DAGs own it.
Every rewrite is an atomic overwrite of an idempotent partition.
"""

import argparse
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

    data_bucket, data_key = _parse_s3_uri(artifacts["data"])
    silver_latest = _max_last_modified(
        s3_client,
        data_bucket,
        data_key,
    )

    return silver_latest is None or bronze_latest > silver_latest


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
    return stale


def recompute_hour(con, date_str, hour_str, run_at=None):
    bronze_to_silver.transform_hour(con, date_str, hour_str, overwrite=True)
    return silver_to_gold.build_gold_hour(con, date_str, hour_str, run_at)


def reprocess_late_data(
    now=None, window_hours=CORRECTION_WINDOW_HOURS, s3_client=None, con=None
) -> dict:
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
                result = recompute_hour(con, date_str, hour_str)
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
