import json
import logging
import math
import os
import signal
import time
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

from ingestion_contract import validate_cycle_manifest

logging.Formatter.converter = time.gmtime
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ZTMApiError(Exception):
    """Raised when the ZTM API returns an error message instead of data."""


url = "https://dane.um.warszawa.pl/api/action/get_ztm_lokalizacja_pojazdow"


def _safe_error_summary(error: Exception) -> str:
    if isinstance(error, requests.HTTPError):
        status = error.response.status_code if error.response is not None else "unknown"
        return f"HTTPError (status={status})"
    return type(error).__name__


def retry_after_seconds(value: str | None) -> float:
    """Parse the server cooldown; malformed headers use a conservative default."""
    try:
        delay = float(value)
        if math.isfinite(delay) and delay >= 0:
            return delay
    except (TypeError, ValueError):
        pass
    try:
        deadline = parsedate_to_datetime(value)
        if deadline.tzinfo is not None:
            return max(0.0, (deadline - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError, OverflowError):
        pass
    return 60.0


def failure_cooldown_seconds(failed_cycles: int) -> int:
    """Reduce API traffic during outages without changing the UTC slot contract."""
    if failed_cycles <= 0:
        return 0
    return min(300, 60 * 2 ** min(failed_cycles - 1, 3))


def fetch_positions(
    vehicle_type: int, api_key: str, max_attempts: int = 3
) -> list[dict]:
    if max_attempts <= 0:
        raise ValueError("max_attempts must be positive")
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(
                url,
                headers={"Authorization": api_key},
                json={"type": vehicle_type},
                timeout=10,
                allow_redirects=False,
            )
            response.raise_for_status()
            if 300 <= response.status_code < 400:
                raise ZTMApiError("API redirect refused to protect credentials")
            data = response.json()
            if not isinstance(data, dict) or "result" not in data:
                raise ZTMApiError("API response is missing the result field")
            result = data["result"]
            if not isinstance(result, list) or any(
                not isinstance(record, dict) for record in result
            ):
                raise ZTMApiError("API result is not a list of position records")
            return result
        except ZTMApiError as error:
            logger.error(
                "Invalid API response for vehicle type %s: %s",
                vehicle_type,
                _safe_error_summary(error),
            )
            raise
        except requests.RequestException as e:
            response = e.response
            status = response.status_code if response is not None else None
            if status is not None and 400 <= status < 500 and status not in {408, 429}:
                logger.error("Non-retryable API error: %s", _safe_error_summary(e))
                raise
            if status == 429:
                # Honor the server delay even on the last attempt, before the next source.
                delay = retry_after_seconds(response.headers.get("Retry-After"))
                logger.warning("API rate limit; waiting %.1f seconds", delay)
                time.sleep(delay)
            if attempt == max_attempts:
                logger.error(
                    f"All {max_attempts} attempts failed for vehicle type {vehicle_type}: {_safe_error_summary(e)}"
                )
                raise
            logger.warning(
                f"Attempt {attempt}/{max_attempts} failed for vehicle type {vehicle_type}: {_safe_error_summary(e)}"
            )
            if status != 429:
                time.sleep(2**attempt)
    raise RuntimeError(f"Failed to fetch positions for vehicle type {vehicle_type}")


def save_to_bronze(
    s3, records, vehicle_label, cycle_slot_start: datetime | None = None
) -> dict | None:
    if not records:
        logger.warning(f"No records fetched for vehicle type {vehicle_label}")
        return

    ingested_at = datetime.now(timezone.utc)
    partition_time = cycle_slot_start or ingested_at
    if partition_time.tzinfo is None or partition_time.utcoffset() is None:
        raise ValueError("cycle_slot_start must be timezone-aware")
    partition_time = partition_time.astimezone(timezone.utc)

    file_name = (
        f"positions_{ingested_at.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex}.jsonl"
    )

    key = (
        f"vehicle_type={vehicle_label}/"
        f"date={partition_time.strftime('%Y-%m-%d')}/"
        f"hour={partition_time.strftime('%H')}/"
        f"{file_name}"
    )

    lines = []
    for record in records:
        enriched_record = {**record, "_ingested_at": ingested_at.isoformat()}
        lines.append(json.dumps(enriched_record, ensure_ascii=False))

    body = "\n".join(lines)

    encoded = body.encode("utf-8")
    response = s3.put_object(Bucket="bronze", Key=key, Body=encoded)

    logger.info(f"Zapisano {len(records)} rekordów do s3://bronze/{key}")
    metadata = {"key": key, "size": len(encoded)}
    if isinstance(response.get("ETag"), str) and response["ETag"]:
        metadata["etag"] = response["ETag"]
    return metadata


def ensure_bucket(s3, buckets=("bronze", "silver", "gold")):
    for bucket in buckets:
        _ensure_single_bucket(s3, bucket)


def ensure_bucket_with_retry(s3, max_attempts=10, initial_delay_seconds=2):
    for attempt in range(1, max_attempts + 1):
        try:
            ensure_bucket(s3)
            return
        except (BotoCoreError, ClientError) as error:
            if isinstance(error, ClientError):
                status = error.response.get("ResponseMetadata", {}).get(
                    "HTTPStatusCode"
                )
                if status not in {429, 500, 502, 503, 504}:
                    raise
            if attempt == max_attempts:
                raise
            delay = initial_delay_seconds * attempt
            logger.warning(
                "Object storage is not ready (attempt %s/%s); retrying in %s seconds",
                attempt,
                max_attempts,
                delay,
            )
            time.sleep(delay)


def _ensure_single_bucket(s3, bucket):
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        error = exc.response.get("Error", {})
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status != 404 and error.get("Code") not in {
            "404",
            "NoSuchBucket",
            "NotFound",
        }:
            raise

        s3.create_bucket(Bucket=bucket)


def write_cycle_manifest(
    s3,
    bus_count,
    tram_count,
    failed_sources,
    empty_sources,
    cycle_started_at,
    cycle_slot_start,
    source_objects: dict | None = None,
    poll_interval_seconds: int | None = None,
) -> None:
    """Record whether a cycle fetched both sources, so Silver can flag partial hours."""
    now = datetime.now(timezone.utc)
    complete = bus_count > 0 and tram_count > 0
    manifest = {
        "schema_version": 1,
        "ingested_at": now.isoformat(),
        "cycle_started_at": cycle_started_at.isoformat(),
        "cycle_slot_start": cycle_slot_start.isoformat(),
        "status": "complete" if complete else "partial",
        "buses": bus_count,
        "trams": tram_count,
        "failed_sources": failed_sources,
        "empty_sources": empty_sources,
    }
    if source_objects is not None:
        manifest.update(
            schema_version=2,
            source_objects=source_objects,
            poll_interval_seconds=poll_interval_seconds,
        )
    validate_cycle_manifest(manifest)
    partition_time = cycle_slot_start.astimezone(timezone.utc)
    key = (
        f"_manifests/date={partition_time.strftime('%Y-%m-%d')}/"
        f"hour={partition_time.strftime('%H')}/"
        f"cycle_{now.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex}.json"
    )
    try:
        s3.put_object(
            Bucket="bronze", Key=key, Body=json.dumps(manifest).encode("utf-8")
        )
    except (BotoCoreError, ClientError) as error:
        logger.error(
            "Could not write cycle manifest: %s",
            _safe_error_summary(error),
        )
        raise


def run_once(api_key, s3):
    cycle_started_at = datetime.now(timezone.utc)
    poll_interval_seconds = get_poll_interval_seconds()
    cycle_slot_start = get_slot_start(cycle_started_at, poll_interval_seconds)
    buses = []
    trams = []
    failed_sources = []
    empty_sources = []

    for vehicle_type, label in ((1, "bus"), (2, "tram")):
        try:
            records = fetch_positions(vehicle_type, api_key=api_key)
        except (requests.RequestException, ZTMApiError, RuntimeError) as error:
            logger.error(
                "Skipping %s positions for this cycle because the source failed: %s",
                label,
                _safe_error_summary(error),
            )
            records = []
            failed_sources.append(label)

        if label == "bus":
            buses = records
        else:
            trams = records

        if not records and label not in failed_sources:
            empty_sources.append(label)

    total = len(buses) + len(trams)

    if not buses:
        logger.warning("No bus positions returned by the ZTM API in this cycle")
    if not trams:
        logger.warning("No tram positions returned by the ZTM API in this cycle")

    logger.info(
        "Fetched %s buses and %s trams (%s total)", len(buses), len(trams), total
    )

    source_objects = {"bus": None, "tram": None}
    if buses:
        source_objects["bus"] = save_to_bronze(
            s3, buses, "bus", cycle_slot_start=cycle_slot_start
        )
    if trams:
        source_objects["tram"] = save_to_bronze(
            s3, trams, "tram", cycle_slot_start=cycle_slot_start
        )

    write_cycle_manifest(
        s3,
        len(buses),
        len(trams),
        failed_sources,
        empty_sources,
        cycle_started_at,
        cycle_slot_start,
        source_objects=source_objects,
        poll_interval_seconds=poll_interval_seconds,
    )

    return {
        "buses": len(buses),
        "trams": len(trams),
        "total": total,
        "failed_sources": failed_sources,
        "empty_sources": empty_sources,
    }


def _handle_sigterm(signum, frame):
    logger.info("Received SIGTERM, shutting down")
    raise SystemExit(0)


def get_poll_interval_seconds():
    interval = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
    if interval <= 0:
        raise ValueError("POLL_INTERVAL_SECONDS must be a positive integer")
    return interval


def seconds_until_next_slot(timestamp, interval_seconds):
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")

    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    timestamp = timestamp.astimezone(timezone.utc)
    epoch_seconds = timestamp.timestamp()
    next_slot = (int(epoch_seconds // interval_seconds) + 1) * interval_seconds
    return next_slot - epoch_seconds


def get_slot_start(timestamp, interval_seconds):
    """Return the UTC-aligned slot start for a timestamp."""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")

    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    timestamp = timestamp.astimezone(timezone.utc)
    epoch_seconds = timestamp.timestamp()
    slot_epoch = int(epoch_seconds // interval_seconds) * interval_seconds
    return datetime.fromtimestamp(slot_epoch, tz=timezone.utc)


def main():
    load_dotenv()
    api_key = os.getenv("ZTM_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set ZTM_API_KEY in your .env file.")

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_USER"),
        aws_secret_access_key=os.getenv("MINIO_PASSWORD"),
        config=Config(
            connect_timeout=5,
            read_timeout=15,
            retries={
                "mode": "standard",
                "max_attempts": 3,
            },
        ),
    )

    ensure_bucket_with_retry(s3)

    interval = get_poll_interval_seconds()
    if interval <= 0:
        raise ValueError("POLL_INTERVAL_SECONDS must be a positive integer")
    alert_threshold = int(os.getenv("DEGRADED_CYCLES_ALERT_THRESHOLD", "10"))
    if alert_threshold <= 0:
        raise ValueError("DEGRADED_CYCLES_ALERT_THRESHOLD must be a positive integer")
    logger.info(f"Starting main loop with interval {interval} seconds")
    signal.signal(signal.SIGTERM, _handle_sigterm)
    degraded_cycles = 0
    failed_cycles = 0
    while True:
        degraded = True
        source_failed = True
        try:
            result = run_once(api_key, s3)
            source_failed = bool(result["failed_sources"])
            degraded = bool(result["failed_sources"] or result["empty_sources"])
            logger.info(
                "Ingestion cycle finished: status=%s buses=%s trams=%s total=%s",
                "degraded" if degraded else "complete",
                result["buses"],
                result["trams"],
                result["total"],
            )
        except (
            requests.RequestException,
            ZTMApiError,
            BotoCoreError,
            ClientError,
            RuntimeError,
        ) as error:
            logger.error(
                "Ingestion cycle failed (%s); retrying in next cycle",
                _safe_error_summary(error),
            )
        degraded_cycles = degraded_cycles + 1 if degraded else 0
        failed_cycles = failed_cycles + 1 if source_failed else 0
        if degraded_cycles == alert_threshold:
            logger.critical(
                "ALERT: ingestion degraded for %s consecutive cycles", degraded_cycles
            )
        sleep_seconds = seconds_until_next_slot(
            datetime.now(timezone.utc),
            interval,
        )
        cooldown = failure_cooldown_seconds(failed_cycles)
        if cooldown:
            logger.warning("API/source failures: waiting at least %s seconds", cooldown)
            sleep_seconds = max(sleep_seconds, cooldown)
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
