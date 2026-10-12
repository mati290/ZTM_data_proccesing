import json
import logging
import os
import signal
import time
import uuid
from datetime import datetime, timezone

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

logging.Formatter.converter = time.gmtime
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ZTMApiError(Exception):
    """Raised when the ZTM API returns an error message instead of data."""


url = "https://api.um.warszawa.pl/api/action/busestrams_get/"


def _safe_error_summary(error: Exception) -> str:
    if isinstance(error, requests.HTTPError):
        status = error.response.status_code if error.response is not None else "unknown"
        return f"HTTPError (status={status})"
    return type(error).__name__


def fetch_positions(vehicle_type, api_key, max_attempts=3):
    params = {
        "resource_id": "f2e5503e-927d-4ad3-9500-4ab9e55deb59",
        "apikey": api_key,
        "type": vehicle_type,
    }
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
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
            if attempt == max_attempts:
                logger.error(
                    f"All {max_attempts} attempts failed for vehicle type {vehicle_type}: {_safe_error_summary(e)}"
                )
                raise
            logger.warning(
                f"Attempt {attempt}/{max_attempts} failed for vehicle type {vehicle_type}: {_safe_error_summary(e)}"
            )
            time.sleep(2**attempt)
    raise RuntimeError(f"Failed to fetch positions for vehicle type {vehicle_type}")


def save_to_bronze(s3, records, vehicle_label):
    if not records:
        logger.warning(f"No records fetched for vehicle type {vehicle_label}")
        return

    ingested_at = datetime.now(timezone.utc)

    file_name = (
        f"positions_{ingested_at.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex}.jsonl"
    )

    key = (
        f"vehicle_type={vehicle_label}/"
        f"date={ingested_at.strftime('%Y-%m-%d')}/"
        f"hour={ingested_at.strftime('%H')}/"
        f"{file_name}"
    )

    lines = []
    for record in records:
        enriched_record = {**record, "_ingested_at": ingested_at.isoformat()}
        lines.append(json.dumps(enriched_record, ensure_ascii=False))

    body = "\n".join(lines)

    s3.put_object(Bucket="bronze", Key=key, Body=body.encode("utf-8"))

    logger.info(f"Zapisano {len(records)} rekordów do s3://bronze/{key}")


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


def write_cycle_manifest(s3, bus_count, tram_count, failed_sources, empty_sources):
    """Record whether a cycle fetched both sources, so Silver can flag partial hours."""
    now = datetime.now(timezone.utc)
    complete = bus_count > 0 and tram_count > 0
    manifest = {
        "ingested_at": now.isoformat(),
        "status": "complete" if complete else "partial",
        "buses": bus_count,
        "trams": tram_count,
        "failed_sources": failed_sources,
        "empty_sources": empty_sources,
    }
    key = (
        f"_manifests/date={now.strftime('%Y-%m-%d')}/hour={now.strftime('%H')}/"
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

    if buses:
        save_to_bronze(s3, buses, "bus")
    if trams:
        save_to_bronze(s3, trams, "tram")

    write_cycle_manifest(s3, len(buses), len(trams), failed_sources, empty_sources)

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
    )

    ensure_bucket_with_retry(s3)

    interval = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
    if interval <= 0:
        raise ValueError("POLL_INTERVAL_SECONDS must be a positive integer")
    alert_threshold = int(os.getenv("DEGRADED_CYCLES_ALERT_THRESHOLD", "10"))
    if alert_threshold <= 0:
        raise ValueError("DEGRADED_CYCLES_ALERT_THRESHOLD must be a positive integer")
    logger.info(f"Starting main loop with interval {interval} seconds")
    signal.signal(signal.SIGTERM, _handle_sigterm)
    degraded_cycles = 0
    while True:
        degraded = True
        try:
            result = run_once(api_key, s3)
            degraded = bool(result["failed_sources"] or result["empty_sources"])
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
        if degraded_cycles == alert_threshold:
            logger.critical(
                "ALERT: ingestion degraded for %s consecutive cycles", degraded_cycles
            )
        time.sleep(interval)


if __name__ == "__main__":
    main()
