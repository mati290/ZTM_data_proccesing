import json
import logging
import os
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
            result = data["result"]
            if not isinstance(result, list):
                raise ZTMApiError(f"API zwróciło błąd: {result}")
            return result
        except (requests.RequestException, ZTMApiError) as e:
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


def ensure_bucket(s3):
    try:
        s3.head_bucket(Bucket="bronze")
    except ClientError as exc:
        error = exc.response.get("Error", {})
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status != 404 and error.get("Code") not in {
            "404",
            "NoSuchBucket",
            "NotFound",
        }:
            raise

        s3.create_bucket(Bucket="bronze")


def run_once(api_key, s3):
    buses = fetch_positions(1, api_key=api_key)
    trams = fetch_positions(2, api_key=api_key)
    logger.info(f"Fetched {len(buses)} buses and {len(trams)} trams")

    save_to_bronze(s3, buses, "bus")
    save_to_bronze(s3, trams, "tram")


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

    ensure_bucket(s3)

    interval = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
    logger.info(f"Starting main loop with interval {interval} seconds")
    while True:
        try:
            run_once(api_key, s3)
        except (
            requests.RequestException,
            ZTMApiError,
            BotoCoreError,
            ClientError,
        ) as error:
            logger.error(
                "Ingestion cycle failed (%s); retrying in next cycle",
                _safe_error_summary(error),
            )
        time.sleep(interval)


if __name__ == "__main__":
    main()
