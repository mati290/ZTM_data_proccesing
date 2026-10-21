import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

DEFAULT_MAX_AGE_SECONDS = 120
MANIFEST_PREFIX = "_manifests/"


def check_ingestion_health(s3, max_age_seconds=DEFAULT_MAX_AGE_SECONDS, now=None):
    """Return (healthy, message) based on the latest Bronze cycle manifest."""
    if max_age_seconds <= 0:
        return False, "INGESTION_MAX_MANIFEST_AGE_SECONDS must be positive"

    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)

    try:
        paginator = s3.get_paginator("list_objects_v2")
        latest = None

        first_date = min(
            (now - timedelta(seconds=max_age_seconds)).date(),
            (now - timedelta(days=1)).date(),
        )
        scan_date = now.date()

        while scan_date >= first_date:
            prefix = f"{MANIFEST_PREFIX}date={scan_date.isoformat()}/"
            for page in paginator.paginate(
                Bucket="bronze",
                Prefix=prefix,
            ):
                for obj in page.get("Contents", []):
                    modified = obj["LastModified"]
                    if latest is None or modified > latest["LastModified"]:
                        latest = obj

            scan_date -= timedelta(days=1)

        if latest is None:
            return False, "No Bronze cycle manifests found"

        age_seconds = (now - latest["LastModified"]).total_seconds()
        if age_seconds < 0:
            return False, "Latest manifest timestamp is in the future"

        if age_seconds > max_age_seconds:
            return (
                False,
                f"Latest Bronze manifest is stale: age={age_seconds:.0f}s limit={max_age_seconds}s",
            )

        response = s3.get_object(Bucket="bronze", Key=latest["Key"])
        body = response["Body"].read()
        if isinstance(body, bytes):
            body = body.decode("utf-8")

        manifest = json.loads(body)
        if not isinstance(manifest, dict):
            return False, "Latest Bronze manifest must be a JSON object"

        status = manifest.get("status")

        if status != "complete":
            return False, f"Latest Bronze manifest has unhealthy status: {status!r}"

        return (
            True,
            f"Latest Bronze manifest is fresh and complete: age={age_seconds:.0f}s limit={max_age_seconds}s",
        )

    except (BotoCoreError, ClientError, KeyError, TypeError, ValueError) as error:
        logger.error("Ingestion health check failed: %s", type(error).__name__)
        return False, f"S3 manifest check failed: {type(error).__name__}"


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        max_age_seconds = int(
            os.getenv(
                "INGESTION_MAX_MANIFEST_AGE_SECONDS",
                str(DEFAULT_MAX_AGE_SECONDS),
            )
        )
    except ValueError:
        print("UNHEALTHY: invalid INGESTION_MAX_MANIFEST_AGE_SECONDS")
        return 1

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_USER"),
        aws_secret_access_key=os.getenv("MINIO_PASSWORD"),
        config=Config(
            connect_timeout=5,
            read_timeout=15,
            retries={"mode": "standard", "max_attempts": 3},
        ),
    )

    healthy, message = check_ingestion_health(s3, max_age_seconds)
    print(f"{'HEALTHY' if healthy else 'UNHEALTHY'}: {message}")
    return 0 if healthy else 1


if __name__ == "__main__":
    sys.exit(main())
