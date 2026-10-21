"""End-to-end smoke test: Bronze JSONL -> Silver -> Gold in MinIO.

Run inside the Airflow container against a dedicated test MinIO instance:
    python /tmp/smoke_e2e.py

Never target the application's real data lake. Cleanup deletes objects
under the fixed test partition prefixes. The lock serializes cooperating
smoke tests; it does not protect against other writers.

An interrupted process may leave a lock. Remove it manually only after
confirming that no smoke test is running and inspecting leftover test data.
"""

import json
import os
import sys
import time

import boto3
import duckdb
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

sys.path.insert(0, "/opt/airflow/src")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import bronze_to_silver

DATE = "2020-01-15"
HOUR = "12"

SMOKE_LOCK_KEY = f"_smoke_locks/date={DATE}/hour={HOUR}/lock.json"


def acquire_smoke_lock(s3) -> None:
    try:
        s3.put_object(
            Bucket="bronze",
            Key=SMOKE_LOCK_KEY,
            Body=b"smoke-test",
            IfNoneMatch="*",
        )
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if error_code == "PreconditionFailed" or status == 412:
            raise RuntimeError(
                "Smoke test is already running or a previous lock remains"
            ) from exc
        raise


def build_records():
    good = {
        "Lines": "523",
        "Lon": 21.0,
        "VehicleNumber": "1001",
        "Time": "2020-01-15 13:00:00",
        "Lat": 52.2,
        "Brigade": "1",
        "_ingested_at": "2020-01-15T12:01:00+00:00",
    }
    stale = {
        **good,
        "VehicleNumber": "1002",
        "_ingested_at": "2020-01-15T12:30:00+00:00",
    }
    outside = {**good, "VehicleNumber": "1003", "Lat": 10.0}
    return [good, good, stale, outside]


def test_partition_prefixes(date_str: str, hour_str: str) -> list[tuple[str, str]]:
    return [
        ("bronze", f"vehicle_type=bus/date={date_str}/hour={hour_str}/"),
        ("bronze", f"vehicle_type=tram/date={date_str}/hour={hour_str}/"),
        ("bronze", f"_manifests/date={date_str}/hour={hour_str}/"),
        ("silver", f"vehicle_positions/date={date_str}/hour={hour_str}/"),
        (
            "silver",
            f"quarantine/vehicle_positions/date={date_str}/hour={hour_str}/",
        ),
        ("silver", f"_versions/date={date_str}/hour={hour_str}/"),
        ("silver", f"_staging/date={date_str}/hour={hour_str}/"),
        ("silver", f"_diagnostics/date={date_str}/hour={hour_str}/"),
        ("gold", f"_versions/date={date_str}/hour={hour_str}/"),
        ("gold", f"_staging/date={date_str}/hour={hour_str}/"),
        ("gold", f"_commits/date={date_str}/hour={hour_str}/"),
        ("gold", f"_diagnostics/date={date_str}/hour={hour_str}/"),
    ]


def assert_test_partition_empty(s3, date_str: str, hour_str: str):
    for bucket, prefix in test_partition_prefixes(date_str, hour_str):
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1,
        )
        if response.get("Contents"):
            raise FileExistsError(
                f"Smoke-test partition is not empty: s3://{bucket}/{prefix}"
            )


def wait_for_storage(s3, timeout_seconds: float = 60) -> set[str]:
    """Bound startup retries; authentication failures must fail immediately."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            return {bucket["Name"] for bucket in s3.list_buckets()["Buckets"]}
        except (BotoCoreError, ClientError) as exc:
            if isinstance(exc, ClientError):
                status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if status not in (429, 500, 502, 503, 504):
                    raise
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "MinIO did not become ready before the deadline"
                ) from exc
            time.sleep(1)


def main(run_test=None):
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["MINIO_USER"],
        aws_secret_access_key=os.environ["MINIO_PASSWORD"],
        config=Config(connect_timeout=2, read_timeout=2, retries={"max_attempts": 0}),
    )

    existing = wait_for_storage(s3)
    for bucket in ("bronze", "silver", "gold"):
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)

    acquire_smoke_lock(s3)
    try:
        (run_test or run_smoke_test)(s3)
    finally:
        primary_error = sys.exc_info()[1]
        try:
            s3.delete_object(Bucket="bronze", Key=SMOKE_LOCK_KEY)
        except (BotoCoreError, ClientError) as exc:
            print(
                "Smoke cleanup: lock release failed "
                f"({type(exc).__name__}); lock may remain",
                file=sys.stderr,
            )
            if primary_error is None:
                raise


def run_smoke_test(s3, scenario=None) -> None:
    assert_test_partition_empty(s3, DATE, HOUR)

    con = None
    try:
        con = bronze_to_silver.get_connection()
        if scenario is not None:
            scenario(con, s3)
            return
        key = f"vehicle_type=bus/date={DATE}/hour={HOUR}/smoke.jsonl"
        body = "\n".join(json.dumps(r) for r in build_records())
        s3.put_object(Bucket="bronze", Key=key, Body=body.encode())

        bronze_to_silver.transform_hour(con, DATE, HOUR)

        import silver_to_gold

        manifest_key = f"_commits/date={DATE}/hour={HOUR}/manifest.json"

        def gold_state():
            manifest_obj = s3.get_object(
                Bucket="gold",
                Key=manifest_key,
            )
            manifest = json.loads(manifest_obj["Body"].read())

            out = {}
            for table in silver_to_gold.GOLD_TABLES:
                uri = manifest["tables"][table]["uri"]
                cols = [
                    r[0]
                    for r in con.execute(
                        f"DESCRIBE SELECT * FROM read_parquet("
                        f"'{uri}', hive_partitioning=false)"
                    ).fetchall()
                    if r[0] != "pipeline_run_at"
                ]
                sel = ", ".join(f'"{c}"' for c in cols)
                out[table] = con.execute(
                    f"SELECT {sel} FROM read_parquet("
                    f"'{uri}', hive_partitioning=false) ORDER BY ALL"
                ).fetchall()
            return out

        silver_to_gold.build_gold_hour(con, DATE, HOUR)
        first = gold_state()
        silver_to_gold.build_gold_hour(con, DATE, HOUR)
        assert gold_state() == first, "Gold rebuild is not idempotent"
        manifest_obj = s3.get_object(
            Bucket="gold",
            Key=manifest_key,
        )
        manifest = json.loads(manifest_obj["Body"].read())
        quality_path = manifest["tables"]["hourly_data_quality"]["uri"]

        quality = con.execute(
            "SELECT bronze_records, duplicate_records, safe_for_dashboard,"
            " data_completeness_status FROM read_parquet("
            f"'{quality_path}', hive_partitioning=false)"
        ).fetchone()
        assert quality == (4, 1, False, "unknown"), quality
        vehicles = first["vehicle_activity_hourly"]
        assert len(vehicles) == 1, vehicles
    finally:
        primary_error = sys.exc_info()[1]
        cleanup_errors: list[Exception] = []

        if con is not None:
            try:
                con.close()
            except duckdb.Error as exc:
                cleanup_errors.append(exc)
                print(
                    f"Smoke cleanup: connection close failed ({type(exc).__name__})",
                    file=sys.stderr,
                )

        for bucket, prefix in test_partition_prefixes(DATE, HOUR):
            try:
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(
                    Bucket=bucket,
                    Prefix=prefix,
                ):
                    for obj in page.get("Contents", []):
                        try:
                            s3.delete_object(
                                Bucket=bucket,
                                Key=obj["Key"],
                            )
                        except (BotoCoreError, ClientError) as exc:
                            cleanup_errors.append(exc)
                            print(
                                "Smoke cleanup: object deletion failed "
                                f"({type(exc).__name__})",
                                file=sys.stderr,
                            )
            except (BotoCoreError, ClientError) as exc:
                cleanup_errors.append(exc)
                print(
                    f"Smoke cleanup: object listing failed ({type(exc).__name__})",
                    file=sys.stderr,
                )

        if cleanup_errors and primary_error is None:
            raise RuntimeError(
                f"Smoke cleanup failed: {len(cleanup_errors)} error(s)"
            ) from cleanup_errors[0]


if __name__ == "__main__":
    main()
