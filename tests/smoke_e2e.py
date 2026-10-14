"""End-to-end smoke test: Bronze JSONL in MinIO -> Silver Parquet.

Run inside the Airflow container (same DuckDB version as production):
    python /tmp/smoke_e2e.py
"""

import json
import os
import sys

import boto3

sys.path.insert(0, "/opt/airflow/src")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import bronze_to_silver

DATE = "2020-01-15"
HOUR = "12"


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


def assert_test_partition_empty(s3, date_str, hour_str):
    prefixes = [
        ("bronze", f"vehicle_type=bus/date={date_str}/hour={hour_str}/"),
        ("bronze", f"vehicle_type=tram/date={date_str}/hour={hour_str}/"),
        ("silver", f"vehicle_positions/date={date_str}/hour={hour_str}/"),
        (
            "silver",
            f"quarantine/vehicle_positions/date={date_str}/hour={hour_str}/",
        ),
        ("gold", f"_versions/date={date_str}/hour={hour_str}/"),
        ("gold", f"_staging/date={date_str}/hour={hour_str}/"),
        ("gold", f"_commits/date={date_str}/hour={hour_str}/"),
    ]

    for bucket, prefix in prefixes:
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1,
        )
        if response.get("Contents"):
            raise FileExistsError(
                f"Smoke-test partition is not empty: s3://{bucket}/{prefix}"
            )


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["MINIO_USER"],
        aws_secret_access_key=os.environ["MINIO_PASSWORD"],
    )

    con = None
    try:
        existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
        for bucket in ("bronze", "silver", "gold"):
            if bucket not in existing:
                s3.create_bucket(Bucket=bucket)

        assert_test_partition_empty(s3, DATE, HOUR)

        key = f"vehicle_type=bus/date={DATE}/hour={HOUR}/smoke.jsonl"
        body = "\n".join(json.dumps(r) for r in build_records())
        s3.put_object(Bucket="bronze", Key=key, Body=body.encode())

        con = bronze_to_silver.get_connection()
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
        if con is not None:
            con.close()
        for bucket, prefix in (
            ("bronze", f"vehicle_type=bus/date={DATE}/hour={HOUR}/"),
            ("bronze", f"vehicle_type=tram/date={DATE}/hour={HOUR}/"),
            ("silver", f"vehicle_positions/date={DATE}/hour={HOUR}/"),
            ("silver", f"quarantine/vehicle_positions/date={DATE}/hour={HOUR}/"),
            ("gold", f"_versions/date={DATE}/hour={HOUR}/"),
            ("gold", f"_staging/date={DATE}/hour={HOUR}/"),
            ("gold", f"_commits/date={DATE}/hour={HOUR}/"),
        ):
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(
                Bucket=bucket,
                Prefix=prefix,
            ):
                for obj in page.get("Contents", []):
                    s3.delete_object(
                        Bucket=bucket,
                        Key=obj["Key"],
                    )
    print("SMOKE OK")


if __name__ == "__main__":
    main()
