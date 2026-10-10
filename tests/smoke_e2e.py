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


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["MINIO_USER"],
        aws_secret_access_key=os.environ["MINIO_PASSWORD"],
    )
    existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    for bucket in ("bronze", "silver"):
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)

    key = f"vehicle_type=bus/date={DATE}/hour={HOUR}/smoke.jsonl"
    body = "\n".join(json.dumps(r) for r in build_records())
    s3.put_object(Bucket="bronze", Key=key, Body=body.encode())

    con = bronze_to_silver.get_connection()
    try:
        bronze_to_silver.transform_hour(con, DATE, HOUR)
        silver = f"s3://silver/vehicle_positions/date={DATE}/hour={HOUR}/data.parquet"
        rows = con.execute(
            f"SELECT vehicle_number FROM read_parquet('{silver}')"
        ).fetchall()
        assert len(rows) == 1 and str(rows[0][0]) == "1001", rows
        rejected = f"s3://silver/quarantine/vehicle_positions/date={DATE}/hour={HOUR}/data.parquet"
        row = con.execute(f"SELECT COUNT(*) FROM read_parquet('{rejected}')").fetchone()
        assert row is not None and row[0] == 3, row
    finally:
        con.close()
        for bucket, prefix in (
            ("bronze", f"vehicle_type=bus/date={DATE}/"),
            ("silver", f"vehicle_positions/date={DATE}/"),
            ("silver", f"quarantine/vehicle_positions/date={DATE}/"),
        ):
            listing = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            for obj in listing.get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
    print("SMOKE OK")


if __name__ == "__main__":
    main()
