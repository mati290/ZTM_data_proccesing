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
    for bucket in ("bronze", "silver", "gold"):
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)

    # Verify that the actual MinIO version honors conditional lock creation.
    import silver_to_gold

    lock_cases = (
        (
            bronze_to_silver.silver_hour_lock,
            silver_to_gold.gold_hour_lock,
            silver_to_gold.GoldHourLockedError,
        ),
        (
            silver_to_gold.gold_hour_lock,
            bronze_to_silver.silver_hour_lock,
            bronze_to_silver.SilverHourLockedError,
        ),
    )
    for first_writer, competing_writer, lock_error in lock_cases:
        with first_writer(DATE, HOUR, s3_client=s3):
            try:
                with competing_writer(DATE, HOUR, s3_client=s3):
                    raise AssertionError(
                        "competing Silver/Gold writer unexpectedly acquired the hour lock"
                    )
            except lock_error:
                pass

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
        marker = s3.get_object(
            Bucket="silver",
            Key=f"vehicle_positions/date={DATE}/hour={HOUR}/_completeness.json",
        )
        assert json.loads(marker["Body"].read())["status"] == "unknown"

        import silver_to_gold

        def gold_state():
            out = {}
            for table in silver_to_gold.GOLD_TABLES:
                uri = f"s3://gold/{table}/date={DATE}/hour={HOUR}/data.parquet"
                cols = [
                    r[0]
                    for r in con.execute(
                        f"DESCRIBE SELECT * FROM read_parquet('{uri}', hive_partitioning=false)"
                    ).fetchall()
                    if r[0] != "pipeline_run_at"
                ]
                sel = ", ".join(f'"{c}"' for c in cols)
                out[table] = con.execute(
                    f"SELECT {sel} FROM read_parquet('{uri}', hive_partitioning=false)"
                    f" ORDER BY ALL"
                ).fetchall()
            return out

        silver_to_gold.build_gold_hour(con, DATE, HOUR)
        first = gold_state()
        silver_to_gold.build_gold_hour(con, DATE, HOUR)
        assert gold_state() == first, "Gold rebuild is not idempotent"
        quality = con.execute(
            "SELECT bronze_records, duplicate_records, safe_for_dashboard,"
            " data_completeness_status FROM read_parquet("
            f"'s3://gold/hourly_data_quality/date={DATE}/hour={HOUR}/data.parquet',"
            " hive_partitioning=false)"
        ).fetchone()
        assert quality == (4, 1, False, "unknown"), quality
        vehicles = first["vehicle_activity_hourly"]
        assert len(vehicles) == 1, vehicles
    finally:
        con.close()
        for bucket, prefix in (
            ("silver", f"_locks/pipeline/date={DATE}/hour={HOUR}/"),
            ("bronze", f"vehicle_type=bus/date={DATE}/"),
            ("silver", f"vehicle_positions/date={DATE}/"),
            ("silver", f"quarantine/vehicle_positions/date={DATE}/"),
            *(
                ("gold", f"{table}/date={DATE}/")
                for table in (
                    "vehicle_activity_hourly",
                    "hourly_line_metrics",
                    "hourly_network_metrics",
                    "hourly_data_quality",
                )
            ),
        ):
            listing = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            for obj in listing.get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
    print("SMOKE OK")


if __name__ == "__main__":
    main()
