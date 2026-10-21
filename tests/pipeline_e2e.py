"""Recovery scenarios on dedicated smoke MinIO; never use production storage.

Run inside the Airflow image with this directory mounted alongside smoke_e2e.py.
The shared smoke lock, occupied-partition guard and cleanup protect test data.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import smoke_e2e

import bronze_to_silver as silver
import late_data
import silver_to_gold as gold

DATE, HOUR = smoke_e2e.DATE, smoke_e2e.HOUR
PARTITION = f"date={DATE}/hour={HOUR}"
START = datetime(2020, 1, 15, 12, tzinfo=timezone.utc)
ACTIVE_KEY = f"vehicle_positions/{PARTITION}/_active.json"
COMMIT_KEY = f"_commits/{PARTITION}/manifest.json"


def read_bytes(s3, bucket: str, key: str) -> bytes:
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def seed_slot(s3, index: int) -> dict:
    slot = START + timedelta(seconds=300 * index)
    sources = {}
    for label in ("bus", "tram"):
        record = {
            "VehicleNumber": "100" if label == "bus" else "200",
            "Lines": "1",
            "Brigade": "1",
            "Lat": 52.2,
            "Lon": 21.0,
            "Time": (slot + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
            "_ingested_at": (slot + timedelta(seconds=5)).isoformat(),
        }
        key = f"vehicle_type={label}/{PARTITION}/slot_{index}.jsonl"
        body = (json.dumps(record) + "\n").encode()
        response = s3.put_object(Bucket="bronze", Key=key, Body=body)
        sources[label] = {"key": key, "size": len(body), "etag": response["ETag"]}
    manifest = {
        "schema_version": 2,
        "status": "complete",
        "buses": 1,
        "trams": 1,
        "failed_sources": [],
        "empty_sources": [],
        "cycle_slot_start": slot.isoformat(),
        "cycle_started_at": slot.isoformat(),
        "ingested_at": (slot + timedelta(seconds=5)).isoformat(),
        "poll_interval_seconds": 300,
        "source_objects": sources,
    }
    s3.put_object(
        Bucket="bronze",
        Key=f"_manifests/{PARTITION}/slot_{index}.json",
        Body=json.dumps(manifest).encode(),
    )
    return manifest


def quality(con, s3) -> tuple:
    manifest = json.loads(read_bytes(s3, "gold", COMMIT_KEY))
    # A readable complete commit must reference all four actual Parquet files.
    for table in gold.GOLD_TABLES:
        uri = manifest["tables"][table]["uri"]
        rows = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{uri}', hive_partitioning=false)"
        ).fetchone()[0]
        assert rows == manifest["tables"][table]["rows"], table
    uri = manifest["tables"]["hourly_data_quality"]["uri"]
    return con.execute(
        "SELECT data_completeness_status, expected_slots, observed_slots, "
        "complete_slots, is_final, safe_for_dashboard "
        f"FROM read_parquet('{uri}', hive_partitioning=false)"
    ).fetchone()


def expect_failure(function, message: str) -> None:
    try:
        function()
    except (RuntimeError, ValueError) as exc:
        assert message in str(exc), str(exc)
    else:
        raise AssertionError(f"Expected failure containing {message!r}")


def recovery_scenario(con, s3) -> None:
    # 11/12 slots is below 95%; adding the missing slot must fix coverage.
    first = seed_slot(s3, 0)
    for index in range(1, 11):
        seed_slot(s3, index)
    silver.transform_hour(con, DATE, HOUR, s3_client=s3)
    gold.build_gold_hour(con, DATE, HOUR, START + timedelta(hours=2), s3_client=s3)
    assert quality(con, s3) == ("incomplete", 12, 11, 11, True, False)
    seed_slot(s3, 11)
    # Today's interval must not reinterpret the recorded five-minute policy.
    with patch.dict(os.environ, {"POLL_INTERVAL_SECONDS": "60"}):
        silver.transform_hour(con, DATE, HOUR, s3_client=s3)
    active = read_bytes(s3, "silver", ACTIVE_KEY)
    old_commit = read_bytes(s3, "gold", COMMIT_KEY)

    # Fail a real publication after one successful versioned S3 copy.
    actual_copy = gold.copy_s3_object
    copies = []

    def fail_second_copy(*args, **kwargs):
        copies.append(args)
        if len(copies) == 2:
            raise RuntimeError("injected second copy failure")
        return actual_copy(*args, **kwargs)

    early = START + timedelta(hours=1)
    with patch.object(gold, "copy_s3_object", side_effect=fail_second_copy):
        expect_failure(
            lambda: gold.build_gold_hour(con, DATE, HOUR, early, s3_client=s3),
            "second copy failure",
        )
    assert read_bytes(s3, "gold", COMMIT_KEY) == old_commit
    assert read_bytes(s3, "silver", ACTIVE_KEY) == active
    quality(con, s3)

    with patch.object(
        silver, "transform_hour", side_effect=AssertionError("Fresh Silver rebuilt")
    ):
        late_data.recompute_hour(con, DATE, HOUR, run_at=early, s3_client=s3)
        assert quality(con, s3) == ("complete", 12, 12, 12, False, False)
        result = late_data.reprocess_late_data(
            now=early + timedelta(minutes=15), window_hours=1, s3_client=s3, con=con
        )
        assert result["recomputed"] == [f"{DATE} {HOUR}"], result
    assert quality(con, s3) == ("complete", 12, 12, 12, True, True)

    # A commit failure after all copies must preserve the old readable publication.
    old_commit = read_bytes(s3, "gold", COMMIT_KEY)
    with patch.object(
        gold,
        "write_publication_manifest",
        side_effect=RuntimeError("injected commit failure"),
    ):
        expect_failure(
            lambda: gold.build_gold_hour(con, DATE, HOUR, NOW, s3_client=s3),
            "commit failure",
        )
    assert read_bytes(s3, "gold", COMMIT_KEY) == old_commit
    quality(con, s3)

    # Repeated manifests do not increase slot coverage.
    s3.put_object(
        Bucket="bronze",
        Key=f"_manifests/{PARTITION}/duplicate.json",
        Body=json.dumps(first).encode(),
    )
    silver.transform_hour(con, DATE, HOUR, s3_client=s3)
    gold.build_gold_hour(con, DATE, HOUR, NOW, s3_client=s3)
    assert quality(con, s3) == ("complete", 12, 12, 12, True, True)
    active = read_bytes(s3, "silver", ACTIVE_KEY)
    old_commit = read_bytes(s3, "gold", COMMIT_KEY)

    # Losing one source file must not activate reduced Silver or publish safe Gold.
    s3.delete_object(Bucket="bronze", Key=first["source_objects"]["tram"]["key"])
    expect_failure(
        lambda: late_data.recompute_hour(con, DATE, HOUR, NOW, s3_client=s3),
        "removed or changed",
    )
    assert read_bytes(s3, "silver", ACTIVE_KEY) == active
    assert read_bytes(s3, "gold", COMMIT_KEY) == old_commit
    print(
        "RECOVERY OK: incomplete/complete, copy and commit failures, retry, finality, duplicates, lost input"
    )


def rejected_scenario(con, s3) -> None:
    record = smoke_e2e.build_records()[0]
    record["Lat"] = 0
    s3.put_object(
        Bucket="bronze",
        Key=f"vehicle_type=bus/{PARTITION}/invalid.jsonl",
        Body=json.dumps(record).encode(),
    )
    expect_failure(
        lambda: silver.transform_hour(con, DATE, HOUR, s3_client=s3),
        "No valid Silver rows",
    )
    response = s3.list_objects_v2(Bucket="silver", Prefix=f"_diagnostics/{PARTITION}/")
    assert response.get("Contents"), "Diagnostic quarantine missing"
    print("DIAGNOSTICS OK: all-rejected input preserved for inspection")


NOW = START + timedelta(hours=2)

if __name__ == "__main__":
    for exercise in (recovery_scenario, rejected_scenario):
        smoke_e2e.main(
            lambda s3, scenario=exercise: smoke_e2e.run_smoke_test(s3, scenario)
        )
    # Both scenarios clean their complete prefixes before releasing the shared lock.
    print("PIPELINE E2E OK")
