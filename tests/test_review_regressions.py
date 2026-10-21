"""Regression cases found during the October pipeline review."""

import io
import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import duckdb
from botocore.exceptions import ClientError, EndpointConnectionError

import bronze_to_silver as silver
import ingest
import late_data
import silver_to_gold as gold
from check_ingestion_health import check_ingestion_health
from ingestion_contract import validate_cycle_manifest
from tests import smoke_e2e
from tests.test_late_data import FakeS3, gold_manifest, gold_publication_objects

START = datetime(2026, 1, 15, 12, tzinfo=timezone.utc)
NOW = START + timedelta(hours=2)


def cycle_manifest(interval: int = 300) -> dict:
    return {
        "schema_version": 2,
        "status": "complete",
        "buses": 1,
        "trams": 1,
        "failed_sources": [],
        "empty_sources": [],
        "ingested_at": START.isoformat(),
        "cycle_started_at": START.isoformat(),
        "cycle_slot_start": START.isoformat(),
        "poll_interval_seconds": interval,
        "source_objects": {
            label: {
                "key": f"vehicle_type={label}/date=2026-01-15/hour=12/a.jsonl",
                "size": 123,
                "etag": '"original"',
            }
            for label in ("bus", "tram")
        },
    }


class InputContractRegressionTests(unittest.TestCase):
    def test_producer_links_manifest_to_exact_written_objects(self):
        s3 = Mock()
        s3.put_object.return_value = {"ETag": '"stored"'}
        with (
            patch.dict(os.environ, {"POLL_INTERVAL_SECONDS": "31"}),
            patch.object(
                ingest,
                "fetch_positions",
                side_effect=[[{"VehicleNumber": "1"}], [{"VehicleNumber": "2"}]],
            ),
        ):
            ingest.run_once("test-key", s3)
        writes = s3.put_object.call_args_list
        self.assertEqual(len(writes), 3)
        manifest = json.loads(writes[2].kwargs["Body"])
        validate_cycle_manifest(manifest)
        self.assertEqual(manifest["schema_version"], 2)
        self.assertEqual(manifest["poll_interval_seconds"], 31)
        for label, write in zip(("bus", "tram"), writes[:2], strict=True):
            self.assertEqual(
                manifest["source_objects"][label],
                {
                    "key": write.kwargs["Key"],
                    "size": len(write.kwargs["Body"]),
                    "etag": '"stored"',
                },
            )

    def assess(self, manifest, missing=None, changed=None):
        key = "_manifests/date=2026-01-15/hour=12/a.json"
        inputs = [{"key": key}]
        for label, source in manifest["source_objects"].items():
            if label != missing and source is not None:
                inputs.append({**source, **(changed or {})})
        s3 = Mock()
        s3.get_object.side_effect = lambda **_: {
            "Body": io.BytesIO(json.dumps(manifest).encode())
        }
        return silver.assess_completeness(
            "2026-01-15", "12", s3_client=s3, source_inputs=inputs, now_utc=NOW
        )

    def test_versioned_manifest_rejects_missing_or_changed_data(self):
        for missing, changed in (
            ("tram", None),
            (None, {"size": 1}),
            (None, {"etag": '"replaced"'}),
        ):
            with (
                self.subTest(missing=missing, changed=changed),
                self.assertRaisesRegex(ValueError, "missing or changed"),
            ):
                self.assess(cycle_manifest(), missing, changed)

    def test_recorded_interval_survives_environment_change(self):
        with patch.dict(os.environ, {"POLL_INTERVAL_SECONDS": "60"}):
            result = self.assess(cycle_manifest())
        self.assertEqual(result["expected_cycles"], 12)
        self.assertEqual(result["observed_slots"], 1)
        self.assertEqual(result["status"], "incomplete")

    def test_mixed_recorded_policies_require_explicit_migration(self):
        keys = [f"_manifests/date=2026-01-15/hour=12/{index}.json" for index in (0, 1)]
        for second in (
            cycle_manifest(600),
            {**cycle_manifest(300), "schema_version": 1},
        ):
            payloads = dict(zip(keys, (cycle_manifest(300), second), strict=True))
            s3 = Mock()
            s3.get_object.side_effect = lambda Bucket, Key, payloads=payloads: {
                "Body": io.BytesIO(json.dumps(payloads[Key]).encode())
            }
            with (
                self.subTest(second=second),
                self.assertRaisesRegex(ValueError, "Mixed poll intervals"),
            ):
                silver.assess_completeness(
                    "2026-01-15",
                    "12",
                    s3_client=s3,
                    source_inputs=[{"key": key} for key in keys],
                    now_utc=NOW,
                )

    def test_version_two_requires_valid_source_identity_and_interval(self):
        cases = (
            {"poll_interval_seconds": True},
            {"poll_interval_seconds": 0},
            {"source_objects": {}},
            {"source_objects": {"bus": None, "tram": None}},
        )
        for fields in cases:
            with (
                self.subTest(fields=fields),
                self.assertRaises((TypeError, ValueError)),
            ):
                validate_cycle_manifest({**cycle_manifest(), **fields})

    def test_existing_snapshot_loss_stops_before_reading_or_publication(self):
        old = [{"key": "bus.jsonl", "size": 123, "etag": '"old"'}]
        for current in ([], [{**old[0], "etag": '"changed"'}]):
            with self.subTest(current=current):
                con = Mock()
                con.execute.return_value.fetchone.return_value = ("some-file",)
                with (
                    patch.object(silver, "capture_bronze_inputs", return_value=current),
                    patch.object(silver, "read_silver_source_inputs", return_value=old),
                    patch.object(silver, "materialize_bronze_inputs") as read,
                    patch.object(silver, "write_silver_active_manifest") as activate,
                    self.assertRaisesRegex(ValueError, "removed or changed"),
                ):
                    silver.transform_hour(con, "2026-01-15", "12", s3_client=Mock())
                read.assert_not_called()
                activate.assert_not_called()


class GoldContractRegressionTests(unittest.TestCase):
    def test_legacy_change_during_read_stops_before_publication(self):
        partition = "date=2026-01-15/hour=12"
        uris = {
            "silver": f"s3://silver/vehicle_positions/{partition}/data.parquet",
            "quarantine": f"s3://silver/quarantine/vehicle_positions/{partition}/data.parquet",
            "completeness": f"s3://silver/vehicle_positions/{partition}/_completeness.json",
            "partition": partition,
        }
        identity = {
            name: '"original"' for name in ("data", "quarantine", "completeness")
        }
        with (
            patch.object(gold, "partition_uris", return_value=uris),
            patch.object(gold, "s3_object_exists", return_value=True),
            patch.object(
                gold,
                "silver_artifact_identity",
                side_effect=[
                    identity,
                    {**identity, "completeness": '"changed"'},
                ],
            ),
            patch.object(
                gold, "load_completeness", return_value={"status": "complete"}
            ),
            patch.object(gold, "load_silver_hour", return_value=1),
            patch.object(gold, "load_quarantine_hour"),
            patch.object(gold, "publish_tables") as publish,
            self.assertRaisesRegex(ValueError, "changed while reading"),
        ):
            gold.build_gold_hour(Mock(), "2026-01-15", "12", NOW, s3_client=Mock())
        publish.assert_not_called()

    def test_changed_legacy_marker_schedules_gold_repair(self):
        partition = "date=2026-01-15/hour=12"
        data = f"vehicle_positions/{partition}/data.parquet"
        marker = f"vehicle_positions/{partition}/_completeness.json"
        quarantine = f"quarantine/vehicle_positions/{partition}/data.parquet"
        manifest = gold_manifest(f"s3://silver/{data}")
        objects = gold_publication_objects(manifest)
        objects.update(
            {
                ("silver", data): START,
                ("silver", marker): START,
                ("silver", quarantine): START,
                ("bronze", f"vehicle_type=bus/{partition}/a.jsonl"): START
                - timedelta(minutes=1),
            }
        )
        s3 = FakeS3(objects)
        actual_head = s3.head_object

        def head(Bucket, Key):
            if Bucket == "silver" and Key == marker:
                return {"ETag": '"changed-marker"'}
            return actual_head(Bucket, Key)

        with patch.object(s3, "head_object", side_effect=head):
            result = late_data.find_stale_hours(
                now=START + timedelta(hours=1, minutes=40), window_hours=1, s3_client=s3
            )
        self.assertEqual(result, [("2026-01-15", "12")])

    def test_legacy_complete_markers_need_consistent_evidence(self):
        cases = (
            {"status": "complete"},
            {"status": "complete", "cycles": 0, "complete_cycles": 0},
            {"status": "complete", "cycles": 1, "complete_cycles": 1},
            {"status": "complete", "cycles": 120, "complete_cycles": 100},
            {
                "status": "complete",
                "cycles": 1,
                "complete_cycles": 1,
                "expected_cycles": 120,
            },
        )
        for marker in cases:
            with (
                self.subTest(marker=marker),
                patch.object(gold, "read_completeness_marker", return_value=marker),
                self.assertRaises(ValueError),
            ):
                gold.load_completeness("2026-01-15", "12")

    def test_zero_cycles_cannot_be_safe_even_for_direct_quality_builder(self):
        con = duckdb.connect()
        self.addCleanup(con.close)
        con.execute(
            "CREATE TABLE silver_hour AS SELECT TIMESTAMPTZ '2026-01-15 12:00:00+00' AS ingested_at"
        )
        con.execute("CREATE TABLE quarantine_hour (rejection_reason VARCHAR)")
        gold.build_quality_table(
            con,
            "2026-01-15",
            "12",
            {
                "status": "complete",
                "cycles": 0,
                "complete_cycles": 0,
            },
            1,
            True,
            NOW,
        )
        self.assertFalse(
            con.execute(
                "SELECT safe_for_dashboard FROM hourly_data_quality"
            ).fetchone()[0]
        )

    def test_selected_completeness_uri_does_not_resolve_active_again(self):
        uri = "s3://silver/_versions/date=2026-01-15/hour=12/a/_completeness.json"
        s3 = Mock()
        s3.get_object.return_value = {
            "Body": io.BytesIO(
                b'{"status":"complete","cycles":120,"complete_cycles":120,"expected_cycles":120}'
            )
        }
        with patch.object(silver, "resolve_silver_artifacts") as resolve:
            result = gold.load_completeness("2026-01-15", "12", s3, marker_uri=uri)
        self.assertEqual(result["status"], "complete")
        resolve.assert_not_called()
        s3.get_object.assert_called_once_with(
            Bucket="silver", Key=uri.removeprefix("s3://silver/")
        )

    def test_legacy_identity_tracks_optional_file_changes_and_absence(self):
        artifacts = {
            "data": "s3://silver/data",
            "quarantine": "s3://silver/quarantine",
            "completeness": "s3://silver/completeness",
        }
        s3 = Mock()
        s3.head_object.return_value = {"ETag": '"old"'}
        before = gold.silver_artifact_identity(artifacts, s3)
        s3.head_object.side_effect = lambda Bucket, Key: {
            "ETag": '"new"' if Key == "completeness" else '"old"'
        }
        after = gold.silver_artifact_identity(artifacts, s3)
        self.assertNotEqual(before, after)
        self.assertEqual(before["data"], after["data"])


class HealthAndSmokeRegressionTests(unittest.TestCase):
    def test_health_rejects_contradictory_and_unknown_version_manifests(self):
        for marker in (
            {"status": "complete", "buses": 0, "trams": 0},
            {"schema_version": 999, "status": "complete"},
        ):
            s3 = Mock()
            s3.get_paginator.return_value.paginate.return_value = [
                {"Contents": [{"Key": "test.json", "LastModified": NOW}]}
            ]
            s3.get_object.return_value = {
                "Body": io.BytesIO(json.dumps(marker).encode())
            }
            with self.subTest(marker=marker):
                healthy, _ = check_ingestion_health(s3, now=NOW)
                self.assertFalse(healthy)

    def test_readiness_retries_transient_errors_and_has_deadline(self):
        error = EndpointConnectionError(endpoint_url="http://test.invalid")
        s3 = Mock()
        s3.list_buckets.side_effect = [error, {"Buckets": [{"Name": "bronze"}]}]
        with patch.object(smoke_e2e.time, "sleep") as sleep:
            self.assertEqual(smoke_e2e.wait_for_storage(s3), {"bronze"})
        sleep.assert_called_once_with(1)
        s3.list_buckets.side_effect = error
        with (
            patch.object(smoke_e2e.time, "monotonic", side_effect=[0, 61]),
            self.assertRaisesRegex(TimeoutError, "deadline"),
        ):
            smoke_e2e.wait_for_storage(s3)

    def test_readiness_does_not_retry_access_denied(self):
        s3 = Mock()
        s3.list_buckets.side_effect = ClientError(
            {
                "Error": {"Code": "AccessDenied"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            },
            "ListBuckets",
        )
        with (
            patch.object(smoke_e2e.time, "sleep") as sleep,
            self.assertRaises(ClientError),
        ):
            smoke_e2e.wait_for_storage(s3)
        sleep.assert_not_called()

    def test_smoke_refuses_existing_silver_diagnostics(self):
        prefix = f"_diagnostics/date={smoke_e2e.DATE}/hour={smoke_e2e.HOUR}/"
        s3 = Mock()
        s3.list_objects_v2.side_effect = lambda Bucket, Prefix, MaxKeys: (
            {"Contents": [{"Key": prefix + "old/quarantine.parquet"}]}
            if Bucket == "silver" and Prefix == prefix
            else {}
        )
        with self.assertRaises(FileExistsError):
            smoke_e2e.assert_test_partition_empty(s3, smoke_e2e.DATE, smoke_e2e.HOUR)
        s3.delete_object.assert_not_called()
