import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import duckdb
from botocore.exceptions import ClientError

import bronze_to_silver
import ingest
from bronze_to_silver import (
    QUALITY_SQL,
    REJECTED_SQL,
    TRANSFORM_SQL,
    BronzeDataMissingError,
    assess_completeness,
    copy_s3_object,
    delete_s3_object,
    get_connection,
    is_before_first_bronze_hour,
    is_hour_complete,
    main,
    parse_s3_endpoint,
    read_completeness_marker,
    resolve_silver_artifacts,
    s3_object_exists,
    transform_hour,
    validate_partition,
    validate_silver_schema,
    write_silver_active_manifest,
)


class CompletenessTests(unittest.TestCase):
    @staticmethod
    def _listing(first_keys):
        client = Mock()
        client.list_objects_v2.side_effect = [
            {"Contents": [{"Key": key}]} if key else {} for key in first_keys
        ]
        return client

    @staticmethod
    def _marker_client(status=None, error_code=None):
        client = Mock()
        if error_code:
            client.get_object.side_effect = ClientError(
                {"Error": {"Code": error_code}}, "GetObject"
            )
        else:
            body = Mock()
            body.read.return_value = json.dumps({"status": status}).encode()
            client.get_object.return_value = {"Body": body}
        return client

    def test_elapsed_slots_follow_epoch_boundaries(self):
        hour_start = datetime(2026, 10, 9, 7, tzinfo=timezone.utc)
        start_epoch = int(hour_start.timestamp())
        interval = 31

        for elapsed in (0, 1, 15, 30, 31, 62, 3599):
            with self.subTest(elapsed=elapsed):
                expected = sum(
                    (start_epoch + second) % interval == 0
                    for second in range(elapsed + 1)
                )
                now = datetime.fromtimestamp(
                    start_epoch + elapsed,
                    tz=timezone.utc,
                )

                actual = bronze_to_silver.expected_slots_elapsed(
                    "2026-10-09", "07", now, interval
                )

                self.assertEqual(actual, expected)

    def test_expected_slots_follow_epoch_alignment(self):
        hour_start = datetime(2026, 10, 9, 7, tzinfo=timezone.utc)
        start_epoch = int(hour_start.timestamp())

        for interval in (7, 31):
            with self.subTest(interval=interval):
                expected = sum(
                    (start_epoch + second) % interval == 0 for second in range(3600)
                )

                actual = bronze_to_silver.expected_slots_elapsed(
                    "2026-10-09",
                    "07",
                    datetime(2026, 10, 9, 8, tzinfo=timezone.utc),
                    interval,
                )

                self.assertEqual(actual, expected)

    def test_active_manifest_snapshot_round_trip(self):
        publication_id = "a" * 32
        partition = "date=2026-10-09/hour=07"
        version_dir = f"s3://silver/_versions/{partition}/{publication_id}"
        artifacts = {
            "data": f"{version_dir}/data.parquet",
            "quarantine": f"{version_dir}/quarantine.parquet",
            "completeness": f"{version_dir}/_completeness.json",
        }
        valid_inputs = [
            {
                "key": f"vehicle_type=bus/{partition}/a.jsonl",
                "etag": '"test-etag"',
                "size": 123,
                "last_modified": "2026-10-09T07:00:05+00:00",
            }
        ]

        for source_inputs in ([], valid_inputs):
            with self.subTest(count=len(source_inputs)):
                client = Mock()
                with patch(
                    "bronze_to_silver._s3_client",
                    return_value=client,
                ):
                    write_silver_active_manifest(
                        "2026-10-09",
                        "7",
                        publication_id,
                        artifacts,
                        source_inputs=source_inputs,
                    )

                body = client.put_object.call_args.kwargs["Body"]
                client.get_object.return_value = {
                    "Body": Mock(read=Mock(return_value=body))
                }
                result = bronze_to_silver.read_silver_source_inputs(
                    "2026-10-09", "7", client
                )

                self.assertEqual(result, source_inputs)

    def test_active_manifest_rejects_invalid_snapshot_before_writing(self):
        publication_id = "a" * 32
        partition = "date=2026-10-09/hour=07"
        version_dir = f"s3://silver/_versions/{partition}/{publication_id}"
        artifacts = {
            "data": f"{version_dir}/data.parquet",
            "quarantine": f"{version_dir}/quarantine.parquet",
            "completeness": f"{version_dir}/_completeness.json",
        }
        source_inputs = [
            {
                "key": f"vehicle_type=bus/{partition}/a.jsonl",
                "etag": '"test-etag"',
                "size": True,
                "last_modified": "2026-10-09T07:00:05+00:00",
            }
        ]

        with patch("bronze_to_silver._s3_client") as client_factory:
            with self.assertRaisesRegex(ValueError, "source_inputs.*size"):
                write_silver_active_manifest(
                    "2026-10-09",
                    "07",
                    publication_id,
                    artifacts,
                    source_inputs=source_inputs,
                )

            client_factory.assert_not_called()

    def test_versioned_cycle_manifest_rejects_invalid_timestamps(self):
        valid_manifest = {
            "schema_version": 1,
            "status": "complete",
            "buses": 1,
            "trams": 1,
            "failed_sources": [],
            "empty_sources": [],
            "ingested_at": "2026-10-09T07:00:05+00:00",
            "cycle_started_at": "2026-10-09T07:00:01+00:00",
            "cycle_slot_start": "2026-10-09T07:00:00+00:00",
        }

        for field in (
            "ingested_at",
            "cycle_started_at",
            "cycle_slot_start",
        ):
            for value in (
                None,
                123,
                "not-a-timestamp",
                "2026-10-09T07:00:00",
            ):
                with self.subTest(field=field, value=value):
                    manifest = {**valid_manifest, field: value}

                    with self.assertRaisesRegex(ValueError, field):
                        bronze_to_silver.validate_cycle_manifest(manifest)

    def test_ingestion_manifests_match_supported_schema(self):
        cases = (
            (1, 1, [], []),
            (0, 1, ["bus"], []),
            (1, 0, [], ["tram"]),
            (0, 0, ["bus"], ["tram"]),
        )
        slot_start = datetime(2026, 10, 9, 7, tzinfo=timezone.utc)

        for buses, trams, failed, empty in cases:
            with self.subTest(buses=buses, trams=trams):
                client = Mock()
                ingest.write_cycle_manifest(
                    client,
                    buses,
                    trams,
                    failed,
                    empty,
                    cycle_started_at=slot_start,
                    cycle_slot_start=slot_start,
                )
                manifest = json.loads(client.put_object.call_args.kwargs["Body"])

                self.assertEqual(manifest["schema_version"], 1)
                self.assertIsNone(bronze_to_silver.validate_cycle_manifest(manifest))

    def test_versioned_cycle_manifest_requires_all_fields(self):
        valid_manifest = {
            "schema_version": 1,
            "status": "complete",
            "buses": 1,
            "trams": 1,
            "failed_sources": [],
            "empty_sources": [],
            "ingested_at": "2026-10-09T07:00:05+00:00",
            "cycle_started_at": "2026-10-09T07:00:01+00:00",
            "cycle_slot_start": "2026-10-09T07:00:00+00:00",
        }

        for field in valid_manifest:
            if field == "schema_version":
                continue
            with self.subTest(missing=field):
                manifest = valid_manifest.copy()
                manifest.pop(field)

                with self.assertRaisesRegex(ValueError, "missing"):
                    bronze_to_silver.validate_cycle_manifest(manifest)

    def test_cycle_manifest_rejects_invalid_schema_version(self):
        for version in (None, True, 0, -1, "1", 1.0, 2):
            with self.subTest(version=version):
                manifest = {
                    "schema_version": version,
                    "status": "complete",
                }

                with self.assertRaisesRegex(ValueError, "schema_version"):
                    bronze_to_silver.validate_cycle_manifest(manifest)

    def test_cycle_manifest_accepts_consistent_source_lists(self):
        cases = (
            ("complete", 1, 1, [], []),
            ("partial", 0, 1, ["bus"], []),
            ("partial", 1, 0, [], ["tram"]),
            ("partial", 0, 0, ["bus"], ["tram"]),
            ("partial", 0, 0, [], ["bus", "tram"]),
        )

        for status, buses, trams, failed, empty in cases:
            with self.subTest(
                status=status,
                failed=failed,
                empty=empty,
            ):
                manifest = {
                    "status": status,
                    "buses": buses,
                    "trams": trams,
                    "failed_sources": failed,
                    "empty_sources": empty,
                }

                self.assertIsNone(bronze_to_silver.validate_cycle_manifest(manifest))

    def test_cycle_manifest_rejects_invalid_source_lists(self):
        cases = (
            ("bus", []),
            (None, []),
            ([123], []),
            (["car"], []),
            (["bus", "bus"], []),
            ([], ["bus", "bus"]),
            (["bus"], ["bus"]),
            (["tram"], ["bus"]),
            ([], ["tram"]),
            ([], []),
        )

        for failed, empty in cases:
            with self.subTest(failed=failed, empty=empty):
                manifest = {
                    "status": "partial",
                    "buses": 0,
                    "trams": 1,
                    "failed_sources": failed,
                    "empty_sources": empty,
                }

                with self.assertRaisesRegex(ValueError, "sources"):
                    bronze_to_silver.validate_cycle_manifest(manifest)

    def test_cycle_manifest_rejects_invalid_source_counts(self):
        for field in ("buses", "trams"):
            for value in (None, True, -1, "1", 1.5):
                with self.subTest(field=field, value=value):
                    manifest = {
                        "status": "partial",
                        "buses": 0,
                        "trams": 0,
                    }
                    manifest[field] = value

                    with self.assertRaisesRegex(ValueError, "count"):
                        bronze_to_silver.validate_cycle_manifest(manifest)

        for field in ("buses", "trams"):
            with self.subTest(missing=field):
                manifest = {
                    "status": "partial",
                    "buses": 0,
                    "trams": 0,
                }
                manifest.pop(field)

                with self.assertRaisesRegex(ValueError, "counter"):
                    bronze_to_silver.validate_cycle_manifest(manifest)

    def test_cycle_manifest_accepts_valid_and_legacy_counts(self):
        cases = (
            {"status": "complete"},
            {"status": "partial"},
            {"status": "complete", "buses": 1, "trams": 1},
            {"status": "partial", "buses": 0, "trams": 1},
            {"status": "partial", "buses": 1, "trams": 0},
            {"status": "partial", "buses": 0, "trams": 0},
        )

        for manifest in cases:
            with self.subTest(manifest=manifest):
                self.assertIsNone(bronze_to_silver.validate_cycle_manifest(manifest))

    def test_cycle_manifest_status_must_match_source_counts(self):
        cases = (
            ("complete", 0, 1),
            ("complete", 1, 0),
            ("complete", 0, 0),
            ("partial", 1, 1),
        )

        for status, buses, trams in cases:
            with self.subTest(status=status, buses=buses, trams=trams):
                manifest = {
                    "status": status,
                    "buses": buses,
                    "trams": trams,
                    "cycle_slot_start": "2026-10-09T07:00:00+00:00",
                }
                client = self._client([status])
                client.get_object.side_effect = None
                client.get_object.return_value = {
                    "Body": Mock(read=Mock(return_value=json.dumps(manifest).encode()))
                }

                with self.assertRaisesRegex(ValueError, "status"):
                    assess_completeness(
                        "2026-10-09",
                        "07",
                        client,
                        poll_interval_seconds=30,
                        now_utc=datetime(2026, 10, 9, 8, tzinfo=timezone.utc),
                    )

    def test_completeness_marker_must_be_a_json_object(self):
        for payload in ([], None, "complete", 123, True):
            with self.subTest(payload=payload):
                client = Mock()
                client.get_object.return_value = {
                    "Body": Mock(read=Mock(return_value=json.dumps(payload).encode()))
                }

                with (
                    patch(
                        "bronze_to_silver.resolve_silver_artifacts",
                        return_value={
                            "completeness": (
                                "s3://silver/vehicle_positions/"
                                "date=2026-10-09/hour=07/_completeness.json"
                            )
                        },
                    ),
                    self.assertRaisesRegex(TypeError, "JSON object"),
                ):
                    read_completeness_marker("2026-10-09", "07", client)

    def test_manifest_must_be_a_json_object(self):
        for payload in ([], None, "complete", 123):
            with self.subTest(payload=payload):
                client = self._client(["complete"])
                client.get_object.side_effect = None
                client.get_object.return_value = {
                    "Body": Mock(read=Mock(return_value=json.dumps(payload).encode()))
                }

                with self.assertRaisesRegex(TypeError, "JSON object"):
                    assess_completeness(
                        "2026-10-09",
                        "07",
                        client,
                        poll_interval_seconds=30,
                        now_utc=datetime(2026, 10, 9, 8, tzinfo=timezone.utc),
                    )

    def test_slot_metrics_distinguish_legacy_and_invalid_slots(self):
        for slot_starts, expected_count, expected_coverage in (
            (None, None, None),
            (["invalid-timestamp"], 0, 0.0),
        ):
            with self.subTest(slot_starts=slot_starts):
                result = assess_completeness(
                    "2026-10-09",
                    "07",
                    self._client(["complete"], slot_starts),
                    poll_interval_seconds=30,
                    now_utc=datetime(2026, 10, 9, 7, 0, 15, tzinfo=timezone.utc),
                )

                self.assertEqual(result["observed_slots"], expected_count)
                self.assertEqual(result["complete_slots"], expected_count)
                self.assertEqual(result["slot_coverage_ratio"], expected_coverage)
                self.assertIsNone(result["complete_slot_ratio"])

    def test_completeness_reads_only_captured_manifests(self):
        client = self._client(["complete", "partial"])
        source_inputs = [
            {
                "key": "_manifests/date=2026-10-09/hour=07/c0.json",
                "etag": '"test-etag"',
                "size": 123,
                "last_modified": "2026-10-09T07:00:05+00:00",
            }
        ]

        result = assess_completeness(
            "2026-10-09",
            "07",
            client,
            poll_interval_seconds=30,
            now_utc=datetime(2026, 10, 9, 8, tzinfo=timezone.utc),
            source_inputs=source_inputs,
        )

        self.assertEqual(result["cycles"], 1)
        self.assertEqual(result["complete_cycles"], 1)
        client.get_paginator.assert_not_called()
        client.get_object.assert_called_once_with(
            Bucket="bronze",
            Key="_manifests/date=2026-10-09/hour=07/c0.json",
        )

    def test_invalid_slots_do_not_fall_back_to_legacy_manifest_counting(self):
        for invalid_slot in (
            ["2026-10-09T07:00:00+00:00"],
            {"start": "2026-10-09T07:00:00+00:00"},
            123,
            True,
            "",
            None,
        ):
            with self.subTest(invalid_slot=invalid_slot):
                result = assess_completeness(
                    "2026-10-09",
                    "07",
                    self._client(["complete"], [invalid_slot]),
                    poll_interval_seconds=30,
                    now_utc=datetime(2026, 10, 9, 7, 0, 15, tzinfo=timezone.utc),
                )

                self.assertEqual(result["missing_cycles"], 1)
                self.assertEqual(result["status"], "incomplete")

    def test_invalid_slot_types_do_not_crash_or_increase_coverage(self):
        for invalid_slot in (
            ["2026-10-09T07:00:30+00:00"],
            {"start": "2026-10-09T07:00:30+00:00"},
            123,
            True,
        ):
            with self.subTest(invalid_slot=invalid_slot):
                result = assess_completeness(
                    "2026-10-09",
                    "07",
                    self._client(
                        ["complete", "complete"],
                        ["2026-10-09T07:00:00+00:00", invalid_slot],
                    ),
                    poll_interval_seconds=30,
                    now_utc=datetime(2026, 10, 9, 7, 0, 30, tzinfo=timezone.utc),
                )

                self.assertEqual(result["missing_cycles"], 1)
                self.assertEqual(result["status"], "incomplete")

    def test_active_manifest_records_bronze_inputs(self):
        publication_id = "a" * 32
        partition = "date=2026-10-09/hour=07"
        version_dir = f"s3://silver/_versions/{partition}/{publication_id}"
        artifacts = {
            "data": f"{version_dir}/data.parquet",
            "quarantine": f"{version_dir}/quarantine.parquet",
            "completeness": f"{version_dir}/_completeness.json",
        }
        source_inputs = [
            {
                "key": "vehicle_type=bus/date=2026-10-09/hour=07/a.jsonl",
                "etag": '"test-etag"',
                "size": 123,
                "last_modified": "2026-10-09T07:00:05+00:00",
            }
        ]
        client = Mock()

        with patch("bronze_to_silver._s3_client", return_value=client):
            write_silver_active_manifest(
                "2026-10-09",
                "07",
                publication_id,
                artifacts,
                source_inputs=source_inputs,
            )

        manifest = json.loads(client.put_object.call_args.kwargs["Body"])
        self.assertEqual(manifest["source_inputs"], source_inputs)

    def test_write_silver_active_manifest_publishes_pointer(self):
        publication_id = "a" * 32
        partition = "date=2026-10-09/hour=07"
        artifacts = {
            "data": (
                f"s3://silver/_versions/{partition}/{publication_id}/data.parquet"
            ),
            "quarantine": (
                f"s3://silver/_versions/{partition}/{publication_id}/quarantine.parquet"
            ),
            "completeness": (
                f"s3://silver/_versions/{partition}/{publication_id}/_completeness.json"
            ),
        }
        client = Mock()

        with patch("bronze_to_silver._s3_client", return_value=client):
            write_silver_active_manifest("2026-10-09", "07", publication_id, artifacts)

        client.put_object.assert_called_once()
        args = client.put_object.call_args.kwargs
        self.assertEqual(args["Bucket"], "silver")
        self.assertEqual(
            args["Key"],
            "vehicle_positions/date=2026-10-09/hour=07/_active.json",
        )
        manifest = json.loads(args["Body"])
        self.assertEqual(manifest["status"], "published")
        self.assertEqual(manifest["partition"], partition)
        self.assertEqual(manifest["publication_id"], publication_id)
        self.assertEqual(manifest["artifacts"], artifacts)

    def test_legacy_manifest_does_not_fill_missing_utc_slot(self):
        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(
                ["complete", "complete"],
                ["2026-10-09T07:00:00+00:00", None],
            ),
            poll_interval_seconds=30,
            now_utc=datetime(2026, 10, 9, 7, 0, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(result["cycles"], 2)
        self.assertEqual(result["missing_cycles"], 1)
        self.assertEqual(result["status"], "incomplete")

    def test_complete_manifest_wins_over_partial_for_same_slot(self):
        for statuses in (
            ["complete", "partial"],
            ["partial", "complete"],
        ):
            with self.subTest(statuses=statuses):
                result = assess_completeness(
                    "2026-10-09",
                    "07",
                    self._client(
                        statuses,
                        [
                            "2026-10-09T07:00:00+00:00",
                            "2026-10-09T07:00:00+00:00",
                        ],
                    ),
                    poll_interval_seconds=30,
                    now_utc=datetime(2026, 10, 9, 7, 0, 15, tzinfo=timezone.utc),
                )

                self.assertEqual(result["expected_cycles"], 1)
                self.assertEqual(result["missing_cycles"], 0)
                self.assertEqual(result["status"], "complete")

    def test_invalid_utc_slot_does_not_increase_coverage(self):
        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(
                ["complete", "complete"],
                [
                    "2026-10-09T07:00:00+00:00",
                    "not-a-timestamp",
                ],
            ),
            poll_interval_seconds=30,
            now_utc=datetime(2026, 10, 9, 7, 0, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(result["missing_cycles"], 1)
        self.assertEqual(result["status"], "incomplete")

    def test_future_utc_slot_does_not_increase_coverage(self):
        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(
                ["complete", "complete"],
                [
                    "2026-10-09T07:00:00+00:00",
                    "2026-10-09T07:01:00+00:00",
                ],
            ),
            poll_interval_seconds=30,
            now_utc=datetime(2026, 10, 9, 7, 0, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(result["missing_cycles"], 1)
        self.assertEqual(result["status"], "incomplete")

    def test_duplicate_complete_manifests_do_not_inflate_complete_ratio(self):
        slot_starts = [
            f"2026-10-09T07:{minute:02d}:{second:02d}+00:00"
            for minute in range(57)
            for second in (0, 30)
        ]
        statuses = ["complete"] * 102 + ["partial"] * 12

        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(
                statuses + ["complete"] * 20,
                slot_starts + [slot_starts[0]] * 20,
            ),
            poll_interval_seconds=30,
            now_utc=datetime(2026, 10, 9, 8, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(result["missing_cycles"], 6)
        self.assertEqual(result["status"], "incomplete")

    def test_misaligned_utc_slot_does_not_increase_coverage(self):
        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(
                ["complete", "complete"],
                [
                    "2026-10-09T07:00:00+00:00",
                    "2026-10-09T07:00:15+00:00",
                ],
            ),
            poll_interval_seconds=30,
            now_utc=datetime(2026, 10, 9, 7, 0, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(result["missing_cycles"], 1)
        self.assertEqual(result["status"], "incomplete")

    def test_write_silver_active_manifest_rejects_invalid_artifact(self):
        publication_id = "a" * 32
        partition = "date=2026-10-09/hour=07"
        artifacts = {
            "data": (
                f"s3://silver/_versions/{partition}/{publication_id}/data.parquet"
            ),
            "quarantine": (
                f"s3://silver/_versions/{partition}/{'b' * 32}/quarantine.parquet"
            ),
            "completeness": (
                f"s3://silver/_versions/{partition}/{publication_id}/_completeness.json"
            ),
        }
        client = Mock()

        with (
            patch("bronze_to_silver._s3_client", return_value=client),
            self.assertRaises(ValueError),
        ):
            write_silver_active_manifest("2026-10-09", "07", publication_id, artifacts)

        client.put_object.assert_not_called()

    def test_write_silver_active_manifest_rejects_extra_path(self):
        publication_id = "a" * 32
        partition = "date=2026-10-09/hour=07"
        artifacts = {
            "data": (
                f"s3://silver/_versions/{partition}/"
                f"{publication_id}/nested/data.parquet"
            ),
            "quarantine": (
                f"s3://silver/_versions/{partition}/{publication_id}/quarantine.parquet"
            ),
            "completeness": (
                f"s3://silver/_versions/{partition}/{publication_id}/_completeness.json"
            ),
        }
        client = Mock()

        with (
            patch("bronze_to_silver._s3_client", return_value=client),
            self.assertRaises(ValueError),
        ):
            write_silver_active_manifest("2026-10-09", "07", publication_id, artifacts)

        client.put_object.assert_not_called()

    def test_active_silver_manifest_resolves_all_artifacts(self):
        manifest = {
            "publication_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "partition": "date=2026-10-09/hour=07",
            "status": "published",
            "artifacts": {
                "data": (
                    "s3://silver/_versions/date=2026-10-09/hour=07/"
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/data.parquet"
                ),
                "quarantine": (
                    "s3://silver/_versions/date=2026-10-09/hour=07/"
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/quarantine.parquet"
                ),
                "completeness": (
                    "s3://silver/_versions/date=2026-10-09/hour=07/"
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/_completeness.json"
                ),
            },
        }

        client = Mock()
        body = Mock()
        body.read.return_value = json.dumps(manifest).encode("utf-8")
        client.get_object.return_value = {"Body": body}

        result = resolve_silver_artifacts("2026-10-09", "07", client)

        self.assertEqual(result, manifest["artifacts"])
        client.get_object.assert_called_once_with(
            Bucket="silver",
            Key="vehicle_positions/date=2026-10-09/hour=07/_active.json",
        )

    def test_read_completeness_marker_uses_active_manifest(self):
        publication_id = "a" * 32
        partition = "date=2026-10-09/hour=07"
        manifest = {
            "publication_id": publication_id,
            "partition": partition,
            "status": "published",
            "artifacts": {
                "data": (
                    f"s3://silver/_versions/{partition}/{publication_id}/data.parquet"
                ),
                "quarantine": (
                    f"s3://silver/_versions/{partition}/"
                    f"{publication_id}/quarantine.parquet"
                ),
                "completeness": (
                    f"s3://silver/_versions/{partition}/{publication_id}/_completeness.json"
                ),
            },
        }
        client = Mock()
        manifest_body = Mock()
        manifest_body.read.return_value = json.dumps(manifest).encode("utf-8")
        marker_body = Mock()
        marker_body.read.return_value = json.dumps(
            {"status": "complete", "cycles": 10, "complete_cycles": 10}
        ).encode("utf-8")
        client.get_object.side_effect = [
            {"Body": manifest_body},
            {"Body": marker_body},
        ]

        result = read_completeness_marker("2026-10-09", "07", client)

        self.assertEqual(result["status"], "complete")
        self.assertEqual(client.get_object.call_count, 2)
        self.assertEqual(
            client.get_object.call_args_list[1].kwargs,
            {
                "Bucket": "silver",
                "Key": (f"_versions/{partition}/{publication_id}/_completeness.json"),
            },
        )

    def test_missing_active_manifest_uses_legacy_paths(self):
        client = Mock()
        client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "GetObject"
        )

        result = resolve_silver_artifacts("2026-10-09", "07", client)

        self.assertEqual(
            result,
            {
                "data": (
                    "s3://silver/vehicle_positions/date=2026-10-09/hour=07/data.parquet"
                ),
                "quarantine": (
                    "s3://silver/quarantine/vehicle_positions/"
                    "date=2026-10-09/hour=07/data.parquet"
                ),
                "completeness": (
                    "s3://silver/vehicle_positions/date=2026-10-09/"
                    "hour=07/_completeness.json"
                ),
            },
        )

    def test_invalid_active_manifest_does_not_fall_back(self):
        manifest = {
            "publication_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "partition": "date=2026-10-08/hour=07",
            "status": "published",
            "artifacts": {},
        }
        client = Mock()
        body = Mock()
        body.read.return_value = json.dumps(manifest).encode("utf-8")
        client.get_object.return_value = {"Body": body}

        with self.assertRaises(ValueError):
            resolve_silver_artifacts("2026-10-09", "07", client)

    def test_is_hour_complete_only_for_complete(self):
        for status, expected in (
            ("complete", True),
            ("incomplete", False),
            ("unknown", False),
        ):
            client = self._marker_client(status)
            with patch(
                "bronze_to_silver.resolve_silver_artifacts",
                return_value={
                    "completeness": (
                        "s3://silver/vehicle_positions/date=2026-10-09/"
                        "hour=07/_completeness.json"
                    )
                },
            ):
                self.assertIs(is_hour_complete("2026-10-09", "07", client), expected)

    def test_missing_marker_is_unknown_not_complete(self):
        client = self._marker_client(error_code="NoSuchKey")
        self.assertEqual(
            read_completeness_marker("2026-10-09", "07", client), {"status": "unknown"}
        )
        self.assertFalse(is_hour_complete("2026-10-09", "07", client))

    def test_other_storage_errors_propagate(self):
        client = self._marker_client(error_code="AccessDenied")
        with self.assertRaises(ClientError):
            is_hour_complete("2026-10-09", "07", client)

    def test_hour_before_first_bronze_is_pre_ingestion(self):
        client = self._listing(
            [
                "vehicle_type=bus/date=2026-10-10/hour=12/a.jsonl",
                "vehicle_type=tram/date=2026-10-10/hour=12/b.jsonl",
            ]
        )
        self.assertTrue(is_before_first_bronze_hour("2026-10-10", "11", client))

    def test_hour_after_first_bronze_is_a_gap(self):
        client = self._listing(
            [
                "vehicle_type=bus/date=2026-10-10/hour=12/a.jsonl",
                "vehicle_type=tram/date=2026-10-10/hour=12/b.jsonl",
            ]
        )
        self.assertFalse(is_before_first_bronze_hour("2026-10-10", "14", client))

    def test_empty_bronze_is_pre_ingestion(self):
        self.assertTrue(
            is_before_first_bronze_hour("2026-10-10", "14", self._listing([None, None]))
        )

    @staticmethod
    def _client(statuses, slot_starts=None):
        client = Mock()
        keys = [
            f"_manifests/date=2026-10-09/hour=07/c{i}.json"
            for i in range(len(statuses))
        ]
        client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": key} for key in keys]}
        ]
        bodies = {}
        for index, (key, status) in enumerate(zip(keys, statuses)):
            manifest = {"status": status}
            if slot_starts is not None:
                manifest["cycle_slot_start"] = slot_starts[index]
            bodies[key] = json.dumps(manifest).encode()

        client.get_object.side_effect = lambda Bucket, Key: {
            "Body": Mock(read=Mock(return_value=bodies[Key]))
        }
        return client

    def test_complete_when_all_cycles_have_both_sources(self):
        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(["complete"] * 10),
            poll_interval_seconds=360,
        )
        self.assertEqual(result["status"], "complete")

    def test_incomplete_when_many_cycles_partial(self):
        result = assess_completeness(
            "2026-10-09", "07", self._client(["complete", "partial", "partial"])
        )
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(result["partial_cycles"], 2)

    def test_unknown_without_manifests(self):
        client = Mock()
        client.get_paginator.return_value.paginate.return_value = [{}]
        result = assess_completeness("2026-10-09", "07", client)
        self.assertEqual(result["status"], "unknown")

    def test_missing_cycles_cannot_be_counted_as_complete(self):
        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(["complete"] * 113),
            poll_interval_seconds=30,
        )

        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(result["expected_cycles"], 120)
        self.assertEqual(result["cycles"], 113)
        self.assertEqual(result["complete_cycles"], 113)

    def test_incomplete_when_hour_has_too_few_cycles(self):
        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(["complete"] * 89),
            poll_interval_seconds=30,
        )

        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(result["expected_cycles"], 120)
        self.assertEqual(result["minimum_required_cycles"], 114)

    def test_incomplete_when_coverage_is_below_95_percent(self):
        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(["complete"] * 113),
            poll_interval_seconds=30,
        )

        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(result["expected_cycles"], 120)
        self.assertEqual(result["minimum_required_cycles"], 114)

    def test_complete_when_hour_meets_minimum_cycle_count(self):
        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(["complete"] * 114),
            poll_interval_seconds=30,
        )

        self.assertEqual(result["status"], "complete")

    def test_coverage_uses_expected_cycles_not_only_existing_manifests(self):
        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(["complete"] * 114),
            poll_interval_seconds=30,
        )

        self.assertEqual(result["expected_cycles"], 120)
        self.assertEqual(result["cycles"], 114)
        self.assertEqual(result["status"], "complete")

    def test_incomplete_when_complete_cycle_ratio_is_below_threshold(self):
        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(["complete"] * 102 + ["partial"] * 12),
            poll_interval_seconds=30,
        )

        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(result["expected_cycles"], 120)
        self.assertEqual(result["minimum_required_cycles"], 114)
        self.assertEqual(result["cycles"], 114)
        self.assertEqual(result["complete_cycles"], 102)
        self.assertEqual(result["partial_cycles"], 12)

    def test_ignores_utc_slots_outside_requested_hour(self):
        slot_starts = [
            "2026-10-09T06:59:30+00:00",
            "2026-10-09T07:00:00+00:00",
        ]

        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(["complete", "complete"], slot_starts),
            poll_interval_seconds=30,
        )

        self.assertEqual(result["cycles"], 2)
        self.assertEqual(result["missing_cycles"], 119)

    def test_duplicate_utc_slot_does_not_increase_coverage(self):
        slot_starts = [
            "2026-10-09T07:00:00+00:00",
            "2026-10-09T07:00:00+00:00",
        ]

        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(["complete", "complete"], slot_starts),
            poll_interval_seconds=30,
            now_utc=datetime(2026, 10, 9, 7, 0, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(result["cycles"], 2)
        self.assertEqual(result["missing_cycles"], 1)
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(result["observed_slots"], 1)
        self.assertEqual(result["complete_slots"], 1)
        self.assertEqual(result["slot_coverage_ratio"], 0.5)
        self.assertEqual(result["complete_slot_ratio"], 1.0)

    def test_reports_missing_utc_slot(self):
        slot_starts = [
            f"2026-10-09T07:{minute:02d}:{second:02d}+00:00"
            for minute in range(60)
            for second in (0, 30)
            if not (minute == 12 and second == 30)
        ]

        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(["complete"] * len(slot_starts), slot_starts),
            poll_interval_seconds=30,
        )

        self.assertEqual(result["cycles"], 119)
        self.assertEqual(result["missing_cycles"], 1)

    def test_reports_missing_cycles(self):
        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(["complete"] * 114),
            poll_interval_seconds=30,
        )

        self.assertEqual(result["expected_cycles"], 120)
        self.assertEqual(result["cycles"], 114)
        self.assertEqual(result["missing_cycles"], 6)

    def test_active_manifest_rejects_artifact_from_another_version(self):
        manifest = {
            "publication_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "partition": "date=2026-10-09/hour=07",
            "status": "published",
            "artifacts": {
                "data": (
                    "s3://silver/_versions/date=2026-10-09/hour=07/"
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/data.parquet"
                ),
                "quarantine": (
                    "s3://silver/_versions/date=2026-10-09/hour=07/"
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/quarantine.parquet"
                ),
                "completeness": (
                    "s3://silver/_versions/date=2026-10-09/hour=07/"
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/_completeness.json"
                ),
            },
        }
        client = Mock()
        body = Mock()
        body.read.return_value = json.dumps(manifest).encode("utf-8")
        client.get_object.return_value = {"Body": body}

        with self.assertRaises(ValueError):
            resolve_silver_artifacts("2026-10-09", "07", client)

    def test_active_manifest_rejects_artifact_with_extra_path(self):
        publication_id = "a" * 32
        partition = "date=2026-10-09/hour=07"
        manifest = {
            "publication_id": publication_id,
            "partition": partition,
            "status": "published",
            "artifacts": {
                "data": (
                    f"s3://silver/_versions/{partition}/{publication_id}/"
                    "nested/data.parquet"
                ),
                "quarantine": (
                    f"s3://silver/_versions/{partition}/{publication_id}/"
                    "quarantine.parquet"
                ),
                "completeness": (
                    f"s3://silver/_versions/{partition}/{publication_id}/"
                    "_completeness.json"
                ),
            },
        }
        client = Mock()
        body = Mock()
        body.read.return_value = json.dumps(manifest).encode("utf-8")
        client.get_object.return_value = {"Body": body}

        with self.assertRaises(ValueError):
            resolve_silver_artifacts("2026-10-09", "07", client)

    def test_complete_when_coverage_is_exactly_95_percent(self):
        result = assess_completeness(
            "2026-10-09",
            "07",
            self._client(["complete"] * 114),
            poll_interval_seconds=30,
        )

        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["expected_cycles"], 120)
        self.assertEqual(result["cycles"], 114)
        self.assertEqual(result["complete_cycles"], 114)


class BronzeToSilverSqlTests(unittest.TestCase):
    def setUp(self):
        self.bronze_path = (
            Path(__file__).parent / "fixtures" / "bronze_sample.jsonl"
        ).as_posix()
        self.connection = duckdb.connect()
        self.connection.execute("SET TimeZone = 'UTC';")

    def test_explicit_schema_preserves_hive_type_and_identifiers(self):
        record = {
            "VehicleNumber": "0012",
            "Lines": "007",
            "Brigade": "01",
            "Lat": 52.2,
            "Lon": 21.0,
            "Time": "2026-07-15 14:00:00",
            "_ingested_at": "2026-07-15T12:00:30+00:00",
        }
        invalid_record = {
            **record,
            "VehicleNumber": "0013",
            "Lat": "not-a-number",
        }

        with tempfile.TemporaryDirectory() as directory:
            path = (
                Path(directory)
                / "vehicle_type=bus"
                / "date=2026-07-15"
                / "hour=12"
                / "positions.jsonl"
            )
            path.parent.mkdir(parents=True)
            path.write_text(
                "\n".join(json.dumps(item) for item in (record, invalid_record)) + "\n",
                encoding="utf-8",
            )
            bronze_to_silver.materialize_bronze_inputs(
                self.connection,
                [path.as_posix()],
            )
            transform = bronze_to_silver.sql_from_materialized_input(TRANSFORM_SQL)
            rejected = bronze_to_silver.sql_from_materialized_input(REJECTED_SQL)
            rows = self.connection.execute(transform).fetchall()
            reasons = self.connection.execute(
                f"SELECT rejection_reason FROM ({rejected})"
            ).fetchall()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][:4], ("bus", "0012", "007", "01"))
        self.assertEqual(reasons, [("invalid_coordinates",)])

    def test_missing_required_fields_are_quarantined(self):
        valid_record = {
            "vehicle_type": "bus",
            "VehicleNumber": "0012",
            "Lines": "007",
            "Brigade": "01",
            "Lat": 52.2,
            "Lon": 21.0,
            "Time": "2026-07-15 14:00:00",
            "_ingested_at": "2026-07-15T12:00:30+00:00",
        }
        cases = (
            ("VehicleNumber", "missing_vehicle_number"),
            ("Lat", "invalid_coordinates"),
            ("Lon", "invalid_coordinates"),
            ("Time", "missing_or_invalid_timestamp"),
            ("_ingested_at", "missing_or_invalid_timestamp"),
        )

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "positions.jsonl"
            for field, expected_reason in cases:
                with self.subTest(field=field):
                    record = valid_record.copy()
                    record.pop(field)
                    path.write_text(
                        json.dumps(record) + "\n",
                        encoding="utf-8",
                    )
                    bronze_to_silver.materialize_bronze_inputs(
                        self.connection,
                        [path.as_posix()],
                    )
                    transform = bronze_to_silver.sql_from_materialized_input(
                        TRANSFORM_SQL
                    )
                    rejected = bronze_to_silver.sql_from_materialized_input(
                        REJECTED_SQL
                    )

                    self.assertEqual(
                        self.connection.execute(transform).fetchall(),
                        [],
                    )
                    reasons = self.connection.execute(
                        f"SELECT rejection_reason FROM ({rejected})"
                    ).fetchall()
                    self.assertEqual(reasons, [(expected_reason,)])

    def test_missing_optional_fields_are_loaded_as_null(self):
        record = {
            "vehicle_type": "bus",
            "VehicleNumber": "0012",
            "Lat": 52.2,
            "Lon": 21.0,
            "Time": "2026-07-15 14:00:00",
            "_ingested_at": "2026-07-15T12:00:30+00:00",
        }

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "positions.jsonl"
            path.write_text(
                json.dumps(record) + "\n",
                encoding="utf-8",
            )
            bronze_to_silver.materialize_bronze_inputs(
                self.connection,
                [path.as_posix()],
            )
            query = bronze_to_silver.sql_from_materialized_input(TRANSFORM_SQL)
            rows = self.connection.execute(query).fetchall()
            direct_rows = self.connection.execute(
                TRANSFORM_SQL.format(bronze_path=path.as_posix())
            ).fetchall()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], "0012")
        self.assertIsNone(rows[0][2])
        self.assertIsNone(rows[0][3])
        self.assertEqual(direct_rows, rows)

    def tearDown(self):
        self.connection.close()

    def test_quality_metrics_count_each_rejection_reason(self):
        query = QUALITY_SQL.format(bronze_path=self.bronze_path)

        metrics = self.connection.execute(query).fetchone()

        self.assertEqual(metrics, (11, 9, 9, 3, 1, 3, 1, 1, 1, 2, 1))

    def test_all_queries_can_use_materialized_input(self):
        bronze_to_silver.materialize_bronze_inputs(self.connection, [self.bronze_path])

        for template in (QUALITY_SQL, TRANSFORM_SQL, REJECTED_SQL):
            with self.subTest(template=template[:80]):
                expected = self.connection.execute(
                    template.format(bronze_path=self.bronze_path)
                ).fetchall()

                query = bronze_to_silver.sql_from_materialized_input(template)

                self.assertNotIn("read_json_auto", query)
                actual = self.connection.execute(query).fetchall()
                self.assertCountEqual(actual, expected)

    def test_materialized_input_is_unchanged_when_source_files_change(self):
        with tempfile.TemporaryDirectory() as directory:
            original = Path(directory) / "original.jsonl"
            original.write_text(
                json.dumps({"VehicleNumber": "initial"}) + "\n",
                encoding="utf-8",
            )

            bronze_to_silver.materialize_bronze_inputs(
                self.connection, [original.as_posix()]
            )

            original.write_text(
                json.dumps({"VehicleNumber": "changed"}) + "\n",
                encoding="utf-8",
            )
            late_file = Path(directory) / "late.jsonl"
            late_file.write_text(
                json.dumps({"VehicleNumber": "late"}) + "\n",
                encoding="utf-8",
            )

            rows = self.connection.execute(
                "SELECT VehicleNumber FROM bronze_input"
            ).fetchall()

        self.assertEqual(rows, [("initial",)])

    def test_transform_keeps_only_fresh_valid_unique_record(self):
        query = TRANSFORM_SQL.format(bronze_path=self.bronze_path)

        rows = self.connection.execute(query).fetchall()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], "valid")
        self.assertEqual(rows[0][4], 52.2)
        self.assertEqual(
            rows[0][7],
            datetime(2026, 7, 15, 12, 0, 30, tzinfo=timezone.utc),
        )

    def test_rejected_records_include_reason_and_duplicates(self):
        query = REJECTED_SQL.format(bronze_path=self.bronze_path)

        counts = dict(
            self.connection.execute(
                f"SELECT rejection_reason, COUNT(*) FROM ({query}) "
                "GROUP BY rejection_reason"
            ).fetchall()
        )

        self.assertEqual(counts["duplicate"], 2)
        self.assertEqual(counts["invalid_coordinates"], 2)
        self.assertEqual(counts["stale_gps_time"], 3)
        self.assertEqual(counts["future_gps_time"], 1)

    def test_local_dst_fold_and_gap_are_quarantined(self):
        records = [
            {
                "vehicle_type": "bus",
                "VehicleNumber": "fold",
                "Lines": "1",
                "Brigade": "1",
                "Lat": 52.2,
                "Lon": 21.0,
                "Time": "2026-10-25 02:30:00",
                "_ingested_at": "2026-10-25T00:31:00+00:00",
            },
            {
                "vehicle_type": "bus",
                "VehicleNumber": "gap",
                "Lines": "1",
                "Brigade": "1",
                "Lat": 52.2,
                "Lon": 21.0,
                "Time": "2026-03-29 02:30:00",
                "_ingested_at": "2026-03-29T01:31:00+00:00",
            },
            {
                "vehicle_type": "bus",
                "VehicleNumber": "",
                "Lines": "1",
                "Brigade": "1",
                "Lat": 52.2,
                "Lon": 21.0,
                "Time": "2026-07-15 14:00:00",
                "_ingested_at": "2026-07-15T12:00:30+00:00",
            },
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "dst.jsonl"
            path.write_text(
                "\n".join(json.dumps(record) for record in records), encoding="utf-8"
            )
            rows = self.connection.execute(
                REJECTED_SQL.format(bronze_path=path.as_posix())
            ).fetchall()

        reasons = {row[-1] for row in rows}
        self.assertEqual(
            reasons,
            {
                "ambiguous_local_time",
                "nonexistent_local_time",
                "missing_vehicle_number",
            },
        )


class TransformHourTests(unittest.TestCase):
    def setUp(self):
        self.completeness = {"status": "complete", "cycles": 10, "complete_cycles": 10}
        self.assess_mock = patch(
            "bronze_to_silver.assess_completeness", return_value=self.completeness
        ).start()
        self.write_marker_mock = patch(
            "bronze_to_silver.write_completeness_marker"
        ).start()
        self.source_inputs = [
            {
                "key": "vehicle_type=bus/date=2026-10-09/hour=07/a.jsonl",
                "etag": '"test-etag"',
                "size": 123,
                "last_modified": "2026-10-09T07:00:05+00:00",
            }
        ]
        patch(
            "bronze_to_silver.capture_bronze_inputs",
            return_value=self.source_inputs,
        ).start()
        patch("bronze_to_silver.materialize_bronze_inputs").start()
        self.addCleanup(patch.stopall)

    @staticmethod
    def _happy_path_connection():
        connection = Mock()
        connection.execute.return_value.fetchone.side_effect = [
            ("bronze-file.jsonl",),
            (1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1),
            (0,),
            (1,),
        ]
        return connection

    def test_fails_when_bronze_partition_has_no_files(self):
        connection = Mock()
        connection.execute.return_value.fetchone.return_value = None

        with self.assertRaises(BronzeDataMissingError):
            transform_hour(connection, "2026-10-09", "7")

        connection.execute.assert_called_once_with(
            "SELECT file FROM glob("
            "'s3://bronze/vehicle_type=*/date=2026-10-09/hour=07/*.jsonl') LIMIT 1"
        )

    def test_transform_records_inputs_captured_before_reading_data(self):
        connection = self._happy_path_connection()
        source_inputs = [
            {
                "key": "vehicle_type=bus/date=2026-10-09/hour=07/a.jsonl",
                "etag": '"test-etag"',
                "size": 123,
                "last_modified": "2026-10-09T07:00:05+00:00",
            }
        ]

        def capture_inputs(*args, **kwargs):
            for statement in connection.execute.call_args_list:
                self.assertNotIn("read_json_auto", statement.args[0])
            return source_inputs

        with (
            patch(
                "bronze_to_silver.capture_bronze_inputs",
                side_effect=capture_inputs,
            ) as capture,
            patch("bronze_to_silver.validate_silver_schema"),
            patch("bronze_to_silver.copy_s3_object"),
            patch("bronze_to_silver.delete_s3_object"),
            patch("bronze_to_silver.write_silver_active_manifest") as publish,
            patch("bronze_to_silver.materialize_bronze_inputs") as materialize,
        ):
            transform_hour(connection, "2026-10-09", "7")

        capture.assert_called_once()
        self.assertEqual(publish.call_args.kwargs["source_inputs"], source_inputs)
        materialize.assert_called_once_with(
            connection,
            ["s3://bronze/vehicle_type=bus/date=2026-10-09/hour=07/a.jsonl"],
        )
        for statement in connection.execute.call_args_list:
            self.assertNotIn("read_json_auto", statement.args[0])

    def test_propagates_bronze_discovery_io_error(self):
        connection = Mock()
        connection.execute.side_effect = duckdb.IOException("MinIO unavailable")

        with self.assertRaises(duckdb.IOException):
            transform_hour(connection, "2026-10-09", "7")

    def test_publishes_validated_temp_file_with_server_side_copy(self):
        connection = self._happy_path_connection()

        with (
            patch("bronze_to_silver.validate_silver_schema"),
            patch("bronze_to_silver.copy_s3_object") as copy_mock,
            patch("bronze_to_silver.delete_s3_object") as delete_mock,
            patch("bronze_to_silver.write_silver_active_manifest") as manifest_mock,
        ):
            transform_hour(connection, "2026-10-09", "7")

        copy_statements = [
            call.args[0]
            for call in connection.execute.call_args_list
            if call.args[0].startswith("COPY ")
        ]
        self.assertEqual(len(copy_statements), 2)

        rejected_source, rejected_destination = copy_mock.call_args_list[0].args
        data_source, data_destination = copy_mock.call_args_list[1].args

        self.assertTrue(rejected_source.endswith(".parquet.pending"))
        self.assertTrue(data_source.endswith(".parquet.pending"))

        version_prefix = "s3://silver/_versions/date=2026-10-09/hour=07/"
        self.assertTrue(rejected_destination.startswith(version_prefix))
        self.assertTrue(rejected_destination.endswith("/quarantine.parquet"))
        self.assertTrue(data_destination.startswith(version_prefix))
        self.assertTrue(data_destination.endswith("/data.parquet"))

        publication_id = data_destination[len(version_prefix) :].split("/")[0]
        self.assertEqual(len(publication_id), 32)
        self.assertTrue(all(char in "0123456789abcdef" for char in publication_id))
        self.assertEqual(
            rejected_destination,
            f"{version_prefix}{publication_id}/quarantine.parquet",
        )

        delete_mock.assert_any_call(data_source)
        delete_mock.assert_any_call(rejected_source)
        self.assertEqual(delete_mock.call_count, 2)

        self.assess_mock.assert_called_once_with(
            "2026-10-09", "07", source_inputs=self.source_inputs
        )
        self.write_marker_mock.assert_called_once_with(
            self.completeness,
            f"{version_prefix}{publication_id}",
        )
        manifest_mock.assert_called_once_with(
            "2026-10-09",
            "07",
            publication_id,
            {
                "data": data_destination,
                "quarantine": rejected_destination,
                "completeness": (
                    f"{version_prefix}{publication_id}/_completeness.json"
                ),
            },
            source_inputs=self.source_inputs,
        )

    def test_warns_and_still_publishes_when_hour_is_incomplete(self):
        self.assess_mock.return_value = {
            "status": "incomplete",
            "cycles": 10,
            "complete_cycles": 5,
        }
        connection = self._happy_path_connection()

        with (
            patch("bronze_to_silver.validate_silver_schema"),
            patch("bronze_to_silver.copy_s3_object") as copy_mock,
            patch("bronze_to_silver.delete_s3_object"),
            patch("bronze_to_silver.write_silver_active_manifest"),
            self.assertLogs("bronze_to_silver", level="WARNING") as logs,
        ):
            transform_hour(connection, "2026-10-09", "7")

        self.assertEqual(copy_mock.call_count, 2)
        self.write_marker_mock.assert_called_once()
        self.assertTrue(any("incomplete" in line for line in logs.output))

    def test_publishes_rejected_rows_to_quarantine(self):
        connection = Mock()
        connection.execute.return_value.fetchone.side_effect = [
            ("bronze-file.jsonl",),
            (2, 2, 2, 1, 0, 1, 0, 0, 1, 0, 1),
            (1,),
            (1,),
        ]

        with (
            patch("bronze_to_silver.validate_silver_schema"),
            patch("bronze_to_silver.copy_s3_object") as copy_mock,
            patch("bronze_to_silver.delete_s3_object"),
            patch("bronze_to_silver.write_silver_active_manifest") as manifest_mock,
        ):
            transform_hour(connection, "2026-10-09", "7")

        destinations = [call.args[1] for call in copy_mock.call_args_list]
        self.assertEqual(len(destinations), 2)

        version_prefix = "s3://silver/_versions/date=2026-10-09/hour=07/"
        rejected_destination, data_destination = destinations

        self.assertTrue(rejected_destination.startswith(version_prefix))
        self.assertTrue(rejected_destination.endswith("/quarantine.parquet"))
        self.assertTrue(data_destination.startswith(version_prefix))
        self.assertTrue(data_destination.endswith("/data.parquet"))

        publication_id = data_destination[len(version_prefix) :].split("/")[0]
        self.assertEqual(len(publication_id), 32)
        self.assertTrue(all(char in "0123456789abcdef" for char in publication_id))
        self.assertEqual(
            rejected_destination,
            f"{version_prefix}{publication_id}/quarantine.parquet",
        )
        manifest_mock.assert_called_once()

    def test_second_publish_copy_failure_propagates_after_first_copy(self):
        connection = self._happy_path_connection()
        copy_calls = []

        def fail_on_second_copy(source, destination):
            copy_calls.append((source, destination))
            if len(copy_calls) == 2:
                raise RuntimeError("forced second publish failure")

        with (
            patch("bronze_to_silver.validate_silver_schema"),
            patch(
                "bronze_to_silver.copy_s3_object",
                side_effect=fail_on_second_copy,
            ),
            patch("bronze_to_silver.delete_s3_object"),
            patch("bronze_to_silver.write_silver_active_manifest") as manifest_mock,
            self.assertRaisesRegex(RuntimeError, "forced second publish failure"),
        ):
            transform_hour(connection, "2026-10-09", "7")

        self.assertEqual(len(copy_calls), 2)

        version_prefix = "s3://silver/_versions/date=2026-10-09/hour=07/"
        rejected_destination = copy_calls[0][1]
        data_destination = copy_calls[1][1]

        self.assertTrue(rejected_destination.startswith(version_prefix))
        self.assertTrue(rejected_destination.endswith("/quarantine.parquet"))
        self.assertTrue(data_destination.startswith(version_prefix))
        self.assertTrue(data_destination.endswith("/data.parquet"))

        manifest_mock.assert_not_called()
        self.write_marker_mock.assert_not_called()

    def test_overwrite_false_refuses_to_replace_existing_output(self):
        connection = self._happy_path_connection()

        with (
            patch("bronze_to_silver.validate_silver_schema"),
            patch("bronze_to_silver.s3_object_exists", return_value=True),
            patch("bronze_to_silver.copy_s3_object") as copy_mock,
            patch("bronze_to_silver.delete_s3_object") as delete_mock,
            patch(
                "bronze_to_silver.resolve_silver_artifacts",
                return_value={
                    "data": "s3://silver/vehicle_positions/date=2026-10-09/hour=07/data.parquet",
                    "quarantine": "s3://silver/vehicle_positions/date=2026-10-09/hour=07/quarantine.parquet",
                    "completeness": "s3://silver/vehicle_positions/date=2026-10-09/hour=07/_completeness.json",
                },
            ),
            self.assertRaises(FileExistsError),
        ):
            transform_hour(connection, "2026-10-09", "7", overwrite=False)

        copy_mock.assert_not_called()
        delete_mock.assert_not_called()

    def test_does_not_publish_when_row_count_differs_from_quality_count(self):
        connection = Mock()
        connection.execute.return_value.fetchone.side_effect = [
            ("bronze-file.jsonl",),
            (1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1),
            (0,),
            (0,),
        ]

        with (
            patch("bronze_to_silver.validate_silver_schema"),
            patch("bronze_to_silver.copy_s3_object") as copy_mock,
            patch("bronze_to_silver.delete_s3_object") as delete_mock,
            self.assertRaisesRegex(ValueError, "refusing to publish"),
        ):
            transform_hour(connection, "2026-10-09", "7")

        copy_mock.assert_not_called()
        self.assertEqual(delete_mock.call_count, 2)

    def test_does_not_publish_when_schema_validation_fails(self):
        connection = self._happy_path_connection()

        with (
            patch(
                "bronze_to_silver.validate_silver_schema",
                side_effect=ValueError("bad schema"),
            ),
            patch("bronze_to_silver.copy_s3_object") as copy_mock,
            patch("bronze_to_silver.delete_s3_object") as delete_mock,
            self.assertRaisesRegex(ValueError, "bad schema"),
        ):
            transform_hour(connection, "2026-10-09", "7")

        copy_mock.assert_not_called()
        self.assertEqual(delete_mock.call_count, 2)

    def test_transform_propagates_write_error_and_still_cleans_temp_file(self):
        connection = Mock()
        connection.execute.side_effect = [
            Mock(fetchone=Mock(return_value=("bronze-file.jsonl",))),
            Mock(fetchone=Mock(return_value=(1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1))),
            RuntimeError("write failed"),
        ]

        with (
            patch("bronze_to_silver.copy_s3_object") as copy_mock,
            patch("bronze_to_silver.delete_s3_object") as delete_mock,
            self.assertRaisesRegex(RuntimeError, "write failed"),
        ):
            transform_hour(connection, "2026-10-09", "7")

        copy_mock.assert_not_called()
        self.assertEqual(delete_mock.call_count, 2)


class ConnectionConfigTests(unittest.TestCase):
    def test_parse_s3_endpoint(self):
        self.assertEqual(parse_s3_endpoint("http://minio:9000"), ("minio:9000", False))
        self.assertEqual(
            parse_s3_endpoint("https://s3.example.com"), ("s3.example.com", True)
        )
        self.assertEqual(parse_s3_endpoint("localhost:9000"), ("localhost:9000", False))
        with self.assertRaises(ValueError):
            parse_s3_endpoint("ftp://x")

    def test_close_failure_preserves_configuration_failure(self):
        env = {
            "MINIO_USER": "test-user",
            "MINIO_PASSWORD": "test-password",
            "S3_ENDPOINT_URL": "http://minio:9000",
        }
        con = Mock()
        setup_error = duckdb.Error("Simulated configuration failure")
        con.execute.side_effect = setup_error
        con.close.side_effect = duckdb.Error("Simulated close failure")

        with (
            patch.dict(os.environ, env, clear=True),
            patch("bronze_to_silver.duckdb.connect", return_value=con),
            self.assertLogs(bronze_to_silver.logger, level="ERROR"),
            self.assertRaises(duckdb.Error) as raised,
        ):
            get_connection()

        self.assertIs(raised.exception, setup_error)
        con.close.assert_called_once_with()

    def test_configuration_failure_closes_connection(self):
        env = {
            "MINIO_USER": "test-user",
            "MINIO_PASSWORD": "test-password",
            "S3_ENDPOINT_URL": "http://minio:9000",
        }
        con = Mock()
        setup_error = duckdb.Error("Simulated configuration failure")
        con.execute.side_effect = setup_error

        with (
            patch.dict(os.environ, env, clear=True),
            patch("bronze_to_silver.duckdb.connect", return_value=con),
            self.assertRaises(duckdb.Error) as raised,
        ):
            get_connection()

        self.assertIs(raised.exception, setup_error)
        con.close.assert_called_once_with()

    def test_get_connection_requires_credentials(self):
        with (
            patch.dict(os.environ, {}, clear=True),
            self.assertRaises(RuntimeError),
        ):
            get_connection()

    def test_get_connection_escapes_secrets(self):
        env = {
            "MINIO_USER": "u'ser",
            "MINIO_PASSWORD": "p'w",
            "S3_ENDPOINT_URL": "https://s3.example.com",
        }
        con = Mock()
        with (
            patch.dict(os.environ, env, clear=True),
            patch("bronze_to_silver.duckdb.connect", return_value=con),
        ):
            get_connection()

        (sql,) = con.execute.call_args_list[-1].args
        self.assertIn("KEY_ID 'u''ser'", sql)
        self.assertIn("SECRET 'p''w'", sql)
        self.assertIn("ENDPOINT 's3.example.com'", sql)
        self.assertIn("USE_SSL true", sql)


class CommandLineTests(unittest.TestCase):
    def test_main_closes_connection_after_transform(self):
        connection = Mock()
        with (
            patch("bronze_to_silver.get_connection", return_value=connection),
            patch("bronze_to_silver.transform_hour") as transform,
            patch.object(
                sys,
                "argv",
                ["bronze_to_silver.py", "--date", "2026-10-09", "--hour", "07"],
            ),
        ):
            main()

        transform.assert_called_once_with(connection, "2026-10-09", "07")
        connection.close.assert_called_once()

    def test_close_failure_after_success_is_reported(self):
        connection = Mock()
        close_error = duckdb.Error("Simulated close failure")
        connection.close.side_effect = close_error

        with (
            patch(
                "bronze_to_silver.get_connection",
                return_value=connection,
            ),
            patch("bronze_to_silver.transform_hour") as transform,
            patch.object(
                sys,
                "argv",
                ["bronze_to_silver.py", "--date", "2026-10-09", "--hour", "07"],
            ),
            self.assertLogs(bronze_to_silver.logger, level="ERROR"),
            self.assertRaises(duckdb.Error) as raised,
        ):
            main()

        self.assertIs(raised.exception, close_error)
        transform.assert_called_once_with(connection, "2026-10-09", "07")
        connection.close.assert_called_once_with()

    def test_close_failure_does_not_hide_transform_failure(self):
        connection = Mock()
        transform_error = ValueError("Simulated transform failure")
        connection.close.side_effect = duckdb.Error("Simulated close failure")

        with (
            patch(
                "bronze_to_silver.get_connection",
                return_value=connection,
            ),
            patch(
                "bronze_to_silver.transform_hour",
                side_effect=transform_error,
            ),
            patch.object(
                sys,
                "argv",
                ["bronze_to_silver.py", "--date", "2026-10-09", "--hour", "07"],
            ),
            self.assertRaises(ValueError) as raised,
        ):
            main()

        self.assertIs(raised.exception, transform_error)
        connection.close.assert_called_once_with()


class SilverSchemaTests(unittest.TestCase):
    def setUp(self):
        self.connection = duckdb.connect()
        self.connection.execute("SET TimeZone = 'UTC';")
        self.tmp = tempfile.TemporaryDirectory()
        self.path = (Path(self.tmp.name) / "silver.parquet").as_posix()

    def tearDown(self):
        self.connection.close()
        self.tmp.cleanup()

    def _write(self, select_sql):
        self.connection.execute(
            f"COPY ({select_sql}) TO '{self.path}' (FORMAT parquet)"
        )

    def test_accepts_output_of_transform_sql(self):
        bronze = (Path(__file__).parent / "fixtures" / "bronze_sample.jsonl").as_posix()
        self._write(TRANSFORM_SQL.format(bronze_path=bronze))

        validate_silver_schema(self.connection, self.path)

    def test_rejects_missing_columns(self):
        self._write("SELECT 'bus' AS vehicle_type")

        with self.assertRaisesRegex(ValueError, "missing columns"):
            validate_silver_schema(self.connection, self.path)

    def test_rejects_wrong_column_types(self):
        self._write(
            "SELECT 'bus' AS vehicle_type, '1' AS vehicle_number, '1' AS line, "
            "'1' AS brigade, '52.2' AS lat, '21.0' AS lon, "
            "'2026-01-01' AS gps_time_utc, '2026-01-01' AS ingested_at"
        )

        with self.assertRaisesRegex(ValueError, "unexpected column types"):
            validate_silver_schema(self.connection, self.path)

    def test_rejects_unreadable_file(self):
        with self.assertRaisesRegex(ValueError, "schema validation failed"):
            validate_silver_schema(self.connection, self.path + ".missing")


class SilverS3HelperTests(unittest.TestCase):
    def test_copy_s3_object_uses_server_side_copy(self):
        s3_client = Mock()
        with patch("bronze_to_silver._s3_client", return_value=s3_client):
            copy_s3_object(
                "s3://silver/a/_staging/x.pending", "s3://silver/a/data.parquet"
            )

        s3_client.copy_object.assert_called_once_with(
            Bucket="silver",
            Key="a/data.parquet",
            CopySource={"Bucket": "silver", "Key": "a/_staging/x.pending"},
        )

    def test_s3_object_exists_uses_supplied_client(self):
        s3 = Mock()

        with patch("bronze_to_silver._s3_client") as client_factory:
            exists = s3_object_exists(
                "s3://silver/a/data.parquet",
                s3_client=s3,
            )

        self.assertTrue(exists)
        s3.head_object.assert_called_once_with(
            Bucket="silver",
            Key="a/data.parquet",
        )
        client_factory.assert_not_called()

    def test_s3_object_exists_handles_missing_and_present(self):
        s3_client = Mock()
        with patch("bronze_to_silver._s3_client", return_value=s3_client):
            self.assertTrue(s3_object_exists("s3://silver/a/data.parquet"))
            s3_client.head_object.side_effect = ClientError(
                {
                    "Error": {"Code": "404"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "HeadObject",
            )
            self.assertFalse(s3_object_exists("s3://silver/a/data.parquet"))

    def test_s3_object_exists_propagates_other_errors(self):
        s3_client = Mock()
        s3_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "403"}, "ResponseMetadata": {"HTTPStatusCode": 403}},
            "HeadObject",
        )
        with (
            patch("bronze_to_silver._s3_client", return_value=s3_client),
            self.assertRaises(ClientError),
        ):
            s3_object_exists("s3://silver/a/data.parquet")


class SilverTempObjectTests(unittest.TestCase):
    def test_delete_s3_object_uses_configured_minio_client(self):
        s3_client = Mock()
        with (
            patch.dict(
                os.environ,
                {
                    "S3_ENDPOINT_URL": "http://minio:9000",
                    "MINIO_USER": "test-user",
                    "MINIO_PASSWORD": "test-password",
                },
            ),
            patch("bronze_to_silver.boto3.client", return_value=s3_client) as client,
        ):
            delete_s3_object(
                "s3://silver/vehicle_positions/_staging/test.parquet.pending"
            )

        client.assert_called_once_with(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="test-user",
            aws_secret_access_key="test-password",
        )
        s3_client.delete_object.assert_called_once_with(
            Bucket="silver",
            Key="vehicle_positions/_staging/test.parquet.pending",
        )

    def test_delete_s3_object_rejects_non_s3_uri(self):
        with self.assertRaises(ValueError):
            delete_s3_object("file:///tmp/test.parquet")


class PartitionValidationTests(unittest.TestCase):
    def setUp(self):
        self.connection = duckdb.connect()
        self.connection.execute("SET TimeZone = 'UTC';")

    def tearDown(self):
        self.connection.close()

    def test_normalizes_single_digit_hour(self):
        result = validate_partition("2026-10-09", "7")

        self.assertEqual(result, ("2026-10-09", "07"))

    def test_accepts_hour_boundaries(self):
        self.assertEqual(
            validate_partition("2026-10-09", "0"),
            ("2026-10-09", "00"),
        )
        self.assertEqual(
            validate_partition("2026-10-09", "23"),
            ("2026-10-09", "23"),
        )

    def test_rejects_invalid_dates(self):
        invalid_dates = ("2026-02-30", "2026-1-09", "2026-10-09/../../x")
        for date_str in invalid_dates:
            with self.subTest(date_str=date_str), self.assertRaises(ValueError):
                validate_partition(date_str, "7")

    def test_rejects_invalid_hours(self):
        invalid_hours = ("-1", "24", "abc", "123")
        for hour_str in invalid_hours:
            with self.subTest(hour_str=hour_str), self.assertRaises(ValueError):
                validate_partition("2026-10-09", hour_str)


if __name__ == "__main__":
    unittest.main()
