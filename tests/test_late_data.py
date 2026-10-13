import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock, patch

from botocore.exceptions import ClientError

import late_data

NOW = datetime(2026, 1, 15, 13, 40, tzinfo=timezone.utc)
T0 = datetime(2026, 1, 15, 12, 30, tzinfo=timezone.utc)



class FakeS3:
    """In-memory S3 fake supporting object listing and manifest reads."""

    def __init__(self, objects):
        self.objects = objects

    def get_paginator(self, _name):
        outer = self

        class Paginator:
            def paginate(self, Bucket, Prefix):
                items = [
                    {"Key": key, "LastModified": modified}
                    for (bucket, key), modified in outer.objects.items()
                    if bucket == Bucket and key.startswith(Prefix)
                ]
                yield {"Contents": items} if items else {}

        return Paginator()


    def list_objects_v2(self, Bucket, Prefix):
        items = [
            {"Key": key, "LastModified": modified}
            for (bucket, key), modified in self.objects.items()
            if bucket == Bucket and key.startswith(Prefix)
        ]
        return {"Contents": items} if items else {}


    def get_object(self, Bucket, Key):
        value = self.objects.get((Bucket, Key))
        if value is None:
            error = ClientError(
                {
                    "Error": {"Code": "NoSuchKey", "Message": "Not found"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "GetObject",
            )
            raise error

        if isinstance(value, bytes):
            body = value
        elif isinstance(value, str):
            body = value.encode("utf-8")
        elif isinstance(value, dict):
            body = json.dumps(value).encode("utf-8")
        else:
            raise TypeError(f"Unsupported fake S3 object value: {type(value)}")

        return {"Body": Mock(read=Mock(return_value=body))}


def bronze(hour, when, label="bus"):
    return (
        ("bronze", f"vehicle_type={label}/date=2026-01-15/hour={hour}/a.jsonl"),
        when,
    )


def silver(hour, when):
    return (
        ("silver", f"vehicle_positions/date=2026-01-15/hour={hour}/data.parquet"),
        when,
    )


def silver_completeness(hour, when):
    return (
        (
            "silver",
            f"vehicle_positions/date=2026-01-15/hour={hour}/_completeness.json",
        ),
        when,
    )


def silver_quarantine(hour, when):
    return (
        (
            "silver",
            f"quarantine/vehicle_positions/date=2026-01-15/hour={hour}/data.parquet",
        ),
        when,
    )


class StaleHourTests(unittest.TestCase):
    def stale(self, *items, window=3):
        return late_data.find_stale_hours(NOW, window, FakeS3(dict(items)))

    def test_late_bronze_after_silver_is_stale(self):
        result = self.stale(
            bronze("12", T0 + timedelta(minutes=40)),
            silver("12", T0 + timedelta(minutes=35)),
        )
        self.assertEqual(result, [("2026-01-15", "12")])

    def test_silver_newer_than_bronze_is_not_stale(self):
        result = self.stale(
            bronze("12", T0),
            silver("12", T0 + timedelta(minutes=35)),
            silver_completeness("12", T0 + timedelta(minutes=35)),
            silver_quarantine("12", T0 + timedelta(minutes=35)),
        )
        self.assertEqual(result, [])

    def test_missing_silver_with_bronze_is_stale_gap_repair(self):
        self.assertEqual(self.stale(bronze("11", T0)), [("2026-01-15", "11")])

    def test_late_tram_data_counts(self):
        result = self.stale(
            bronze("12", T0),
            bronze("12", T0 + timedelta(minutes=50), "tram"),
            silver("12", T0 + timedelta(minutes=35)),
        )
        self.assertEqual(result, [("2026-01-15", "12")])

    def test_hour_without_bronze_is_ignored(self):
        self.assertEqual(self.stale(), [])

    def test_current_open_hour_is_never_recomputed(self):
        self.assertEqual(self.stale(bronze("13", T0)), [])

    def test_oldest_hour_inside_window_is_included(self):
        result = self.stale(
            bronze("10", T0),
            window=3,
        )
        self.assertEqual(result, [("2026-01-15", "10")])

    def test_hours_outside_window_are_frozen(self):
        self.assertEqual(self.stale(bronze("08", T0), window=3), [])

    def test_missing_completeness_marker_is_stale(self):
        result = self.stale(
            bronze("12", T0),
            silver("12", T0 + timedelta(minutes=35)),
            silver_quarantine("12", T0 + timedelta(minutes=35)),
        )
        self.assertEqual(result, [("2026-01-15", "12")])

    def test_missing_quarantine_file_is_stale(self):
        result = self.stale(
            bronze("12", T0),
            silver("12", T0 + timedelta(minutes=35)),
            silver_completeness("12", T0 + timedelta(minutes=35)),
        )
        self.assertEqual(result, [("2026-01-15", "12")])

    def test_similarly_named_silver_object_does_not_count(self):
        objects = dict(
            [
                bronze("12", T0),
                silver_completeness("12", T0 + timedelta(minutes=35)),
                silver_quarantine("12", T0 + timedelta(minutes=35)),
            ]
        )
        objects[
            (
                "silver",
                "vehicle_positions/date=2026-01-15/hour=12/data.parquet.backup",
            )
        ] = T0 + timedelta(minutes=35)

        result = self.stale(*objects.items())
        self.assertEqual(result, [("2026-01-15", "12")])


    def test_manifest_storage_error_is_propagated(self):
        objects = dict([bronze("12", T0)])
        s3 = FakeS3(objects)

        error = ClientError(
            {
                "Error": {
                    "Code": "InternalError",
                    "Message": "Storage unavailable",
                },
                "ResponseMetadata": {"HTTPStatusCode": 500},
            },
            "GetObject",
        )

        with patch(
            "late_data.resolve_silver_artifacts",
            side_effect=error,
        ), self.assertRaises(ClientError):
            late_data.is_hour_stale("2026-01-15", "12", s3)


    def test_active_manifest_artifacts_are_checked(self):
        publication_id = "a" * 32
        partition = "date=2026-01-15/hour=12"
        version_prefix = (
            f"_versions/{partition}/{publication_id}/"
        )
        manifest = {
            "publication_id": publication_id,
            "partition": partition,
            "status": "published",
            "artifacts": {
                "data": f"s3://silver/{version_prefix}data.parquet",
                "quarantine": (
                    f"s3://silver/{version_prefix}quarantine.parquet"
                ),
                "completeness": (
                    f"s3://silver/{version_prefix}_completeness.json"
                ),
            },
        }
        objects: dict[tuple[str, str], datetime | dict] = dict([bronze("12", T0)])
        manifest_key = "vehicle_positions/date=2026-01-15/hour=12/_active.json"
        objects[("silver", manifest_key)] = manifest
        objects[("silver", f"{version_prefix}data.parquet")] = T0
        objects[("silver", f"{version_prefix}quarantine.parquet")] = T0
        objects[("silver", f"{version_prefix}_completeness.json")] = T0

        result = late_data.is_hour_stale(
            "2026-01-15", "12", FakeS3(objects)
        )

        self.assertFalse(result)



class ReprocessTests(unittest.TestCase):
    def test_nothing_stale_does_not_open_connection(self):
        with patch.object(late_data, "find_stale_hours", return_value=[]):
            result = late_data.reprocess_late_data(con=None)
        self.assertEqual(result["recomputed"], [])

    def test_recomputes_all_and_raises_after_failures(self):
        calls = []

        def fake(con, date_str, hour_str, run_at=None):
            calls.append(hour_str)
            if hour_str == "10":
                raise ValueError("boom")
            return {"status": "complete"}

        with (
            patch.object(
                late_data,
                "find_stale_hours",
                return_value=[("2026-01-15", "10"), ("2026-01-15", "11")],
            ),
            patch.object(late_data, "recompute_hour", side_effect=fake),
            self.assertRaises(RuntimeError),
        ):
            late_data.reprocess_late_data(con=MagicMock())
        self.assertEqual(calls, ["10", "11"])


if __name__ == "__main__":
    unittest.main()
