import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock, call, patch

from botocore.exceptions import ClientError

import bronze_to_silver
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


class BronzeInputSnapshotTests(unittest.TestCase):
    def test_new_manifest_changes_input_snapshot(self):
        s3 = FakeS3(dict([bronze("12", T0)]))
        before = bronze_to_silver.capture_bronze_inputs("2026-01-15", "12", s3)

        s3.objects[("bronze", "_manifests/date=2026-01-15/hour=12/new.json")] = (
            T0 + timedelta(minutes=1)
        )

        after = bronze_to_silver.capture_bronze_inputs("2026-01-15", "12", s3)

        self.assertNotEqual(before, after)


class StaleHourTests(unittest.TestCase):
    def stale(self, *items, window=3):
        return late_data.find_stale_hours(NOW, window, FakeS3(dict(items)))

    def test_late_bronze_after_silver_is_stale(self):
        result = self.stale(
            bronze("12", T0 + timedelta(minutes=40)),
            silver("12", T0 + timedelta(minutes=35)),
        )
        self.assertEqual(result, [("2026-01-15", "12")])

    def test_input_change_before_silver_publication_requires_rebuild(self):
        partition = "date=2026-01-15/hour=12"
        publication_id = "a" * 32
        version_prefix = f"_versions/{partition}/{publication_id}/"

        for new_key in (
            None,
            f"vehicle_type=tram/{partition}/late.jsonl",
            f"_manifests/{partition}/late.json",
        ):
            with self.subTest(new_key=new_key):
                objects = dict([bronze("12", T0)])
                s3 = FakeS3(objects)
                captured = bronze_to_silver.capture_bronze_inputs(
                    "2026-01-15", "12", s3
                )
                artifacts = {
                    "data": f"s3://silver/{version_prefix}data.parquet",
                    "quarantine": (f"s3://silver/{version_prefix}quarantine.parquet"),
                    "completeness": (f"s3://silver/{version_prefix}_completeness.json"),
                }

                objects[("silver", f"vehicle_positions/{partition}/_active.json")] = {
                    "publication_id": publication_id,
                    "partition": partition,
                    "status": "published",
                    "artifacts": artifacts,
                    "source_inputs": captured,
                }

                for uri in artifacts.values():
                    key = uri.removeprefix("s3://silver/")
                    objects[("silver", key)] = T0 + timedelta(minutes=35)

                if new_key is not None:
                    objects[("bronze", new_key)] = T0 + timedelta(minutes=1)

                self.assertEqual(
                    late_data.is_hour_stale("2026-01-15", "12", s3),
                    new_key is not None,
                )

    def test_fresh_silver_and_matching_gold_do_not_require_repair(self):
        result = self.stale(
            bronze("12", T0),
            silver("12", T0 + timedelta(minutes=35)),
            silver_completeness("12", T0 + timedelta(minutes=35)),
            silver_quarantine("12", T0 + timedelta(minutes=35)),
            (
                (
                    "gold",
                    "_commits/date=2026-01-15/hour=12/manifest.json",
                ),
                {
                    "partition": "date=2026-01-15/hour=12",
                    "source_silver_uri": (
                        "s3://silver/vehicle_positions/"
                        "date=2026-01-15/hour=12/data.parquet"
                    ),
                    "is_final": True,
                },
            ),
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

        with (
            patch(
                "late_data.resolve_silver_artifacts",
                side_effect=error,
            ),
            self.assertRaises(ClientError),
        ):
            late_data.is_hour_stale("2026-01-15", "12", s3)

    def test_active_manifest_artifacts_are_checked(self):
        publication_id = "a" * 32
        partition = "date=2026-01-15/hour=12"
        version_prefix = f"_versions/{partition}/{publication_id}/"
        manifest = {
            "publication_id": publication_id,
            "partition": partition,
            "status": "published",
            "artifacts": {
                "data": f"s3://silver/{version_prefix}data.parquet",
                "quarantine": (f"s3://silver/{version_prefix}quarantine.parquet"),
                "completeness": (f"s3://silver/{version_prefix}_completeness.json"),
            },
        }
        objects: dict[tuple[str, str], datetime | dict] = dict([bronze("12", T0)])
        manifest_key = "vehicle_positions/date=2026-01-15/hour=12/_active.json"
        objects[("silver", manifest_key)] = manifest
        objects[("silver", f"{version_prefix}data.parquet")] = T0
        objects[("silver", f"{version_prefix}quarantine.parquet")] = T0
        objects[("silver", f"{version_prefix}_completeness.json")] = T0

        result = late_data.is_hour_stale("2026-01-15", "12", FakeS3(objects))

        self.assertFalse(result)


class StaleGoldTests(unittest.TestCase):
    def test_missing_gold_requires_rebuild(self):
        silver_uri = (
            f"s3://silver/_versions/date=2026-01-15/hour=12/{'a' * 32}/data.parquet"
        )
        s3 = FakeS3({})

        with patch.object(
            late_data,
            "resolve_silver_artifacts",
            return_value={"data": silver_uri},
        ):
            result = late_data.is_gold_stale("2026-01-15", "12", s3)

        self.assertTrue(result)

    def test_gold_without_finality_metadata_requires_rebuild_after_grace(self):
        partition = "date=2026-01-15/hour=12"
        silver_uri = f"s3://silver/_versions/{partition}/{'a' * 32}/data.parquet"
        s3 = FakeS3(
            {
                ("gold", f"_commits/{partition}/manifest.json"): {
                    "partition": partition,
                    "source_silver_uri": silver_uri,
                }
            }
        )

        with patch.object(
            late_data,
            "resolve_silver_artifacts",
            return_value={"data": silver_uri},
        ):
            result = late_data.is_gold_stale("2026-01-15", "12", s3, now=NOW)

        self.assertTrue(result)

    def test_hour_with_fresh_silver_and_missing_gold_is_selected_for_repair(self):
        published_at = T0 + timedelta(minutes=35)
        s3 = FakeS3(
            dict(
                [
                    bronze("12", T0),
                    silver("12", published_at),
                    silver_completeness("12", published_at),
                    silver_quarantine("12", published_at),
                ]
            )
        )

        result = late_data.find_stale_hours(
            now=NOW,
            window_hours=1,
            s3_client=s3,
        )

        self.assertEqual(result, [("2026-01-15", "12")])

    def test_gold_freshness_depends_on_source_silver_version(self):
        partition = "date=2026-01-15/hour=12"
        current_uri = f"s3://silver/_versions/{partition}/{'a' * 32}/data.parquet"
        previous_uri = f"s3://silver/_versions/{partition}/{'b' * 32}/data.parquet"

        for source_uri, expected_stale in (
            (current_uri, False),
            (previous_uri, True),
            (None, True),
        ):
            with self.subTest(source_uri=source_uri):
                manifest = {"partition": partition, "is_final": True}
                if source_uri is not None:
                    manifest["source_silver_uri"] = source_uri

                s3 = FakeS3(
                    {
                        (
                            "gold",
                            f"_commits/{partition}/manifest.json",
                        ): manifest
                    }
                )

                with patch.object(
                    late_data,
                    "resolve_silver_artifacts",
                    return_value={"data": current_uri},
                ):
                    result = late_data.is_gold_stale("2026-01-15", "12", s3)

                self.assertEqual(result, expected_stale)

    def test_nonfinal_gold_requires_rebuild_after_grace_period(self):
        partition = "date=2026-01-15/hour=12"
        silver_uri = f"s3://silver/_versions/{partition}/{'a' * 32}/data.parquet"
        s3 = FakeS3(
            {
                ("gold", f"_commits/{partition}/manifest.json"): {
                    "partition": partition,
                    "source_silver_uri": silver_uri,
                    "is_final": False,
                }
            }
        )

        with patch.object(
            late_data,
            "resolve_silver_artifacts",
            return_value={"data": silver_uri},
        ):
            result = late_data.is_gold_stale("2026-01-15", "12", s3, now=NOW)

        self.assertTrue(result)


class ReprocessTests(unittest.TestCase):
    def test_nothing_stale_does_not_open_connection(self):
        with patch.object(late_data, "find_stale_hours", return_value=[]):
            result = late_data.reprocess_late_data(con=None)
        self.assertEqual(result["recomputed"], [])

    def test_gold_repair_does_not_rebuild_fresh_silver(self):
        con = Mock()
        expected = {"status": "complete", "rows": {}}

        with (
            patch.object(
                late_data,
                "is_hour_stale",
                return_value=False,
            ),
            patch.object(
                late_data.bronze_to_silver,
                "transform_hour",
            ) as transform,
            patch.object(
                late_data.silver_to_gold,
                "build_gold_hour",
                return_value=expected,
            ) as build_gold,
        ):
            result = late_data.recompute_hour(
                con,
                "2026-01-15",
                "12",
                run_at=NOW,
                s3_client=MagicMock(),
            )

        transform.assert_not_called()
        build_gold.assert_called_once_with(con, "2026-01-15", "12", NOW)
        self.assertEqual(result, expected)

    def test_nonfinal_gold_is_finalized_without_new_bronze(self):
        published_at = T0 + timedelta(minutes=35)
        manifest_key = (
            "gold",
            "_commits/date=2026-01-15/hour=12/manifest.json",
        )
        manifest = {
            "partition": "date=2026-01-15/hour=12",
            "source_silver_uri": (
                "s3://silver/vehicle_positions/date=2026-01-15/hour=12/data.parquet"
            ),
            "is_final": False,
        }
        s3 = FakeS3(
            dict(
                [
                    bronze("12", T0),
                    silver("12", published_at),
                    silver_completeness("12", published_at),
                    silver_quarantine("12", published_at),
                    (manifest_key, manifest),
                ]
            )
        )
        con = Mock()
        before_grace = NOW.replace(minute=14)
        after_grace = NOW.replace(minute=15)

        def publish_gold(connection, date_str, hour_str, run_at):
            self.assertEqual(run_at, after_grace)
            manifest["is_final"] = late_data.silver_to_gold.is_hour_final(
                date_str, hour_str, run_at
            )
            return {"status": "complete", "rows": {}}

        with (
            patch.object(late_data.bronze_to_silver, "transform_hour") as transform,
            patch.object(
                late_data.silver_to_gold,
                "build_gold_hour",
                side_effect=publish_gold,
            ) as build_gold,
        ):
            early = late_data.reprocess_late_data(
                now=before_grace, window_hours=1, s3_client=s3, con=con
            )
            self.assertEqual(early["recomputed"], [])
            build_gold.assert_not_called()

            finalized = late_data.reprocess_late_data(
                now=after_grace, window_hours=1, s3_client=s3, con=con
            )
            self.assertEqual(finalized["recomputed"], ["2026-01-15 12"])
            self.assertIs(manifest["is_final"], True)
            transform.assert_not_called()
            build_gold.assert_called_once()

            remaining = late_data.find_stale_hours(
                now=after_grace, window_hours=1, s3_client=s3
            )
            self.assertEqual(remaining, [])

    def test_retry_repairs_gold_after_silver_was_published(self):
        s3 = FakeS3(dict([bronze("12", T0)]))
        con = Mock()
        published_at = T0 + timedelta(minutes=35)
        attempts = []

        def publish_silver(*args, **kwargs):
            s3.objects.update(
                [
                    silver("12", published_at),
                    silver_completeness("12", published_at),
                    silver_quarantine("12", published_at),
                ]
            )

        def publish_gold(*args, **kwargs):
            attempts.append("gold")
            if len(attempts) == 1:
                raise RuntimeError("Simulated Gold failure")

            s3.objects[("gold", "_commits/date=2026-01-15/hour=12/manifest.json")] = {
                "partition": "date=2026-01-15/hour=12",
                "source_silver_uri": (
                    "s3://silver/vehicle_positions/date=2026-01-15/hour=12/data.parquet"
                ),
                "is_final": True,
            }
            return {"status": "complete", "rows": {}}

        with (
            patch.object(
                late_data.bronze_to_silver,
                "transform_hour",
                side_effect=publish_silver,
            ) as transform,
            patch.object(
                late_data.silver_to_gold,
                "build_gold_hour",
                side_effect=publish_gold,
            ) as build_gold,
        ):
            with self.assertRaises(RuntimeError):
                late_data.reprocess_late_data(
                    now=NOW, window_hours=1, s3_client=s3, con=con
                )

            result = late_data.reprocess_late_data(
                now=NOW, window_hours=1, s3_client=s3, con=con
            )

            self.assertEqual(transform.call_count, 1)
            self.assertEqual(build_gold.call_count, 2)
            self.assertEqual(result["recomputed"], ["2026-01-15 12"])
            self.assertEqual(result["failed"], [])

            remaining = late_data.find_stale_hours(
                now=NOW, window_hours=1, s3_client=s3
            )
            self.assertEqual(remaining, [])

    def test_stale_silver_is_rebuilt_before_gold(self):
        con = Mock()
        s3 = Mock()
        expected = {"status": "complete", "rows": {}}
        operations = Mock()

        with (
            patch.object(
                late_data,
                "is_hour_stale",
                return_value=True,
            ) as is_stale,
            patch.object(
                late_data.bronze_to_silver,
                "transform_hour",
            ) as transform,
            patch.object(
                late_data.silver_to_gold,
                "build_gold_hour",
                return_value=expected,
            ) as build_gold,
        ):
            operations.attach_mock(transform, "silver")
            operations.attach_mock(build_gold, "gold")

            result = late_data.recompute_hour(
                con,
                "2026-01-15",
                "12",
                run_at=NOW,
                s3_client=s3,
            )

        is_stale.assert_called_once_with("2026-01-15", "12", s3)
        self.assertEqual(
            operations.mock_calls,
            [
                call.silver(con, "2026-01-15", "12", overwrite=True),
                call.gold(con, "2026-01-15", "12", NOW),
            ],
        )
        self.assertEqual(result, expected)

    def test_recomputes_all_and_raises_after_failures(self):
        calls = []

        def fake(con, date_str, hour_str, run_at=None, s3_client=None):
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
