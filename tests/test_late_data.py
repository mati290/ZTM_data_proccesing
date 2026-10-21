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
                    {
                        "Key": key,
                        "LastModified": modified,
                        "ETag": '"fixture-etag"',
                        "Size": 123,
                    }
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

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError(
                {
                    "Error": {"Code": "NoSuchKey"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "HeadObject",
            )
        return {"ETag": '"fixture-etag"'}

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

    @staticmethod
    def _read_inputs(source_inputs: list) -> list[dict] | None:
        partition = "date=2026-01-15/hour=12"
        s3 = FakeS3(
            {
                (
                    "silver",
                    f"vehicle_positions/{partition}/_active.json",
                ): {
                    "partition": partition,
                    "source_inputs": source_inputs,
                }
            }
        )
        return bronze_to_silver.read_silver_source_inputs("2026-01-15", "12", s3)

    def test_recorded_source_inputs_accept_valid_metadata(self):
        items = [
            {
                "key": key,
                "etag": '"opaque-etag-2"',
                "size": 0,
                "last_modified": T0.isoformat(),
            }
            for key in (
                "vehicle_type=bus/date=2026-01-15/hour=12/a.jsonl",
                "vehicle_type=tram/date=2026-01-15/hour=12/b.jsonl",
                "_manifests/date=2026-01-15/hour=12/c.json",
            )
        ]

        for source_inputs in ([], items):
            with self.subTest(count=len(source_inputs)):
                self.assertEqual(
                    self._read_inputs(source_inputs),
                    source_inputs,
                )

    def test_recorded_source_inputs_reject_invalid_metadata(self):
        valid_item = {
            "key": "vehicle_type=bus/date=2026-01-15/hour=12/a.jsonl",
            "etag": '"fixture-etag"',
            "size": 123,
            "last_modified": T0.isoformat(),
        }
        cases = (
            ("size", None),
            ("size", True),
            ("size", -1),
            ("size", "123"),
            ("etag", None),
            ("etag", ""),
            ("etag", 123),
            ("last_modified", None),
            ("last_modified", "not-a-timestamp"),
            ("last_modified", "2026-01-15T12:30:00"),
        )

        for field, value in cases:
            with self.subTest(field=field, value=value):
                item = {**valid_item, field: value}

                with self.assertRaisesRegex(
                    ValueError,
                    f"source_inputs.*{field}",
                ):
                    self._read_inputs([item])

    def test_recorded_source_inputs_reject_duplicate_keys(self):
        item = {
            "key": "vehicle_type=bus/date=2026-01-15/hour=12/a.jsonl",
            "etag": '"fixture-etag"',
            "size": 123,
            "last_modified": T0.isoformat(),
        }

        with self.assertRaisesRegex(
            ValueError,
            "source_inputs.*duplicate key",
        ):
            self._read_inputs([item, item.copy()])

    def test_recorded_source_inputs_reject_invalid_keys(self):
        invalid_keys = (
            None,
            123,
            "",
            "vehicle_type=bus/date=2026-01-15/hour=11/a.jsonl",
            "vehicle_type=car/date=2026-01-15/hour=12/a.jsonl",
            "_manifests/date=2026-01-15/hour=12/a.txt",
            "unrelated/a.jsonl",
        )

        for key in invalid_keys:
            with self.subTest(key=key):
                item = {
                    "key": key,
                    "etag": '"fixture-etag"',
                    "size": 123,
                    "last_modified": T0.isoformat(),
                }

                with self.assertRaisesRegex(ValueError, "source_inputs.*key"):
                    self._read_inputs([item])

    def test_recorded_source_inputs_reject_non_object_items(self):
        partition = "date=2026-01-15/hour=12"

        for item in (None, [], 123, "a.jsonl", True):
            with self.subTest(item=item):
                manifest = {
                    "partition": partition,
                    "source_inputs": [item],
                }
                s3 = FakeS3(
                    {
                        (
                            "silver",
                            f"vehicle_positions/{partition}/_active.json",
                        ): manifest
                    }
                )

                with self.assertRaisesRegex(
                    TypeError,
                    "source_inputs items must be JSON objects",
                ):
                    bronze_to_silver.read_silver_source_inputs("2026-01-15", "12", s3)


class StaleHourTests(unittest.TestCase):
    def stale(self, *items, window=3):
        return late_data.find_stale_hours(NOW, window, FakeS3(dict(items)))

    def test_late_bronze_after_silver_is_stale(self):
        result = self.stale(
            bronze("12", T0 + timedelta(minutes=40)),
            silver("12", T0 + timedelta(minutes=35)),
        )
        self.assertEqual(result, [("2026-01-15", "12")])

    def test_deleted_bronze_inputs_make_existing_silver_stale(self):
        objects = dict([bronze("12", T0)])
        s3 = FakeS3(objects)
        captured = bronze_to_silver.capture_bronze_inputs("2026-01-15", "12", s3)
        self.assertTrue(captured)

        objects.clear()
        objects.update(
            [
                silver("12", T0 + timedelta(minutes=35)),
                silver_completeness("12", T0 + timedelta(minutes=35)),
                silver_quarantine("12", T0 + timedelta(minutes=35)),
            ]
        )

        with patch.object(
            bronze_to_silver,
            "read_silver_source_inputs",
            return_value=captured,
        ):
            result = late_data.is_hour_stale("2026-01-15", "12", s3)

        self.assertTrue(result)

    def test_window_requires_integer_before_storage_access(self):
        for window_hours in (True, False, 1.5, "24", None):
            with self.subTest(window_hours=window_hours):
                s3 = Mock()

                with self.assertRaisesRegex(TypeError, "window_hours"):
                    late_data.find_stale_hours(
                        now=NOW,
                        window_hours=window_hours,
                        s3_client=s3,
                    )

                self.assertEqual(s3.mock_calls, [])

    def test_nonpositive_window_is_rejected_before_storage_access(self):
        for window_hours in (0, -1):
            with self.subTest(window_hours=window_hours):
                s3 = Mock()

                with self.assertRaisesRegex(ValueError, "window_hours"):
                    late_data.find_stale_hours(
                        now=NOW,
                        window_hours=window_hours,
                        s3_client=s3,
                    )

                self.assertEqual(s3.mock_calls, [])

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
        published_at = T0 + timedelta(minutes=35)
        silver_uri = (
            "s3://silver/vehicle_positions/date=2026-01-15/hour=12/data.parquet"
        )
        objects = dict(
            [
                bronze("12", T0),
                silver("12", published_at),
                silver_completeness("12", published_at),
                silver_quarantine("12", published_at),
            ]
        )
        objects.update(gold_publication_objects(gold_manifest(silver_uri)))

        result = self.stale(*objects.items())

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


def gold_manifest(silver_uri: str, is_final: bool = True) -> dict:
    partition = "date=2026-01-15/hour=12"
    publication_id = "b" * 32
    return {
        "publication_id": publication_id,
        "partition": partition,
        "source_silver_uri": silver_uri,
        "source_silver_etag": (
            '"fixture-etag"'
            if silver_uri.startswith("s3://silver/vehicle_positions/")
            else None
        ),
        "is_final": is_final,
        "tables": {
            table: {
                "uri": (
                    f"s3://gold/_versions/{partition}/"
                    f"{publication_id}/{table}/data.parquet"
                ),
                "rows": 1,
            }
            for table in late_data.silver_to_gold.GOLD_TABLES
        },
    }


def gold_publication_objects(manifest: dict) -> dict:
    objects = {
        (
            "gold",
            f"_commits/{manifest['partition']}/manifest.json",
        ): manifest,
    }
    for metadata in manifest["tables"].values():
        bucket, key = bronze_to_silver._parse_s3_uri(metadata["uri"])
        objects[(bucket, key)] = NOW
    return objects


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

    def test_legacy_gold_without_source_etag_requires_rebuild(self):
        silver_key = "vehicle_positions/date=2026-01-15/hour=12/data.parquet"
        silver_uri = f"s3://silver/{silver_key}"
        manifest = gold_manifest(silver_uri)
        manifest.pop("source_silver_etag")
        objects = gold_publication_objects(manifest)
        objects[("silver", silver_key)] = NOW
        s3 = FakeS3(objects)

        with patch.object(
            late_data,
            "resolve_silver_artifacts",
            return_value={"data": silver_uri},
        ):
            result = late_data.is_gold_stale("2026-01-15", "12", s3, now=NOW)

        self.assertTrue(result)

    def test_unchanged_legacy_silver_does_not_require_gold_rebuild(self):
        silver_key = "vehicle_positions/date=2026-01-15/hour=12/data.parquet"
        silver_uri = f"s3://silver/{silver_key}"
        manifest = gold_manifest(silver_uri)
        objects = gold_publication_objects(manifest)
        objects[("silver", silver_key)] = NOW
        s3 = FakeS3(objects)

        with patch.object(
            late_data,
            "resolve_silver_artifacts",
            return_value={"data": silver_uri},
        ):
            result = late_data.is_gold_stale("2026-01-15", "12", s3, now=NOW)

        self.assertFalse(result)

    def test_changed_legacy_silver_etag_requires_gold_rebuild(self):
        silver_key = "vehicle_positions/date=2026-01-15/hour=12/data.parquet"
        silver_uri = f"s3://silver/{silver_key}"
        manifest = gold_manifest(silver_uri)
        manifest["source_silver_etag"] = '"previous-etag"'

        s3 = Mock()
        s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=json.dumps(manifest).encode()))
        }

        def head_object(Bucket, Key):
            if Bucket == "silver" and Key == silver_key:
                return {"ETag": '"current-etag"'}
            return {}

        s3.head_object.side_effect = head_object

        with patch.object(
            late_data,
            "resolve_silver_artifacts",
            return_value={"data": silver_uri},
        ):
            result = late_data.is_gold_stale("2026-01-15", "12", s3, now=NOW)

        self.assertTrue(result)
        s3.head_object.assert_any_call(Bucket="silver", Key=silver_key)

    def test_invalid_gold_artifact_metadata_requires_rebuild(self):
        silver_uri = (
            f"s3://silver/_versions/date=2026-01-15/hour=12/{'a' * 32}/data.parquet"
        )
        table = late_data.silver_to_gold.GOLD_TABLES[0]
        invalid_values = (
            None,
            {"uri": "s3://other-bucket/data.parquet", "rows": 1},
            {
                "uri": (
                    "s3://gold/_versions/date=2026-01-15/hour=11/"
                    f"{'b' * 32}/{table}/data.parquet"
                ),
                "rows": 1,
            },
        )

        for metadata in invalid_values:
            with self.subTest(metadata=metadata):
                manifest = gold_manifest(silver_uri)
                manifest["tables"][table] = metadata
                s3 = Mock()
                s3.get_object.return_value = {
                    "Body": Mock(read=Mock(return_value=json.dumps(manifest).encode()))
                }

                with patch.object(
                    late_data,
                    "resolve_silver_artifacts",
                    return_value={"data": silver_uri},
                ):
                    result = late_data.is_gold_stale("2026-01-15", "12", s3, now=NOW)

                self.assertTrue(result)
                s3.head_object.assert_not_called()

    def test_complete_gold_publication_does_not_require_rebuild(self):
        silver_uri = (
            f"s3://silver/_versions/date=2026-01-15/hour=12/{'a' * 32}/data.parquet"
        )
        s3 = Mock()
        s3.get_object.return_value = {
            "Body": Mock(
                read=Mock(return_value=json.dumps(gold_manifest(silver_uri)).encode())
            )
        }
        s3.head_object.return_value = {}

        with patch.object(
            late_data,
            "resolve_silver_artifacts",
            return_value={"data": silver_uri},
        ):
            result = late_data.is_gold_stale("2026-01-15", "12", s3, now=NOW)

        self.assertFalse(result)
        self.assertEqual(s3.head_object.call_count, 4)

    def test_gold_file_storage_errors_propagate(self):
        silver_uri = (
            f"s3://silver/_versions/date=2026-01-15/hour=12/{'a' * 32}/data.parquet"
        )

        for code, status in (("AccessDenied", 403), ("InternalError", 500)):
            with self.subTest(code=code):
                error = ClientError(
                    {
                        "Error": {"Code": code},
                        "ResponseMetadata": {"HTTPStatusCode": status},
                    },
                    "HeadObject",
                )
                s3 = Mock()
                s3.get_object.return_value = {
                    "Body": Mock(
                        read=Mock(
                            return_value=json.dumps(gold_manifest(silver_uri)).encode()
                        )
                    )
                }
                s3.head_object.side_effect = error

                with (
                    patch.object(
                        late_data,
                        "resolve_silver_artifacts",
                        return_value={"data": silver_uri},
                    ),
                    self.assertRaises(ClientError) as raised,
                ):
                    late_data.is_gold_stale("2026-01-15", "12", s3, now=NOW)

                self.assertIs(raised.exception, error)

    def test_missing_gold_file_requires_rebuild(self):
        partition = "date=2026-01-15/hour=12"
        publication_id = "b" * 32
        silver_uri = f"s3://silver/_versions/{partition}/{'a' * 32}/data.parquet"
        tables = {
            table: {
                "uri": (
                    f"s3://gold/_versions/{partition}/"
                    f"{publication_id}/{table}/data.parquet"
                ),
                "rows": 1,
            }
            for table in late_data.silver_to_gold.GOLD_TABLES
        }
        manifest = {
            "publication_id": publication_id,
            "partition": partition,
            "source_silver_uri": silver_uri,
            "is_final": True,
            "tables": tables,
        }
        missing_key = (
            f"_versions/{partition}/{publication_id}/"
            "hourly_network_metrics/data.parquet"
        )

        def head_object(Bucket, Key):
            if Bucket == "gold" and Key == missing_key:
                raise ClientError(
                    {
                        "Error": {"Code": "NoSuchKey"},
                        "ResponseMetadata": {"HTTPStatusCode": 404},
                    },
                    "HeadObject",
                )
            return {}

        s3 = Mock()
        s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=json.dumps(manifest).encode()))
        }
        s3.head_object.side_effect = head_object

        with patch.object(
            late_data,
            "resolve_silver_artifacts",
            return_value={"data": silver_uri},
        ):
            result = late_data.is_gold_stale("2026-01-15", "12", s3, now=NOW)

        self.assertTrue(result)
        s3.head_object.assert_any_call(Bucket="gold", Key=missing_key)

    def test_gold_without_required_tables_requires_rebuild(self):
        partition = "date=2026-01-15/hour=12"
        silver_uri = f"s3://silver/_versions/{partition}/{'a' * 32}/data.parquet"

        s3 = FakeS3(
            {
                ("gold", f"_commits/{partition}/manifest.json"): {
                    "publication_id": "b" * 32,
                    "partition": partition,
                    "source_silver_uri": silver_uri,
                    "is_final": True,
                    "tables": {},
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

    def test_gold_without_finality_metadata_requires_rebuild_after_grace(self):
        partition = "date=2026-01-15/hour=12"
        silver_uri = f"s3://silver/_versions/{partition}/{'a' * 32}/data.parquet"

        for minute, expected_stale in ((14, False), (15, True)):
            with self.subTest(minute=minute):
                manifest = gold_manifest(silver_uri)
                manifest.pop("is_final")
                s3 = FakeS3(gold_publication_objects(manifest))
                checked_at = NOW.replace(minute=minute, second=0, microsecond=0)

                with patch.object(
                    late_data,
                    "resolve_silver_artifacts",
                    return_value={"data": silver_uri},
                ):
                    result = late_data.is_gold_stale(
                        "2026-01-15", "12", s3, now=checked_at
                    )

                self.assertEqual(result, expected_stale)

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
                manifest = gold_manifest(current_uri)
                if source_uri is None:
                    manifest.pop("source_silver_uri")
                else:
                    manifest["source_silver_uri"] = source_uri

                s3 = FakeS3(gold_publication_objects(manifest))

                with patch.object(
                    late_data,
                    "resolve_silver_artifacts",
                    return_value={"data": current_uri},
                ):
                    result = late_data.is_gold_stale("2026-01-15", "12", s3, now=NOW)

                self.assertEqual(result, expected_stale)

    def test_nonfinal_gold_requires_rebuild_after_grace_period(self):
        partition = "date=2026-01-15/hour=12"
        silver_uri = f"s3://silver/_versions/{partition}/{'a' * 32}/data.parquet"

        for minute, expected_stale in ((14, False), (15, True)):
            with self.subTest(minute=minute):
                manifest = gold_manifest(silver_uri, is_final=False)
                s3 = FakeS3(gold_publication_objects(manifest))
                checked_at = NOW.replace(minute=minute, second=0, microsecond=0)

                with patch.object(
                    late_data,
                    "resolve_silver_artifacts",
                    return_value={"data": silver_uri},
                ):
                    result = late_data.is_gold_stale(
                        "2026-01-15", "12", s3, now=checked_at
                    )

                self.assertEqual(result, expected_stale)


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

    def test_missing_bronze_stops_repair_before_gold_publication(self):
        con = Mock()
        con.execute.return_value.fetchone.return_value = None
        s3 = Mock()

        with (
            patch.object(late_data, "is_hour_stale", return_value=True),
            patch.object(late_data.silver_to_gold, "build_gold_hour") as build_gold,
            patch.object(
                late_data.bronze_to_silver, "write_silver_active_manifest"
            ) as write_active,
        ):
            with self.assertRaises(bronze_to_silver.BronzeDataMissingError):
                late_data.recompute_hour(
                    con,
                    "2026-01-15",
                    "12",
                    run_at=NOW,
                    s3_client=s3,
                )

            build_gold.assert_not_called()
            write_active.assert_not_called()

        s3.put_object.assert_not_called()
        s3.delete_object.assert_not_called()

    def test_nonfinal_gold_is_finalized_without_new_bronze(self):
        published_at = T0 + timedelta(minutes=35)
        silver_uri = (
            "s3://silver/vehicle_positions/date=2026-01-15/hour=12/data.parquet"
        )
        manifest = gold_manifest(silver_uri, is_final=False)
        objects = dict(
            [
                bronze("12", T0),
                silver("12", published_at),
                silver_completeness("12", published_at),
                silver_quarantine("12", published_at),
            ]
        )
        objects.update(gold_publication_objects(manifest))
        s3 = FakeS3(objects)
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

            silver_uri = (
                "s3://silver/vehicle_positions/date=2026-01-15/hour=12/data.parquet"
            )
            s3.objects.update(gold_publication_objects(gold_manifest(silver_uri)))
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
