import json
import os
import sys
import tempfile
import unittest
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, call, patch

import duckdb
from botocore.exceptions import ClientError

from bronze_to_silver import (
    QUALITY_SQL,
    REJECTED_SQL,
    TRANSFORM_SQL,
    BronzeDataMissingError,
    SilverHourLockedError,
    assess_completeness,
    copy_s3_object,
    delete_s3_object,
    get_connection,
    is_before_first_bronze_hour,
    is_hour_complete,
    main,
    parse_s3_endpoint,
    read_completeness_marker,
    s3_object_exists,
    silver_hour_lock,
    transform_hour,
    validate_partition,
    validate_silver_schema,
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

    def test_is_hour_complete_only_for_complete(self):
        for status, expected in (
            ("complete", True),
            ("incomplete", False),
            ("unknown", False),
        ):
            client = self._marker_client(status)
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
    def _client(statuses):
        client = Mock()
        keys = [
            f"_manifests/date=2026-10-09/hour=07/c{i}.json"
            for i in range(len(statuses))
        ]
        client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": key} for key in keys]}
        ]
        bodies = {
            key: json.dumps({"status": status}).encode()
            for key, status in zip(keys, statuses)
        }
        client.get_object.side_effect = lambda Bucket, Key: {
            "Body": Mock(read=Mock(return_value=bodies[Key]))
        }
        return client

    def test_complete_when_all_cycles_have_both_sources(self):
        result = assess_completeness(
            "2026-10-09", "07", self._client(["complete"] * 10)
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


class BronzeToSilverSqlTests(unittest.TestCase):
    def setUp(self):
        self.bronze_path = (
            Path(__file__).parent / "fixtures" / "bronze_sample.jsonl"
        ).as_posix()
        self.connection = duckdb.connect()
        self.connection.execute("SET TimeZone = 'UTC';")

    def tearDown(self):
        self.connection.close()

    def test_quality_metrics_count_each_rejection_reason(self):
        query = QUALITY_SQL.format(bronze_path=self.bronze_path)

        metrics = self.connection.execute(query).fetchone()

        self.assertEqual(metrics, (11, 9, 9, 3, 1, 3, 1, 1, 1, 2, 1))

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
        patch("bronze_to_silver.silver_hour_lock", return_value=nullcontext()).start()
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

    def test_silver_lock_uses_conditional_create_and_releases_owned_lock(self):
        client = Mock()
        client.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=b'{"token": "silver-token"}')),
            "ETag": '"silver-etag"',
        }
        with patch("bronze_to_silver.uuid.uuid4") as uuid4:
            uuid4.return_value.hex = "silver-token"
            with silver_hour_lock("2026-10-09", "07", client):
                pass
        self.assertEqual(client.put_object.call_args.kwargs["IfNoneMatch"], "*")
        client.delete_object.assert_called_once_with(
            Bucket="silver",
            Key="_locks/pipeline/date=2026-10-09/hour=07/writer.json",
            IfMatch='"silver-etag"',
        )

    def test_silver_lock_collision_fails_closed(self):
        client = Mock()
        client.put_object.side_effect = ClientError(
            {
                "Error": {"Code": "PreconditionFailed"},
                "ResponseMetadata": {"HTTPStatusCode": 412},
            },
            "PutObject",
        )
        with (
            self.assertRaises(SilverHourLockedError),
            silver_hour_lock("2026-10-09", "07", client),
        ):
            self.fail("must not enter while another writer owns the hour")
        client.delete_object.assert_not_called()

    def test_fails_when_bronze_partition_has_no_files(self):
        connection = Mock()
        connection.execute.return_value.fetchone.return_value = None

        with self.assertRaises(BronzeDataMissingError):
            transform_hour(connection, "2026-10-09", "7")

        connection.execute.assert_called_once_with(
            "SELECT file FROM glob("
            "'s3://bronze/vehicle_type=*/date=2026-10-09/hour=07/*.jsonl') LIMIT 1"
        )

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
        ):
            transform_hour(connection, "2026-10-09", "7")

        copy_statements = [
            call.args[0]
            for call in connection.execute.call_args_list
            if call.args[0].startswith("COPY ")
        ]
        self.assertEqual(len(copy_statements), 2)
        source, destination = copy_mock.call_args.args
        self.assertTrue(source.endswith(".parquet.pending"))
        self.assertEqual(
            destination,
            "s3://silver/vehicle_positions/date=2026-10-09/hour=07/data.parquet",
        )
        self.assertEqual(
            copy_mock.call_args_list[0].args[1],
            "s3://silver/quarantine/vehicle_positions/date=2026-10-09/hour=07/data.parquet",
        )
        delete_mock.assert_any_call(source)
        self.assertEqual(delete_mock.call_count, 3)
        self.assertIn(
            call(
                "s3://silver/vehicle_positions/date=2026-10-09/hour=07/_completeness.json"
            ),
            delete_mock.call_args_list,
        )
        self.assess_mock.assert_called_once_with("2026-10-09", "07")
        self.write_marker_mock.assert_called_once_with(
            self.completeness,
            "s3://silver/vehicle_positions/date=2026-10-09/hour=07",
        )

    def test_marker_is_invalidated_before_any_published_object_copy(self):
        connection = self._happy_path_connection()
        events = []
        with (
            patch("bronze_to_silver.validate_silver_schema"),
            patch(
                "bronze_to_silver.delete_s3_object",
                side_effect=lambda uri: events.append(("delete", uri)),
            ),
            patch(
                "bronze_to_silver.copy_s3_object",
                side_effect=lambda src, dst: events.append(("copy", dst)),
            ),
            patch(
                "bronze_to_silver.assess_completeness",
                side_effect=lambda *args: (
                    events.append(("assess", args)) or self.completeness
                ),
            ),
            patch(
                "bronze_to_silver.write_completeness_marker",
                side_effect=lambda *args: events.append(("marker", args)),
            ),
        ):
            transform_hour(connection, "2026-10-09", "7")

        marker_key = (
            "s3://silver/vehicle_positions/date=2026-10-09/hour=07/_completeness.json"
        )
        marker_delete = events.index(("delete", marker_key))
        first_copy = next(i for i, event in enumerate(events) if event[0] == "copy")
        assess = next(i for i, event in enumerate(events) if event[0] == "assess")
        marker_write = next(i for i, event in enumerate(events) if event[0] == "marker")
        self.assertLess(marker_delete, first_copy)
        self.assertLess(first_copy, assess)
        self.assertLess(assess, marker_write)

    def test_assessment_failure_leaves_previous_marker_invalidated(self):
        connection = self._happy_path_connection()
        with (
            patch("bronze_to_silver.validate_silver_schema"),
            patch("bronze_to_silver.copy_s3_object"),
            patch("bronze_to_silver.delete_s3_object") as delete_mock,
            patch(
                "bronze_to_silver.assess_completeness",
                side_effect=RuntimeError("manifest listing failed"),
            ),
            patch("bronze_to_silver.write_completeness_marker") as marker_mock,
            self.assertRaisesRegex(RuntimeError, "manifest listing failed"),
        ):
            transform_hour(connection, "2026-10-09", "7")
        delete_mock.assert_any_call(
            "s3://silver/vehicle_positions/date=2026-10-09/hour=07/_completeness.json"
        )
        marker_mock.assert_not_called()

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
        ):
            transform_hour(connection, "2026-10-09", "7")

        destinations = [call.args[1] for call in copy_mock.call_args_list]
        self.assertEqual(
            destinations,
            [
                "s3://silver/quarantine/vehicle_positions/date=2026-10-09/hour=07/data.parquet",
                "s3://silver/vehicle_positions/date=2026-10-09/hour=07/data.parquet",
            ],
        )

    def test_overwrite_false_refuses_to_replace_existing_output(self):
        connection = self._happy_path_connection()

        with (
            patch("bronze_to_silver.validate_silver_schema"),
            patch("bronze_to_silver.s3_object_exists", return_value=True),
            patch("bronze_to_silver.copy_s3_object") as copy_mock,
            patch("bronze_to_silver.delete_s3_object") as delete_mock,
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
