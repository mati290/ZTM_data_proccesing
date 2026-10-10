import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import duckdb
from botocore.exceptions import ClientError

from bronze_to_silver import (
    QUALITY_SQL,
    TRANSFORM_SQL,
    BronzeDataMissingError,
    check_bronze_freshness,
    copy_s3_object,
    delete_s3_object,
    s3_object_exists,
    transform_hour,
    validate_partition,
    validate_silver_schema,
)


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

        self.assertEqual(metrics, (11, 9, 3, 1, 3, 1, 1, 1, 2, 1))

    def test_transform_keeps_only_fresh_valid_unique_record(self):
        query = TRANSFORM_SQL.format(bronze_path=self.bronze_path)

        rows = self.connection.execute(query).fetchall()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], "valid")
        self.assertEqual(rows[0][4], 52.2)
        self.assertEqual(
            rows[0][7],
            datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc),
        )


class TransformHourTests(unittest.TestCase):
    @staticmethod
    def _happy_path_connection():
        connection = Mock()
        connection.execute.return_value.fetchone.side_effect = [
            ("bronze-file.jsonl",),
            (1, 1, 1, 0, 0, 0, 0, 0, 0, 1),
            (1,),
        ]
        return connection

    def test_fails_when_bronze_partition_has_no_files(self):
        connection = Mock()
        connection.execute.return_value.fetchone.return_value = None

        with self.assertRaises(BronzeDataMissingError):
            transform_hour(connection, "2026-10-09", "7")

        connection.execute.assert_called_once_with(
            "SELECT file FROM glob(?) LIMIT 1",
            ["s3://bronze/vehicle_type=*/date=2026-10-09/hour=07/*.jsonl"],
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
        self.assertEqual(len(copy_statements), 1)
        source, destination = copy_mock.call_args.args
        self.assertTrue(source.endswith(".parquet.pending"))
        self.assertEqual(
            destination,
            "s3://silver/vehicle_positions/date=2026-10-09/hour=07/data.parquet",
        )
        delete_mock.assert_called_once_with(source)

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
        delete_mock.assert_called_once()

    def test_does_not_publish_when_row_count_differs_from_quality_count(self):
        connection = Mock()
        connection.execute.return_value.fetchone.side_effect = [
            ("bronze-file.jsonl",),
            (1, 1, 1, 0, 0, 0, 0, 0, 0, 1),
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
        delete_mock.assert_called_once()

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
        delete_mock.assert_called_once()

    def test_transform_propagates_write_error_and_still_cleans_temp_file(self):
        connection = Mock()
        connection.execute.side_effect = [
            Mock(fetchone=Mock(return_value=("bronze-file.jsonl",))),
            Mock(fetchone=Mock(return_value=(1, 1, 1, 0, 0, 0, 0, 0, 0, 1))),
            RuntimeError("write failed"),
        ]

        with (
            patch("bronze_to_silver.copy_s3_object") as copy_mock,
            patch("bronze_to_silver.delete_s3_object") as delete_mock,
            self.assertRaisesRegex(RuntimeError, "write failed"),
        ):
            transform_hour(connection, "2026-10-09", "7")

        copy_mock.assert_not_called()
        delete_mock.assert_called_once()


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

    def test_check_bronze_freshness_accepts_recent_partition(self):
        now = datetime(2026, 10, 9, 12, 30, tzinfo=timezone.utc)
        connection = Mock()
        connection.execute.return_value.fetchall.return_value = [("f.jsonl",)]
        result = check_bronze_freshness(connection, "2026-10-09", "12", now_utc=now)

        self.assertEqual(result["date"], "2026-10-09")
        self.assertEqual(result["hour"], "12")
        self.assertEqual(result["file_count"], 1)
        self.assertGreaterEqual(result["age_minutes"], 0)

    def test_check_bronze_freshness_fails_without_files(self):
        now = datetime(2026, 10, 9, 12, 30, tzinfo=timezone.utc)
        connection = Mock()
        connection.execute.return_value.fetchall.return_value = []

        with self.assertRaises(BronzeDataMissingError):
            check_bronze_freshness(connection, "2026-10-09", "12", now_utc=now)

    def test_check_bronze_freshness_rejects_stale_partition(self):
        now = datetime(2026, 10, 9, 12, 30, tzinfo=timezone.utc)
        with self.assertRaises(ValueError):
            check_bronze_freshness(
                self.connection, "2026-10-08", "12", max_age_minutes=30, now_utc=now
            )


if __name__ == "__main__":
    unittest.main()
