import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import duckdb

from bronze_to_silver import (
    QUALITY_SQL,
    TRANSFORM_SQL,
    transform_hour,
    validate_partition,
)


class BronzeToSilverSqlTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.bronze_path = (Path(self.temp_dir.name) / "bronze.jsonl").as_posix()
        self.connection = duckdb.connect()
        self.connection.execute("SET TimeZone = 'UTC';")

        def record(
            vehicle_number: str,
            gps_time: str | None,
            *,
            lat: float = 52.2,
            ingested_at: str = "2026-07-15T12:00:00+00:00",
        ) -> dict[str, object]:
            return {
                "vehicle_type": "bus",
                "VehicleNumber": vehicle_number,
                "Lines": "1",
                "Brigade": "001",
                "Lat": lat,
                "Lon": 21.0,
                "Time": gps_time,
                "_ingested_at": ingested_at,
            }

        records = [
            record("valid", "2026-07-15 13:58:00"),
            record(
                "valid",
                "2026-07-15 13:58:00",
                ingested_at="2026-07-15T12:00:30+00:00",
            ),
            record("outside", "2026-07-15 13:58:00", lat=50.0),
            record("future", "2026-07-15 14:01:00"),
            record("stale_5_10", "2026-07-15 13:54:00"),
            record("stale_10_30", "2026-07-15 13:45:00"),
            record("stale_over_30", "2026-07-15 13:00:00"),
            record("missing_time", None),
        ]

        with Path(self.bronze_path).open("w", encoding="utf-8") as file:
            file.writelines(json.dumps(item) + "\n" for item in records)

    def tearDown(self):
        self.connection.close()
        self.temp_dir.cleanup()

    def test_quality_metrics_count_each_rejection_reason(self):
        query = QUALITY_SQL.format(bronze_path=self.bronze_path)

        metrics = self.connection.execute(query).fetchone()

        self.assertEqual(metrics, (8, 7, 2, 1, 3, 1, 1, 1, 1, 1))

    def test_transform_keeps_only_fresh_valid_unique_record(self):
        query = TRANSFORM_SQL.format(bronze_path=self.bronze_path)

        rows = self.connection.execute(query).fetchall()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], "valid")
        self.assertEqual(
            rows[0][7],
            datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc),
        )


class TransformHourTests(unittest.TestCase):
    def test_skips_when_bronze_partition_has_no_files(self):
        connection = Mock()
        connection.execute.return_value.fetchone.return_value = None

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


class PartitionValidationTests(unittest.TestCase):
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
