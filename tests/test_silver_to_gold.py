import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import duckdb

import silver_to_gold as gold

RUN_AT = datetime(2026, 1, 15, 13, 5, tzinfo=timezone.utc)

SILVER_SQL = """
CREATE TABLE src AS SELECT * FROM (VALUES
  ('bus',  '1000', ' 523 ', 'b1', 52.1e0, 21.0e0, TIMESTAMPTZ '2026-01-15 12:01:00+00', TIMESTAMPTZ '2026-01-15 12:01:10+00'),
  ('bus',  '1000', '523',   'b1', 52.1e0, 21.0e0, TIMESTAMPTZ '2026-01-15 12:02:00+00', TIMESTAMPTZ '2026-01-15 12:02:10+00'),
  ('bus',  '1001', '174',   'b1', 52.1e0, 21.0e0, TIMESTAMPTZ '2026-01-15 12:03:00+00', TIMESTAMPTZ '2026-01-15 12:03:10+00'),
  ('tram', '1000', '523',   'b1', 52.1e0, 21.0e0, TIMESTAMPTZ '2026-01-15 12:04:00+00', TIMESTAMPTZ '2026-01-15 12:04:10+00'),
  ('tram', '2000', NULL,    'b1', 52.1e0, 21.0e0, TIMESTAMPTZ '2026-01-15 12:05:00+00', TIMESTAMPTZ '2026-01-15 12:05:10+00'),
  ('bus',  '1002', '  ',    'b1', 52.1e0, 21.0e0, TIMESTAMPTZ '2026-01-15 12:06:00+00', TIMESTAMPTZ '2026-01-15 12:06:10+00')
) t(vehicle_type, vehicle_number, line, brigade, lat, lon, gps_time_utc, ingested_at)
"""


def make_silver(directory, extra_sql=None):
    path = os.path.join(directory, "silver.parquet").replace("\\", "/")
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute(extra_sql or SILVER_SQL)
    con.execute(f"COPY src TO '{path}' (FORMAT parquet)")
    con.close()
    return path


def build_all(con, path, status="complete", cycles=(60, 60), silver_rows=None):
    rows = gold.load_silver_hour(con, path)
    gold.build_metric_tables(con, "2026-01-15", "12", status, RUN_AT)
    completeness = {
        "status": status,
        "cycles": cycles[0],
        "complete_cycles": cycles[1],
    }
    con.execute(
        "CREATE OR REPLACE TEMP TABLE quarantine_hour (rejection_reason VARCHAR)"
    )
    con.execute("INSERT INTO quarantine_hour VALUES ('b'), ('a'), ('b')")
    gold.build_quality_table(con, "2026-01-15", "12", completeness, rows, True, RUN_AT)
    return rows


class GoldAggregationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.con = duckdb.connect()
        self.con.execute("SET TimeZone='UTC'")
        self.addCleanup(self.con.close)
        self.path = make_silver(self.tmp.name)

    def test_network_metrics_follow_contract(self):
        build_all(self.con, self.path)
        row = self.con.execute(
            "SELECT active_vehicles, active_bus_vehicles, active_tram_vehicles, "
            "position_events, active_lines, data_completeness_status, "
            "CAST(first_gps_time_utc AS VARCHAR), CAST(last_gps_time_utc AS VARCHAR) "
            "FROM hourly_network_metrics"
        ).fetchall()
        self.assertEqual(len(row), 1)
        # bus 1000/1001/1002 + tram 1000/2000: same number across types counts twice
        self.assertEqual(row[0][:6], (5, 3, 2, 6, 3, "complete"))
        self.assertTrue(row[0][6].startswith("2026-01-15 12:01:00"))
        self.assertTrue(row[0][7].startswith("2026-01-15 12:06:00"))

    def test_line_metrics_trim_and_skip_empty_lines(self):
        build_all(self.con, self.path)
        rows = self.con.execute(
            "SELECT vehicle_type, line, active_vehicles, position_events "
            "FROM hourly_line_metrics ORDER BY vehicle_type, line"
        ).fetchall()
        self.assertEqual(
            rows,
            [("bus", "174", 1, 1), ("bus", "523", 1, 2), ("tram", "523", 1, 1)],
        )

    def test_vehicle_activity_has_sorted_observed_lines(self):
        build_all(self.con, self.path)
        rows = self.con.execute(
            "SELECT vehicle_type, vehicle_number, position_events, observed_lines "
            "FROM vehicle_activity_hourly ORDER BY vehicle_type, vehicle_number"
        ).fetchall()
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[0], ("bus", "1000", 2, ["523"]))
        self.assertEqual(rows[2], ("bus", "1002", 1, []))

    def test_quality_row_counts_and_reasons(self):
        build_all(self.con, self.path, cycles=(60, 57))
        row = self.con.execute(
            "SELECT silver_partition_status, manifest_cycles, "
            "complete_manifest_cycles, complete_cycle_ratio, records_processed, "
            "silver_position_events, quarantined_records, quarantined_by_reason "
            "FROM hourly_data_quality"
        ).fetchone()
        self.assertEqual(row[:3], ("present", 60, 57))
        self.assertAlmostEqual(row[3], 0.95)
        self.assertEqual(row[4:7], (9, 6, 3))
        self.assertEqual(
            row[7],
            [{"reason": "a", "records": 1}, {"reason": "b", "records": 2}],
        )

    def test_unknown_marker_gives_null_ratio_and_is_not_complete(self):
        build_all(self.con, self.path, status="unknown", cycles=(None, None))
        row = self.con.execute(
            "SELECT data_completeness_status, manifest_cycles, complete_cycle_ratio "
            "FROM hourly_data_quality"
        ).fetchone()
        self.assertEqual(row, ("unknown", None, None))
        status = self.con.execute(
            "SELECT DISTINCT data_completeness_status FROM hourly_network_metrics"
        ).fetchall()
        self.assertEqual(status, [("unknown",)])

    def test_gold_validation_passes_for_consistent_tables(self):
        build_all(self.con, self.path)
        gold.validate_gold_tables(self.con, gold.GOLD_TABLES)

    def test_gold_validation_detects_inconsistency(self):
        build_all(self.con, self.path)
        self.con.execute("UPDATE hourly_network_metrics SET position_events = 99")
        with self.assertRaisesRegex(ValueError, "consistency"):
            gold.validate_gold_tables(self.con, gold.GOLD_TABLES)

    def test_gold_validation_detects_duplicate_key(self):
        build_all(self.con, self.path)
        self.con.execute(
            "INSERT INTO hourly_line_metrics SELECT * FROM hourly_line_metrics"
        )
        with self.assertRaisesRegex(ValueError, "unique key"):
            gold.validate_gold_tables(self.con, gold.GOLD_TABLES)

    def test_output_schema_matches_contract(self):
        build_all(self.con, self.path)
        for table, schema in gold.GOLD_SCHEMAS.items():
            actual = [
                (name, gold._normalize_type(str(kind)))
                for name, kind, *_ in self.con.execute(
                    f"DESCRIBE SELECT * FROM {table}"
                ).fetchall()
            ]
            expected = [(n, gold._normalize_type(k)) for n, k in schema]
            self.assertEqual(actual, expected, table)

    def test_idempotent_output_with_fixed_run_time(self):
        build_all(self.con, self.path)
        first = self.con.execute(
            "SELECT * FROM vehicle_activity_hourly ORDER BY ALL"
        ).fetchall()
        build_all(self.con, self.path)
        second = self.con.execute(
            "SELECT * FROM vehicle_activity_hourly ORDER BY ALL"
        ).fetchall()
        self.assertEqual(first, second)

    def test_silver_with_duplicate_observations_is_rejected(self):
        path = make_silver(
            self.tmp.name,
            SILVER_SQL.replace("'1001', '174'", "'1000', '523'").replace(
                "12:03:00+00'", "12:01:00+00'", 1
            ),
        )
        with self.assertRaisesRegex(ValueError, "duplicate_observations"):
            gold.load_silver_hour(self.con, path)

    def test_silver_with_wrong_schema_is_rejected(self):
        path = make_silver(
            self.tmp.name, "CREATE TABLE src AS SELECT 'bus' AS vehicle_type"
        )
        with self.assertRaises(ValueError):
            gold.load_silver_hour(self.con, path)


class CompletenessTests(unittest.TestCase):
    def test_missing_marker_is_unknown(self):
        with patch(
            "silver_to_gold.read_completeness_marker",
            return_value={"status": "unknown"},
        ):
            result = gold.load_completeness("2026-01-15", "12")
        self.assertEqual(
            result, {"status": "unknown", "cycles": None, "complete_cycles": None}
        )

    def test_invalid_status_is_rejected(self):
        with (
            patch(
                "silver_to_gold.read_completeness_marker",
                return_value={"status": "ok"},
            ),
            self.assertRaises(ValueError),
        ):
            gold.load_completeness("2026-01-15", "12")

    def test_marker_for_other_partition_is_rejected(self):
        marker = {"status": "complete", "date": "2026-01-14", "hour": "12"}
        with (
            patch("silver_to_gold.read_completeness_marker", return_value=marker),
            self.assertRaises(ValueError),
        ):
            gold.load_completeness("2026-01-15", "12")

    def test_inconsistent_counters_are_rejected(self):
        marker = {"status": "complete", "cycles": 5, "complete_cycles": 6}
        with (
            patch("silver_to_gold.read_completeness_marker", return_value=marker),
            self.assertRaises(ValueError),
        ):
            gold.load_completeness("2026-01-15", "12")


class BuildGoldHourTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = make_silver(self.tmp.name)
        self.con = duckdb.connect()
        self.con.execute("SET TimeZone='UTC'")
        self.addCleanup(self.con.close)

    def run_build(self, status="complete", silver=True, path=None):
        marker = {"status": status, "cycles": 60, "complete_cycles": 60}
        original = gold.partition_uris
        silver_path = path or self.path

        def fake_uris(date_str, hour_str):
            return {**original(date_str, hour_str), "silver": silver_path}

        with (
            patch("silver_to_gold.read_completeness_marker", return_value=marker),
            patch(
                "silver_to_gold.s3_object_exists",
                side_effect=lambda uri: silver and "quarantine" not in uri,
            ),
            patch("silver_to_gold.partition_uris", side_effect=fake_uris),
            patch(
                "silver_to_gold.publish_tables",
                side_effect=lambda con, tables, uris: {t: 1 for t in tables},
            ) as publish,
        ):
            try:
                result = gold.build_gold_hour(self.con, "2026-01-15", "12", RUN_AT)
            except Exception:
                self.publish_calls = [c.args[1] for c in publish.call_args_list]
                raise
        self.publish_calls = [c.args[1] for c in publish.call_args_list]
        return result

    def test_complete_hour_publishes_all_tables_quality_last(self):
        result = self.run_build()
        self.assertEqual(result["status"], "complete")
        self.assertEqual(self.publish_calls, [gold.GOLD_TABLES])
        self.assertEqual(gold.GOLD_TABLES[-1], "hourly_data_quality")

    def test_incomplete_hour_is_labelled_and_warned(self):
        with self.assertLogs("silver_to_gold", level="WARNING"):
            result = self.run_build(status="incomplete")
        self.assertEqual(result["status"], "incomplete")
        status = self.con.execute(
            "SELECT DISTINCT data_completeness_status FROM hourly_network_metrics"
        ).fetchall()
        self.assertEqual(status, [("incomplete",)])

    def test_missing_silver_records_quality_only_and_fails(self):
        with self.assertRaises(gold.SilverDataMissingError):
            self.run_build(silver=False)
        self.assertEqual(self.publish_calls, [("hourly_data_quality",)])
        status = self.con.execute(
            "SELECT silver_partition_status, silver_position_events "
            "FROM hourly_data_quality"
        ).fetchone()
        self.assertEqual(status, ("missing", None))

    def test_empty_silver_fails_without_publishing_metrics(self):
        path = make_silver(self.tmp.name, SILVER_SQL + " LIMIT 0")
        with self.assertRaisesRegex(ValueError, "empty"):
            self.run_build(path=path)
        self.assertEqual(self.publish_calls, [("hourly_data_quality",)])

    def test_invalid_partition_is_rejected(self):
        with self.assertRaises(ValueError):
            gold.build_gold_hour(self.con, "bad", "12")


class PublishTests(unittest.TestCase):
    def test_partition_uris_use_gold_bucket_and_staging(self):
        uris = gold.partition_uris("2026-01-15", "09")
        self.assertEqual(
            uris["gold"]["hourly_network_metrics"],
            "s3://gold/hourly_network_metrics/date=2026-01-15/hour=09/data.parquet",
        )
        for table in gold.GOLD_TABLES:
            self.assertTrue(uris["staging"][table].endswith(".parquet.pending"))
            self.assertIn("/_staging/", uris["staging"][table])


if __name__ == "__main__":
    unittest.main()
