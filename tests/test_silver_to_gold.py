import contextlib
import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import duckdb

import silver_to_gold as gold

RUN_AT = datetime(2026, 1, 15, 13, 20, tzinfo=timezone.utc)

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


def build_all(con, path, status="complete", cycles=(60, 60), reasons=("b", "a", "b")):
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
    for reason in reasons:
        con.execute("INSERT INTO quarantine_hour VALUES (?)", [reason])
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

    def test_quarantine_accepts_empty_and_known_reasons(self):
        cases = ((), gold.QUARANTINE_REJECTION_REASONS)

        for index, reasons in enumerate(cases):
            with self.subTest(reasons=reasons):
                path = (f"{self.tmp.name}/valid_quarantine_{index}.parquet").replace(
                    "\\", "/"
                )
                self.con.execute(
                    "CREATE OR REPLACE TEMP TABLE valid_quarantine "
                    "(rejection_reason VARCHAR)"
                )
                for reason in reasons:
                    self.con.execute(
                        "INSERT INTO valid_quarantine VALUES (?)",
                        [reason],
                    )
                self.con.execute(f"COPY valid_quarantine TO '{path}' (FORMAT parquet)")

                gold.load_quarantine_hour(self.con, path)

                self.assertCountEqual(
                    self.con.execute(
                        "SELECT rejection_reason FROM quarantine_hour"
                    ).fetchall(),
                    [(reason,) for reason in reasons],
                )

    def test_quarantine_rejects_invalid_rejection_reason(self):
        cases = (
            ("unexpected_reason", "VARCHAR"),
            (" duplicate ", "VARCHAR"),
            (123, "INTEGER"),
        )

        for index, (reason, column_type) in enumerate(cases):
            with self.subTest(reason=reason):
                path = (f"{self.tmp.name}/invalid_reason_{index}.parquet").replace(
                    "\\", "/"
                )
                self.con.execute(
                    "CREATE OR REPLACE TEMP TABLE invalid_quarantine "
                    f"(rejection_reason {column_type})"
                )
                self.con.execute(
                    "INSERT INTO invalid_quarantine VALUES (?)",
                    [reason],
                )
                self.con.execute(
                    f"COPY invalid_quarantine TO '{path}' (FORMAT parquet)"
                )

                with self.assertRaisesRegex(ValueError, "rejection_reason"):
                    gold.load_quarantine_hour(self.con, path)

    def test_quarantine_rejects_missing_rejection_reason(self):
        for index, reason in enumerate((None, "", "   ")):
            with self.subTest(reason=reason):
                path = (f"{self.tmp.name}/quarantine_{index}.parquet").replace(
                    "\\", "/"
                )
                self.con.execute(
                    "CREATE OR REPLACE TEMP TABLE invalid_quarantine "
                    "(rejection_reason VARCHAR)"
                )
                self.con.execute(
                    "INSERT INTO invalid_quarantine VALUES (?)",
                    [reason],
                )
                self.con.execute(
                    f"COPY invalid_quarantine TO '{path}' (FORMAT parquet)"
                )

                with self.assertRaisesRegex(ValueError, "rejection_reason"):
                    gold.load_quarantine_hour(self.con, path)

    def test_quality_distinguishes_slot_and_manifest_metrics(self):
        silver_rows = build_all(self.con, self.path)
        marker = {
            "status": "incomplete",
            "cycles": 2,
            "complete_cycles": 2,
            "expected_cycles": 2,
            "observed_slots": 1,
            "complete_slots": 1,
        }

        with patch(
            "silver_to_gold.read_completeness_marker",
            return_value=marker,
        ):
            completeness = gold.load_completeness("2026-01-15", "12")

        gold.build_quality_table(
            self.con,
            "2026-01-15",
            "12",
            completeness,
            silver_rows,
            True,
            RUN_AT,
        )

        row = self.con.execute(
            "SELECT manifest_cycles, complete_manifest_cycles, "
            "complete_cycle_ratio, expected_slots, observed_slots, "
            "complete_slots, slot_coverage_ratio, complete_slot_ratio "
            "FROM hourly_data_quality"
        ).fetchone()

        self.assertEqual(row, (2, 2, 1.0, 2, 1, 1, 0.5, 1.0))

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
            [
                ("bus", "174", 1, 1),
                ("bus", "523", 1, 2),
                ("bus", "unknown", 1, 1),
                ("tram", "523", 1, 1),
                ("tram", "unknown", 1, 1),
            ],
        )
        total = self.con.execute(
            "SELECT SUM(position_events) FROM hourly_line_metrics"
        ).fetchone()
        if total is None:
            self.fail("Expected an aggregate result for hourly_line_metrics")
        self.assertEqual(total[0], 6)

    def test_brigades_and_activity_span(self):
        build_all(self.con, self.path)
        self.assertEqual(
            self.con.execute(
                "SELECT active_brigades, silver_records FROM hourly_network_metrics"
            ).fetchone(),
            (3, 6),
        )
        row = self.con.execute(
            "SELECT unique_lines, active_seconds, first_lat, last_lon, "
            "min_lat, max_lon FROM vehicle_activity_hourly "
            "WHERE vehicle_type = 'bus' AND vehicle_number = '1000'"
        ).fetchone()
        self.assertEqual(row, (1, 60, 52.1, 21.0, 52.1, 21.0))

    def test_quality_safe_for_dashboard_rules(self):
        cases = [
            ("complete", ["stale_gps_time"] * 3 + ["duplicate"], True),
            ("complete", ["invalid_coordinates"] * 3, False),
            ("incomplete", ["duplicate"], False),
            ("unknown", [], False),
        ]
        for status, reasons, expected in cases:
            with self.subTest(status=status, reasons=reasons):
                build_all(self.con, self.path, status=status, reasons=reasons)
                row = self.con.execute(
                    "SELECT safe_for_dashboard, bronze_records, rejection_pct, "
                    "duplicate_records, partial_manifest_cycles "
                    "FROM hourly_data_quality"
                ).fetchone()
                if row is None:
                    self.fail("Expected a quality metrics row")
                self.assertIs(row[0], expected)
                self.assertEqual(row[1], 6 + len(reasons))
                self.assertAlmostEqual(
                    row[2], 100.0 * len(reasons) / (6 + len(reasons))
                )
                self.assertEqual(row[3], reasons.count("duplicate"))
                self.assertEqual(row[4], 0)

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
            "complete_manifest_cycles, complete_cycle_ratio, bronze_records, "
            "silver_position_events, quarantined_records, quarantined_by_reason, "
            "partial_manifest_cycles FROM hourly_data_quality"
        ).fetchone()
        if row is None:
            self.fail("Expected a quality metrics row")
        self.assertEqual(row[8], 3)
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
            result,
            {
                "status": "unknown",
                "cycles": None,
                "complete_cycles": None,
                "expected_cycles": None,
                "observed_slots": None,
                "complete_slots": None,
                "slot_coverage_ratio": None,
                "complete_slot_ratio": None,
            },
        )

    def test_complete_marker_rejects_zero_slot_counts(self):
        for expected, observed in ((0, 0), (120, 0)):
            with self.subTest(expected=expected, observed=observed):
                marker = {
                    "status": "complete",
                    "cycles": 0,
                    "complete_cycles": 0,
                    "expected_cycles": expected,
                    "observed_slots": observed,
                    "complete_slots": 0,
                }

                with (
                    patch(
                        "silver_to_gold.read_completeness_marker",
                        return_value=marker,
                    ),
                    self.assertRaises(ValueError),
                ):
                    gold.load_completeness("2026-01-15", "12")

    def test_complete_marker_accepts_exact_slot_thresholds(self):
        for observed, complete in ((114, 114), (120, 108)):
            with self.subTest(observed=observed, complete=complete):
                marker = {
                    "status": "complete",
                    "cycles": 120,
                    "complete_cycles": complete,
                    "expected_cycles": 120,
                    "observed_slots": observed,
                    "complete_slots": complete,
                }

                with patch(
                    "silver_to_gold.read_completeness_marker",
                    return_value=marker,
                ):
                    result = gold.load_completeness("2026-01-15", "12")

                self.assertEqual(result["status"], "complete")
                self.assertEqual(result["observed_slots"], observed)
                self.assertEqual(result["complete_slots"], complete)

    def test_complete_marker_with_insufficient_complete_slot_ratio_is_rejected(self):
        marker = {
            "status": "complete",
            "cycles": 120,
            "complete_cycles": 107,
            "expected_cycles": 120,
            "observed_slots": 120,
            "complete_slots": 107,
        }

        with (
            patch(
                "silver_to_gold.read_completeness_marker",
                return_value=marker,
            ),
            self.assertRaises(ValueError),
        ):
            gold.load_completeness("2026-01-15", "12")

    def test_complete_marker_with_insufficient_slot_coverage_is_rejected(self):
        marker = {
            "status": "complete",
            "cycles": 120,
            "complete_cycles": 120,
            "expected_cycles": 120,
            "observed_slots": 1,
            "complete_slots": 1,
        }

        with (
            patch(
                "silver_to_gold.read_completeness_marker",
                return_value=marker,
            ),
            self.assertRaises(ValueError),
        ):
            gold.load_completeness("2026-01-15", "12")

    def test_complete_slots_cannot_exceed_complete_manifest_count(self):
        marker = {
            "status": "incomplete",
            "cycles": 3,
            "complete_cycles": 1,
            "expected_cycles": 2,
            "observed_slots": 2,
            "complete_slots": 2,
        }

        with (
            patch(
                "silver_to_gold.read_completeness_marker",
                return_value=marker,
            ),
            self.assertRaises(ValueError),
        ):
            gold.load_completeness("2026-01-15", "12")

    def test_invalid_status_is_rejected(self):
        with (
            patch(
                "silver_to_gold.read_completeness_marker",
                return_value={"status": "ok"},
            ),
            self.assertRaises(ValueError),
        ):
            gold.load_completeness("2026-01-15", "12")

    def test_loads_slot_counts_separately_from_manifest_counts(self):
        marker = {
            "status": "incomplete",
            "cycles": 2,
            "complete_cycles": 2,
            "expected_cycles": 2,
            "observed_slots": 1,
            "complete_slots": 1,
        }

        with patch(
            "silver_to_gold.read_completeness_marker",
            return_value=marker,
        ):
            result = gold.load_completeness("2026-01-15", "12")

        self.assertEqual(result["cycles"], 2)
        self.assertEqual(result["complete_cycles"], 2)
        self.assertEqual(result["expected_cycles"], 2)
        self.assertEqual(result["observed_slots"], 1)
        self.assertEqual(result["complete_slots"], 1)
        self.assertEqual(result["slot_coverage_ratio"], 0.5)
        self.assertEqual(result["complete_slot_ratio"], 1.0)

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

    def test_naive_run_time_is_rejected_before_storage_access(self):
        s3 = Mock()
        stop_at_resolution = RuntimeError("Unexpected storage resolution")

        with (
            patch.object(
                gold,
                "partition_uris",
                side_effect=stop_at_resolution,
            ) as resolve,
            self.assertRaisesRegex(ValueError, "timezone-aware"),
        ):
            gold.build_gold_hour(
                self.con,
                "2026-01-15",
                "12",
                run_at=RUN_AT.replace(tzinfo=None),
                s3_client=s3,
            )

        resolve.assert_not_called()
        self.assertEqual(s3.mock_calls, [])

    def test_missing_silver_passes_client_to_diagnostic_manifest(self):
        s3 = Mock()

        with self.assertRaises(gold.SilverDataMissingError):
            self.run_build(silver=False, s3_client=s3)

        self.assertEqual(self.diagnostic_clients, [s3])

    def test_build_passes_s3_client_to_existence_check(self):
        s3 = Mock()
        silver_uri = "s3://silver/test/data.parquet"
        stop_after_check = RuntimeError("Stop at existence check")

        with (
            patch.object(
                gold,
                "partition_uris",
                return_value={"silver": silver_uri},
            ),
            patch.object(gold, "load_completeness", return_value={}),
            patch.object(
                gold,
                "s3_object_exists",
                side_effect=stop_after_check,
            ) as exists,
            self.assertRaises(RuntimeError) as raised,
        ):
            gold.build_gold_hour(
                self.con,
                "2026-01-15",
                "12",
                RUN_AT,
                s3_client=s3,
            )

        self.assertIs(raised.exception, stop_after_check)
        exists.assert_called_once_with(silver_uri, s3_client=s3)

    def test_build_passes_s3_client_to_partition_resolution(self):
        s3 = Mock()
        stop_after_resolution = RuntimeError("Stop after partition resolution")

        with (
            patch.object(
                gold,
                "partition_uris",
                side_effect=stop_after_resolution,
            ) as resolve,
            self.assertRaises(RuntimeError) as raised,
        ):
            gold.build_gold_hour(
                self.con,
                "2026-01-15",
                "12",
                RUN_AT,
                s3_client=s3,
            )

        self.assertIs(raised.exception, stop_after_resolution)
        resolve.assert_called_once_with("2026-01-15", "12", s3_client=s3)

    def test_legacy_silver_etag_is_captured_before_reading(self):
        silver_key = "vehicle_positions/date=2026-01-15/hour=12/data.parquet"
        silver_uri = f"s3://silver/{silver_key}"
        s3 = Mock()
        s3.head_object.return_value = {"ETag": '"source-etag"'}
        original_load = gold.load_silver_hour

        def load_after_capture(connection, uri):
            self.assertEqual(uri, silver_uri)
            s3.head_object.assert_called_once_with(
                Bucket="silver",
                Key=silver_key,
            )
            return original_load(connection, self.path)

        with (
            patch("silver_to_gold._s3_client", return_value=s3),
            patch(
                "silver_to_gold.load_silver_hour",
                side_effect=load_after_capture,
            ),
        ):
            self.run_build(path=silver_uri)

    def run_build(
        self, status="complete", silver=True, path=None, s3_client=None, run_at=RUN_AT
    ):
        marker = {"status": status, "cycles": 60, "complete_cycles": 60}
        silver_path = path or self.path

        def fake_uris(date_str, hour_str, s3_client=None):
            partition = f"date={date_str}/hour={hour_str}"
            publication_id = "test-publication"

            return {
                "silver": silver_path,
                "quarantine": f"{silver_path.rsplit('/', 1)[0]}/quarantine.parquet",
                "gold": {
                    table: (
                        f"s3://gold/_versions/{partition}/{publication_id}/"
                        f"{table}/data.parquet"
                    )
                    for table in gold.GOLD_TABLES
                },
                "staging": {
                    table: (
                        f"s3://gold/_staging/{partition}/{publication_id}/"
                        f"{table}.parquet.pending"
                    )
                    for table in gold.GOLD_TABLES
                },
                "publication_id": publication_id,
                "partition": partition,
                "manifest_uri": gold.publication_manifest_uri(date_str, hour_str),
            }

        with (
            patch("silver_to_gold.read_completeness_marker", return_value=marker),
            patch("silver_to_gold.write_diagnostic_manifest") as write_diagnostic,
            patch(
                "silver_to_gold.s3_object_exists",
                side_effect=lambda uri, s3_client=None: (
                    silver and "quarantine" not in uri
                ),
            ),
            patch("silver_to_gold.partition_uris", side_effect=fake_uris),
            patch(
                "silver_to_gold.publish_tables",
                side_effect=lambda con, tables, uris, s3_client=None: {
                    t: 1 for t in tables
                },
            ) as publish,
        ):
            try:
                result = gold.build_gold_hour(
                    self.con,
                    "2026-01-15",
                    "12",
                    run_at,
                    s3_client=s3_client,
                )
            except Exception:
                self.publish_calls = [c.args[1] for c in publish.call_args_list]
                self.diagnostic_clients = [
                    call.kwargs.get("s3_client")
                    for call in write_diagnostic.call_args_list
                ]
                raise
        self.publish_calls = [c.args[1] for c in publish.call_args_list]
        self.publish_clients = [
            call.kwargs.get("s3_client") for call in publish.call_args_list
        ]
        return result

    def test_aware_run_time_is_normalized_to_utc(self):
        local_run_at = RUN_AT.astimezone(timezone(timedelta(hours=2)))

        with patch.object(
            gold,
            "is_hour_final",
            wraps=gold.is_hour_final,
        ) as check_final:
            self.run_build(run_at=local_run_at)

        self.assertTrue(check_final.called)
        for operation in check_final.call_args_list:
            checked_time = operation.args[2]
            self.assertEqual(checked_time, RUN_AT)
            self.assertIs(checked_time.tzinfo, timezone.utc)

    def test_build_passes_s3_client_to_publication(self):
        s3 = Mock()

        self.run_build(s3_client=s3)

        self.assertEqual(self.publish_clients, [s3])

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
    def test_partition_uris_use_versioned_gold_and_staging(self):
        with patch(
            "silver_to_gold.resolve_silver_artifacts",
            return_value={
                "data": "s3://silver/vehicle_positions/date=2026-01-15/hour=09/data.parquet",
                "quarantine": "s3://silver/vehicle_positions/date=2026-01-15/hour=09/quarantine.parquet",
                "completeness": "s3://silver/vehicle_positions/date=2026-01-15/hour=09/_completeness.json",
            },
        ):
            uris = gold.partition_uris("2026-01-15", "09")

        self.assertIn("s3://gold/_versions/", uris["gold"]["hourly_network_metrics"])
        self.assertIn(
            "/date=2026-01-15/hour=09/", uris["gold"]["hourly_network_metrics"]
        )
        self.assertTrue(
            uris["gold"]["hourly_network_metrics"].endswith(
                "/hourly_network_metrics/data.parquet"
            )
        )

        self.assertTrue(uris["publication_id"])
        self.assertEqual(uris["partition"], "date=2026-01-15/hour=09")

        for table in gold.GOLD_TABLES:
            self.assertTrue(uris["staging"][table].endswith(".parquet.pending"))
            self.assertIn("/_staging/", uris["staging"][table])
            self.assertIn(uris["publication_id"], uris["staging"][table])

    def test_publication_passes_s3_client_to_commit_manifest(self):
        con = Mock()
        con.execute.return_value.fetchone.return_value = (1,)
        s3 = Mock()
        uris = {
            "partition": "date=2026-01-15/hour=12",
            "publication_id": "test-publication",
            "silver": "s3://silver/test/data.parquet",
            "manifest_uri": "s3://gold/_commits/test/manifest.json",
            "staging": {
                table: f"s3://gold/_staging/test/{table}.parquet"
                for table in gold.GOLD_TABLES
            },
            "gold": {
                table: f"s3://gold/_versions/test/{table}.parquet"
                for table in gold.GOLD_TABLES
            },
        }

        with (
            patch.object(gold, "validate_staged_file"),
            patch.object(gold, "copy_s3_object"),
            patch.object(gold, "delete_s3_object"),
            patch.object(gold, "write_publication_manifest") as write_manifest,
        ):
            gold.publish_tables(con, gold.GOLD_TABLES, uris, s3_client=s3)

        write_manifest.assert_called_once()
        self.assertEqual(write_manifest.call_args.args[0], uris["manifest_uri"])
        self.assertIs(write_manifest.call_args.kwargs.get("s3_client"), s3)

    def test_manifest_writers_use_supplied_s3_client(self):
        cases = (
            (
                gold.write_publication_manifest,
                "_commits/date=2026-01-15/hour=12/manifest.json",
            ),
            (
                gold.write_diagnostic_manifest,
                "_diagnostics/date=2026-01-15/hour=12/manifest.json",
            ),
        )
        manifest = {"partition": "date=2026-01-15/hour=12"}

        for writer, key in cases:
            with self.subTest(writer=writer.__name__):
                s3 = Mock()

                with patch.object(gold, "_s3_client") as client_factory:
                    writer(
                        f"s3://gold/{key}",
                        manifest,
                        s3_client=s3,
                    )

                s3.put_object.assert_called_once_with(
                    Bucket="gold",
                    Key=key,
                    Body=json.dumps(manifest, sort_keys=True).encode("utf-8"),
                    ContentType="application/json",
                )
                client_factory.assert_not_called()

    def test_publish_tables_passes_s3_client_to_copy(self):
        con = Mock()
        con.execute.return_value.fetchone.return_value = (1,)
        s3 = Mock()
        table = "hourly_data_quality"
        staging_uri = "s3://gold/_staging/test/quality.parquet.pending"
        output_uri = "s3://gold/_versions/test/quality.parquet"
        uris = {
            "staging": {table: staging_uri},
            "gold": {table: output_uri},
        }

        with (
            patch.object(gold, "validate_staged_file"),
            patch.object(gold, "copy_s3_object") as copy_object,
            patch.object(gold, "delete_s3_object") as delete_object,
        ):
            counts = gold.publish_tables(
                con,
                (table,),
                uris,
                s3_client=s3,
            )

        self.assertEqual(counts, {table: 1})
        copy_object.assert_called_once_with(
            staging_uri,
            output_uri,
            s3_client=s3,
        )
        delete_object.assert_called_once_with(
            staging_uri,
            s3_client=s3,
        )

    def test_partition_uris_uses_supplied_s3_client(self):
        s3 = Mock()
        artifacts = {
            "data": "s3://silver/test/data.parquet",
            "quarantine": "s3://silver/test/quarantine.parquet",
        }

        with patch.object(
            gold,
            "resolve_silver_artifacts",
            return_value=artifacts,
        ) as resolve:
            uris = gold.partition_uris(
                "2026-01-15",
                "09",
                s3_client=s3,
            )

        resolve.assert_called_once_with("2026-01-15", "09", s3_client=s3)
        self.assertEqual(uris["silver"], artifacts["data"])
        self.assertEqual(uris["quarantine"], artifacts["quarantine"])

    def test_partition_uris_include_commit_manifest(self):
        with patch(
            "silver_to_gold.resolve_silver_artifacts",
            return_value={
                "data": "s3://silver/vehicle_positions/date=2026-01-15/hour=09/data.parquet",
                "quarantine": "s3://silver/vehicle_positions/date=2026-01-15/hour=09/quarantine.parquet",
                "completeness": "s3://silver/vehicle_positions/date=2026-01-15/hour=09/_completeness.json",
            },
        ):
            uris = gold.partition_uris("2026-01-15", "09")

        self.assertEqual(
            uris["manifest_uri"],
            gold.publication_manifest_uri("2026-01-15", "09"),
        )
        self.assertIn("/_commits/", uris["manifest_uri"])
        self.assertTrue(uris["manifest_uri"].endswith("/manifest.json"))


class LocalPipelineTestCase(unittest.TestCase):
    """Runs build_gold_hour end to end against a local directory instead of S3."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = self.tmp.name.replace("\\", "/")
        self.silver = make_silver(self.tmp.name)
        self.quarantine = f"{self.root}/quarantine.parquet"
        self.con = duckdb.connect()
        self.con.execute("SET TimeZone='UTC'")
        self.addCleanup(self.con.close)
        self.status = "complete"

    def write_quarantine(self, reasons):
        values = ", ".join(f"('{r}')" for r in reasons)
        tmp = duckdb.connect()
        tmp.execute(
            f"COPY (SELECT * FROM (VALUES {values}) t(rejection_reason)) "
            f"TO '{self.quarantine}' (FORMAT parquet)"
        )
        tmp.close()

    def write_local_manifest(self, manifest_uri, manifest, s3_client=None):
        path = manifest_uri.replace("\\", "/")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as file:
            json.dump(manifest, file, sort_keys=True)

    # Generates local URIs for gold, staging, and manifest files.
    def local_uris(self, date_str, hour_str, s3_client=None):
        partition = f"date={date_str}/hour={hour_str}"
        publication_id = gold.uuid.uuid4().hex

        gold_uris = {
            table: (
                f"{self.root}/gold/_versions/{partition}/"
                f"{publication_id}/{table}/data.parquet"
            )
            for table in gold.GOLD_TABLES
        }
        staging_uris = {
            table: (
                f"{self.root}/gold/_staging/{partition}/"
                f"{publication_id}/{table}.parquet.pending"
            )
            for table in gold.GOLD_TABLES
        }

        # DuckDB wymaga, aby lokalne katalogi docelowe już istniały.
        for uri in gold_uris.values():
            os.makedirs(os.path.dirname(uri), exist_ok=True)

        for uri in staging_uris.values():
            os.makedirs(os.path.dirname(uri), exist_ok=True)

        manifest_uri = f"{self.root}/gold/_commits/{partition}/manifest.json"
        os.makedirs(os.path.dirname(manifest_uri), exist_ok=True)
        diagnostic_uri = f"{self.root}/gold/_diagnostics/{partition}/manifest.json"
        os.makedirs(os.path.dirname(diagnostic_uri), exist_ok=True)
        return {
            "silver": self.silver,
            "quarantine": self.quarantine,
            "gold": gold_uris,
            "staging": staging_uris,
            "publication_id": publication_id,
            "partition": partition,
            "manifest_uri": manifest_uri,
            "diagnostic_manifest_uri": diagnostic_uri,
        }

    @staticmethod
    def fake_copy(src, dst, s3_client=None):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copyfile(src, dst)

    @staticmethod
    def fake_delete(uri, s3_client=None):
        if os.path.exists(uri):
            os.remove(uri)

    def build(self, run_at=RUN_AT, validate=None, copy_fn=None, manifest_fn=None):
        os.makedirs(f"{self.root}/staging", exist_ok=True)
        marker = {"status": self.status, "cycles": 60, "complete_cycles": 60}
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                patch(
                    "silver_to_gold.write_publication_manifest",
                    side_effect=manifest_fn or self.write_local_manifest,
                )
            )
            stack.enter_context(
                patch(
                    "silver_to_gold.diagnostic_manifest_uri",
                    side_effect=lambda date_str, hour_str: (
                        f"{self.root}/gold/_diagnostics/"
                        f"date={date_str}/hour={hour_str}/manifest.json"
                    ),
                )
            )
            stack.enter_context(
                patch(
                    "silver_to_gold.write_diagnostic_manifest",
                    side_effect=self.write_local_manifest,
                )
            )
            stack.enter_context(
                patch("silver_to_gold.read_completeness_marker", return_value=marker)
            )
            stack.enter_context(
                patch(
                    "silver_to_gold.s3_object_exists",
                    side_effect=lambda uri, s3_client=None: os.path.exists(uri),
                )
            )
            stack.enter_context(
                patch("silver_to_gold.partition_uris", side_effect=self.local_uris)
            )
            stack.enter_context(
                patch(
                    "silver_to_gold.copy_s3_object",
                    side_effect=copy_fn or self.fake_copy,
                )
            )
            stack.enter_context(
                patch("silver_to_gold.delete_s3_object", side_effect=self.fake_delete)
            )
            if validate is not None:
                stack.enter_context(
                    patch("silver_to_gold.validate_gold_tables", validate)
                )
            return gold.build_gold_hour(self.con, "2026-01-15", "12", run_at)

    def published(self):
        return sorted(
            os.path.relpath(os.path.join(d, f), self.root)
            for d, _, files in os.walk(f"{self.root}/gold")
            for f in files
        )

    def _manifest_table_uri(self, table):
        manifest_uri = (
            f"{self.root}/gold/_commits/date=2026-01-15/hour=12/manifest.json"
        )
        with open(manifest_uri, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        return manifest["tables"][table]["uri"]

    def read_gold(self, table, exclude=("pipeline_run_at",)):
        uri = self._manifest_table_uri(table)

        cols = [
            row[0]
            for row in self.con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{uri}', hive_partitioning=false)"
            ).fetchall()
            if row[0] not in exclude
        ]
        sel = ", ".join(f'"{col}"' for col in cols)

        return self.con.execute(
            f"SELECT {sel} FROM "
            f"read_parquet('{uri}', hive_partitioning=false) "
            "ORDER BY ALL"
        ).fetchall()


class GoldProcessTests(LocalPipelineTestCase):
    def test_all_tables_are_published_once(self):
        self.write_quarantine(["duplicate", "stale_gps_time"])
        result = self.build()
        self.assertEqual(result["status"], "complete")
        parquet_files = [path for path in self.published() if path.endswith(".parquet")]
        self.assertEqual(len(parquet_files), len(gold.GOLD_TABLES))
        self.assertEqual(os.listdir(f"{self.root}/staging"), [])

    def test_manifest_records_source_silver_etag(self):
        self.write_quarantine(["duplicate", "stale_gps_time"])
        original_local_uris = self.local_uris

        def local_uris_with_etag(date_str, hour_str, s3_client=None):
            uris = original_local_uris(date_str, hour_str, s3_client=s3_client)
            uris["source_silver_etag"] = '"source-etag"'
            return uris

        with patch.object(
            self,
            "local_uris",
            side_effect=local_uris_with_etag,
        ):
            self.build()

        manifest_path = (
            f"{self.root}/gold/_commits/date=2026-01-15/hour=12/manifest.json"
        )
        with open(manifest_path, encoding="utf-8") as file:
            manifest = json.load(file)

        self.assertEqual(manifest["source_silver_etag"], '"source-etag"')

    def test_rerun_is_idempotent_and_creates_no_duplicates(self):
        self.write_quarantine(["duplicate", "stale_gps_time"])
        self.build()

        first = {t: self.read_gold(t) for t in gold.GOLD_TABLES}
        manifest_uri = (
            f"{self.root}/gold/_commits/date=2026-01-15/hour=12/manifest.json"
        )
        with open(manifest_uri, "r", encoding="utf-8") as f:
            first_manifest = json.load(f)

        later = RUN_AT.replace(minute=45)
        self.build(run_at=later)

        with open(manifest_uri, "r", encoding="utf-8") as f:
            second_manifest = json.load(f)

        self.assertNotEqual(
            first_manifest["publication_id"],
            second_manifest["publication_id"],
        )
        self.assertEqual(
            set(second_manifest["tables"]),
            set(gold.GOLD_TABLES),
        )

        for table in gold.GOLD_TABLES:
            self.assertEqual(self.read_gold(table), first[table], table)
            self.assertTrue(len(first[table]) > 0)
            self.assertIn("uri", second_manifest["tables"][table])
            self.assertTrue(os.path.exists(second_manifest["tables"][table]["uri"]))

        self.assertEqual(len(first["hourly_network_metrics"]), 1)
        self.assertEqual(len(first["hourly_data_quality"]), 1)

    def test_manifest_finality_matches_published_quality(self):
        self.write_quarantine(["duplicate", "stale_gps_time"])

        for minute, expected_final in ((14, False), (15, True)):
            with self.subTest(minute=minute):
                self.build(run_at=RUN_AT.replace(minute=minute))

                manifest_path = (
                    f"{self.root}/gold/_commits/date=2026-01-15/hour=12/manifest.json"
                )
                with open(manifest_path, encoding="utf-8") as file:
                    manifest = json.load(file)

                self.assertIs(manifest["is_final"], expected_final)

                quality_uri = manifest["tables"]["hourly_data_quality"]["uri"]
                actual_final = self.con.execute(
                    "SELECT is_final FROM read_parquet("
                    f"'{quality_uri}', hive_partitioning=false)"
                ).fetchone()[0]
                self.assertIs(actual_final, expected_final)

    def test_manifest_records_source_silver_uri(self):
        self.write_quarantine(["duplicate", "stale_gps_time"])
        self.build()

        manifest_path = (
            f"{self.root}/gold/_commits/date=2026-01-15/hour=12/manifest.json"
        )
        with open(manifest_path, encoding="utf-8") as file:
            manifest = json.load(file)

        self.assertEqual(manifest["source_silver_uri"], self.silver)

    def test_missing_silver_keeps_previous_full_manifest(self):
        self.build()

        manifest_uri = (
            f"{self.root}/gold/_commits/date=2026-01-15/hour=12/manifest.json"
        )
        with open(manifest_uri, "r", encoding="utf-8") as file:
            previous_manifest = json.load(file)

        # Simulate a missing Silver partition on the next attempt.
        self.silver = os.path.join(self.tmp.name, "missing-silver.parquet")

        with self.assertRaises(gold.SilverDataMissingError):
            self.build()

        with open(manifest_uri, "r", encoding="utf-8") as file:
            current_manifest = json.load(file)

        self.assertEqual(current_manifest, previous_manifest)
        self.assertEqual(set(current_manifest["tables"]), set(gold.GOLD_TABLES))

        diagnostic_uri = (
            f"{self.root}/gold/_diagnostics/date=2026-01-15/hour=12/manifest.json"
        )
        with open(diagnostic_uri, "r", encoding="utf-8") as file:
            diagnostic_manifest = json.load(file)

        self.assertEqual(diagnostic_manifest["status"], "silver_missing")
        self.assertEqual(
            set(diagnostic_manifest["tables"]),
            {"hourly_data_quality"},
        )

    def test_empty_silver_writes_diagnostic_manifest(self):
        self.build()

        manifest_uri = (
            f"{self.root}/gold/_commits/date=2026-01-15/hour=12/manifest.json"
        )
        with open(manifest_uri, "r", encoding="utf-8") as file:
            previous_manifest = json.load(file)

        self.silver = make_silver(self.tmp.name, SILVER_SQL + " LIMIT 0")

        with self.assertRaisesRegex(ValueError, "empty"):
            self.build()

        with open(manifest_uri, "r", encoding="utf-8") as file:
            current_manifest = json.load(file)

        self.assertEqual(current_manifest, previous_manifest)

        diagnostic_uri = (
            f"{self.root}/gold/_diagnostics/date=2026-01-15/hour=12/manifest.json"
        )
        with open(diagnostic_uri, "r", encoding="utf-8") as file:
            diagnostic_manifest = json.load(file)

        self.assertEqual(diagnostic_manifest["status"], "silver_empty")
        self.assertEqual(
            set(diagnostic_manifest["tables"]),
            {"hourly_data_quality"},
        )

    def test_hour_is_not_final_during_grace_period(self):
        self.build(run_at=datetime(2026, 1, 15, 13, 5, tzinfo=timezone.utc))
        quality = self.read_gold("hourly_data_quality")
        cols = [c for c, _ in gold.GOLD_SCHEMAS["hourly_data_quality"]]
        row = dict(zip(cols, quality[0]))
        self.assertIs(row["is_final"], False)
        self.assertIs(row["safe_for_dashboard"], False)
        self.assertIsNotNone(row["source_max_ingested_at"])

    def test_is_hour_final_boundaries(self):
        f = gold.is_hour_final
        utc = timezone.utc
        self.assertFalse(
            f("2026-01-15", "12", datetime(2026, 1, 15, 12, 30, tzinfo=utc))
        )
        self.assertFalse(
            f("2026-01-15", "12", datetime(2026, 1, 15, 13, 14, tzinfo=utc))
        )
        self.assertTrue(
            f("2026-01-15", "12", datetime(2026, 1, 15, 13, 15, tzinfo=utc))
        )

    def test_rerun_with_changed_silver_replaces_not_appends(self):
        self.build()
        self.silver = make_silver(
            self.tmp.name, SILVER_SQL.replace("('tram', '2000'", "('tram', '2001'")
        )
        self.build()
        trams = self.con.execute(
            "SELECT vehicle_number "
            f"FROM read_parquet('{self._manifest_table_uri('vehicle_activity_hourly')}', "
            "hive_partitioning=false) "
            "WHERE vehicle_type = 'tram' ORDER BY 1"
        ).fetchall()

        self.assertEqual(trams, [("1000",), ("2001",)])

    def test_incomplete_status_is_never_published_as_complete(self):
        self.status = "incomplete"
        self.write_quarantine(["duplicate"])

        with self.assertLogs("silver_to_gold", level="WARNING"):
            result = self.build()

        self.assertEqual(result["status"], "incomplete")

        for table in ("hourly_network_metrics", "hourly_data_quality"):
            statuses = {
                row[0]
                for row in self.con.execute(
                    f"SELECT data_completeness_status "
                    f"FROM read_parquet('{self._manifest_table_uri(table)}', "
                    "hive_partitioning=false)"
                ).fetchall()
            }
            self.assertEqual(statuses, {"incomplete"}, table)

        quality_uri = self._manifest_table_uri("hourly_data_quality")
        safe = self.con.execute(
            f"SELECT safe_for_dashboard "
            f"FROM read_parquet('{quality_uri}', "
            "hive_partitioning=false)"
        ).fetchone()
        if safe is None:
            self.fail("Expected a dashboard safety result")

        self.assertIs(safe[0], False)

    def test_quarantined_duplicates_do_not_inflate_silver_metrics(self):
        self.write_quarantine(["duplicate"] * 5)
        self.build()

        totals = self.con.execute(
            "SELECT SUM(active_vehicles), SUM(position_events), SUM(silver_records) "
            f"FROM read_parquet('{self._manifest_table_uri('hourly_network_metrics')}', "
            "hive_partitioning=false)"
        ).fetchone()

        self.assertEqual(totals, (5, 6, 6))

        quality = self.con.execute(
            "SELECT bronze_records, silver_position_events, duplicate_records "
            f"FROM read_parquet('{self._manifest_table_uri('hourly_data_quality')}', "
            "hive_partitioning=false)"
        ).fetchone()

        self.assertEqual(quality, (11, 6, 5))

    def test_validation_failure_publishes_nothing(self):
        def boom(con, tables):
            raise ValueError("Gold consistency checks failed: forced")

        with self.assertRaisesRegex(ValueError, "forced"):
            self.build(validate=boom)
        self.assertEqual(self.published(), [])

    def test_failed_staging_validation_publishes_nothing_and_cleans_up(self):
        calls = []

        def fail_on_third(con, table, uri, expected):
            calls.append(table)
            if len(calls) == 3:
                raise ValueError("staged file is corrupt")

        with (
            patch("silver_to_gold.validate_staged_file", side_effect=fail_on_third),
            self.assertRaisesRegex(ValueError, "corrupt"),
        ):
            self.build()
        self.assertEqual(self.published(), [])
        self.assertEqual(os.listdir(f"{self.root}/staging"), [])

    def test_failure_on_rerun_keeps_previous_publication(self):
        self.build()
        before = {t: self.read_gold(t) for t in gold.GOLD_TABLES}

        def boom(con, tables):
            raise ValueError("forced")

        with self.assertRaises(ValueError):
            self.build(validate=boom)
        for table in gold.GOLD_TABLES:
            self.assertEqual(self.read_gold(table), before[table], table)

    def test_failed_manifest_write_keeps_previous_publication(self):
        self.build()

        before = {
            table: self.read_gold(table, exclude=()) for table in gold.GOLD_TABLES
        }

        manifest_uri = (
            f"{self.root}/gold/_commits/date=2026-01-15/hour=12/manifest.json"
        )
        with open(manifest_uri, "r", encoding="utf-8") as file:
            previous_manifest = json.load(file)

        def fail_manifest(uri, manifest, s3_client=None):
            raise RuntimeError("forced manifest write failure")

        next_run_at = RUN_AT + timedelta(seconds=1)

        with self.assertRaisesRegex(RuntimeError, "forced manifest write failure"):
            self.build(
                run_at=next_run_at,
                manifest_fn=fail_manifest,
            )

        with open(manifest_uri, "r", encoding="utf-8") as file:
            current_manifest = json.load(file)

        self.assertEqual(current_manifest, previous_manifest)

        after = {table: self.read_gold(table, exclude=()) for table in gold.GOLD_TABLES}
        self.assertEqual(after, before)

    def test_failed_publish_copy_keeps_previous_publication(self):
        # First publish: establish a known-good Gold state.
        self.build()
        before = {
            table: self.read_gold(table, exclude=()) for table in gold.GOLD_TABLES
        }

        calls = []

        def fail_on_second_copy(src, dst, s3_client=None):
            calls.append(dst)
            if len(calls) == 2:
                raise RuntimeError("forced publish failure")
            self.fake_copy(src, dst, s3_client=s3_client)

        # Change run_at so the next publication contains different metadata.
        next_run_at = RUN_AT + timedelta(seconds=1)

        with self.assertRaisesRegex(RuntimeError, "forced publish failure"):
            self.build(run_at=next_run_at, copy_fn=fail_on_second_copy)

        self.assertEqual(len(calls), 2)

        after = {table: self.read_gold(table, exclude=()) for table in gold.GOLD_TABLES}
        self.assertEqual(after, before)


class GoldSilverContractTests(LocalPipelineTestCase):
    def assert_rejected_without_publication(self, sql):
        self.silver = make_silver(self.tmp.name, sql)
        with self.assertRaises(ValueError):
            self.build()
        self.assertEqual(self.published(), [])

    def test_invalid_gps_age_is_rejected(self):
        cases = (
            ("future", "gps_time_utc - INTERVAL 1 SECOND"),
            ("stale", "gps_time_utc + INTERVAL 301 SECOND"),
        )

        for name, ingestion_time in cases:
            with self.subTest(case=name):
                self.assert_rejected_without_publication(
                    SILVER_SQL + f"; UPDATE src SET ingested_at = {ingestion_time}"
                )

    def test_gps_age_boundaries_are_accepted(self):
        self.silver = make_silver(
            self.tmp.name,
            SILVER_SQL
            + """
            ; UPDATE src
            SET ingested_at = CASE
                WHEN vehicle_type = 'bus' THEN gps_time_utc
                ELSE gps_time_utc + INTERVAL 5 MINUTE
            END
            """,
        )

        self.build()

        rows = self.con.execute("SELECT COUNT(*) FROM silver_hour").fetchone()[0]
        self.assertEqual(rows, 6)
        self.assertTrue(self.published())

    def test_nonfinite_coordinates_are_rejected(self):
        for column in ("lat", "lon"):
            for value in ("NaN", "Infinity", "-Infinity"):
                with self.subTest(column=column, value=value):
                    self.assert_rejected_without_publication(
                        SILVER_SQL
                        + f"; UPDATE src SET {column} = CAST('{value}' AS DOUBLE)"
                    )

    def test_missing_ingestion_time_is_rejected(self):
        self.assert_rejected_without_publication(
            SILVER_SQL + "; UPDATE src SET ingested_at = NULL"
        )

    def test_coordinates_on_allowed_boundaries_are_accepted(self):
        self.silver = make_silver(
            self.tmp.name,
            SILVER_SQL
            + """
            ; UPDATE src
            SET lat = CASE
                    WHEN vehicle_type = 'bus' THEN 51.9
                    ELSE 52.6
                END,
                lon = CASE
                    WHEN vehicle_type = 'bus' THEN 20.5
                    ELSE 21.6
                END
            """,
        )

        self.build()

        rows = self.con.execute("SELECT COUNT(*) FROM silver_hour").fetchone()[0]
        self.assertEqual(rows, 6)
        self.assertTrue(self.published())

    def test_coordinates_outside_allowed_area_are_rejected(self):
        cases = (
            ("lat", 51.8),
            ("lat", 52.7),
            ("lon", 20.4),
            ("lon", 21.7),
        )

        for column, value in cases:
            with self.subTest(column=column, value=value):
                self.assert_rejected_without_publication(
                    SILVER_SQL + f"; UPDATE src SET {column} = {value}"
                )

    def test_missing_column_is_rejected(self):
        self.assert_rejected_without_publication(
            "CREATE TABLE src AS SELECT vehicle_type, vehicle_number, line, "
            "brigade, lat, lon, gps_time_utc FROM (VALUES "
            "('bus', '1', '1', '1', 52.1e0, 21.0e0, "
            "TIMESTAMPTZ '2026-01-15 12:01:00+00')) "
            "t(vehicle_type, vehicle_number, line, brigade, lat, lon, gps_time_utc)"
        )

    def test_wrong_column_type_is_rejected(self):
        self.assert_rejected_without_publication(
            "CREATE TABLE src AS SELECT 'bus' AS vehicle_type, '1' AS vehicle_number, "
            "'1' AS line, '1' AS brigade, 'north' AS lat, 21.0e0 AS lon, "
            "TIMESTAMPTZ '2026-01-15 12:01:00+00' AS gps_time_utc, "
            "TIMESTAMPTZ '2026-01-15 12:01:10+00' AS ingested_at"
        )

    def test_unexpected_vehicle_type_is_rejected(self):
        self.assert_rejected_without_publication(
            SILVER_SQL.replace("('tram', '2000'", "('metro', '2000'")
        )

    def test_null_coordinates_are_rejected(self):
        self.assert_rejected_without_publication(
            SILVER_SQL.replace(
                "'b1', 52.1e0, 21.0e0, TIMESTAMPTZ '2026-01-15 12:05",
                "'b1', NULL, 21.0e0, TIMESTAMPTZ '2026-01-15 12:05",
            )
        )

    def test_duplicate_observations_are_rejected(self):
        self.assert_rejected_without_publication(
            SILVER_SQL.replace("'2026-01-15 12:02:00+00'", "'2026-01-15 12:01:00+00'")
        )

    def test_empty_line_is_kept_as_unknown_not_dropped(self):
        self.build()

        lines = dict(
            self.con.execute(
                "SELECT line, SUM(position_events) "
                f"FROM read_parquet('{self._manifest_table_uri('hourly_line_metrics')}', "
                "hive_partitioning=false) "
                "GROUP BY line"
            ).fetchall()
        )

        self.assertEqual(lines["unknown"], 2)
        self.assertEqual(sum(lines.values()), 6)


class GoldCommandLineTests(unittest.TestCase):
    def test_close_failure_does_not_hide_build_failure(self):
        connection = Mock()
        build_error = ValueError("Simulated Gold failure")
        connection.close.side_effect = duckdb.Error("Simulated close failure")

        with (
            patch.object(gold, "get_connection", return_value=connection),
            patch.object(gold, "build_gold_hour", side_effect=build_error),
            patch.object(
                sys,
                "argv",
                ["silver_to_gold.py", "--date", "2026-01-15", "--hour", "12"],
            ),
            self.assertRaises(ValueError) as raised,
        ):
            gold.main()

        self.assertIs(raised.exception, build_error)
        connection.close.assert_called_once_with()

    def test_close_failure_after_success_is_reported(self):
        connection = Mock()
        close_error = duckdb.Error("Simulated close failure")
        connection.close.side_effect = close_error

        with (
            patch.object(gold, "get_connection", return_value=connection),
            patch.object(gold, "build_gold_hour") as build_gold,
            patch.object(
                sys,
                "argv",
                ["silver_to_gold.py", "--date", "2026-01-15", "--hour", "12"],
            ),
            self.assertLogs(gold.logger, level="ERROR"),
            self.assertRaises(duckdb.Error) as raised,
        ):
            gold.main()

        self.assertIs(raised.exception, close_error)
        build_gold.assert_called_once_with(connection, "2026-01-15", "12")
        connection.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
