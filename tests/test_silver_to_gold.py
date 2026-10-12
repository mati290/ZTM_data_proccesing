import contextlib
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import duckdb
from botocore.exceptions import ClientError

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
        ).fetchone()[0]
        self.assertEqual(total, 6)

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
            patch("silver_to_gold.gold_hour_lock", return_value=contextlib.nullcontext()),
            patch("silver_to_gold.delete_s3_object"),
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



class GoldHourLockTests(unittest.TestCase):
    def test_lock_uses_conditional_create_and_releases_owned_lock(self):
        client = Mock()
        client.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=b'{"token": "same-token"}')),
            "ETag": '"gold-etag"',
        }
        with patch("silver_to_gold.uuid.uuid4") as uuid4:
            uuid4.return_value.hex = "same-token"
            with gold.gold_hour_lock("2026-10-09", "07", client):
                pass

        client.put_object.assert_called_once()
        self.assertEqual(client.put_object.call_args.kwargs["IfNoneMatch"], "*")
        self.assertEqual(client.put_object.call_args.kwargs["Bucket"], "gold")
        client.delete_object.assert_called_once_with(
            Bucket="gold",
            Key="_locks/gold/date=2026-10-09/hour=07/writer.json",
            IfMatch='"gold-etag"',
        )

    def test_lock_collision_fails_closed_without_deleting_existing_lock(self):
        client = Mock()
        client.put_object.side_effect = ClientError(
            {
                "Error": {"Code": "PreconditionFailed"},
                "ResponseMetadata": {"HTTPStatusCode": 412},
            },
            "PutObject",
        )
        with (
            self.assertRaises(gold.GoldHourLockedError),
            gold.gold_hour_lock("2026-10-09", "07", client),
        ):
            self.fail("must not enter while another writer owns the hour")
        client.delete_object.assert_not_called()
        client.get_object.assert_not_called()

    def test_lock_refuses_to_release_when_ownership_changed(self):
        client = Mock()
        client.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=b'{"token": "someone-else"}')),
            "ETag": '"other-etag"',
        }
        with (
            patch("silver_to_gold.uuid.uuid4") as uuid4,
            self.assertRaisesRegex(RuntimeError, "ownership changed"),
            gold.gold_hour_lock("2026-10-09", "07", client),
        ):
            uuid4.return_value.hex = "our-token"
        client.delete_object.assert_not_called()


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
        self.copy_calls = 0
        self.fail_copy_on = None

    def write_quarantine(self, reasons):
        values = ", ".join(f"('{r}')" for r in reasons)
        tmp = duckdb.connect()
        tmp.execute(
            f"COPY (SELECT * FROM (VALUES {values}) t(rejection_reason)) "
            f"TO '{self.quarantine}' (FORMAT parquet)"
        )
        tmp.close()

    def local_uris(self, date_str, hour_str):
        partition = f"date={date_str}/hour={hour_str}"
        return {
            "silver": self.silver,
            "quarantine": self.quarantine,
            "gold": {
                t: f"{self.root}/gold/{t}/{partition}/data.parquet"
                for t in gold.GOLD_TABLES
            },
            "staging": {
                t: f"{self.root}/staging/{t}.parquet.pending" for t in gold.GOLD_TABLES
            },
        }

    def fake_copy(self, src, dst):
        self.copy_calls += 1
        if self.fail_copy_on == self.copy_calls:
            raise OSError("forced Gold object copy failure")
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copyfile(src, dst)

    @staticmethod
    def fake_delete(uri):
        if os.path.exists(uri):
            os.remove(uri)

    def build(self, run_at=RUN_AT, validate=None):
        os.makedirs(f"{self.root}/staging", exist_ok=True)
        marker = {"status": self.status, "cycles": 60, "complete_cycles": 60}
        patches = [
            patch("silver_to_gold.read_completeness_marker", return_value=marker),
            patch("silver_to_gold.s3_object_exists", side_effect=os.path.exists),
            patch("silver_to_gold.partition_uris", side_effect=self.local_uris),
            patch("silver_to_gold.gold_hour_lock", return_value=contextlib.nullcontext()),
            patch("silver_to_gold.copy_s3_object", side_effect=self.fake_copy),
            patch("silver_to_gold.delete_s3_object", side_effect=self.fake_delete),
        ]
        if validate is not None:
            patches.append(patch("silver_to_gold.validate_gold_tables", validate))
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            return gold.build_gold_hour(self.con, "2026-01-15", "12", run_at)

    def published(self):
        return sorted(
            os.path.relpath(os.path.join(d, f), self.root)
            for d, _, files in os.walk(f"{self.root}/gold")
            for f in files
        )

    def read_gold(self, table, exclude=("pipeline_run_at",)):
        uri = f"{self.root}/gold/{table}/date=2026-01-15/hour=12/data.parquet"
        cols = [
            r[0]
            for r in self.con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{uri}', hive_partitioning=false)"
            ).fetchall()
            if r[0] not in exclude
        ]
        sel = ", ".join(f'"{c}"' for c in cols)
        return self.con.execute(
            f"SELECT {sel} FROM read_parquet('{uri}', hive_partitioning=false) "
            "ORDER BY ALL"
        ).fetchall()


class GoldProcessTests(LocalPipelineTestCase):
    def test_all_tables_are_published_once(self):
        self.write_quarantine(["duplicate", "stale_gps_time"])
        result = self.build()
        self.assertEqual(result["status"], "complete")
        self.assertEqual(len(self.published()), len(gold.GOLD_TABLES))
        self.assertEqual(os.listdir(f"{self.root}/staging"), [])

    def test_rerun_is_idempotent_and_creates_no_duplicates(self):
        self.write_quarantine(["duplicate", "stale_gps_time"])
        self.build()
        first = {t: self.read_gold(t) for t in gold.GOLD_TABLES}
        files = self.published()
        later = RUN_AT.replace(minute=45)
        self.build(run_at=later)
        self.assertEqual(self.published(), files)
        for table in gold.GOLD_TABLES:
            self.assertEqual(self.read_gold(table), first[table], table)
            self.assertEqual(len(first[table]) > 0, True)
        self.assertEqual(len(first["hourly_network_metrics"]), 1)
        self.assertEqual(len(first["hourly_data_quality"]), 1)

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
            "SELECT vehicle_number FROM read_parquet('"
            f"{self.root}/gold/vehicle_activity_hourly/date=2026-01-15/hour=12/"
            "data.parquet', hive_partitioning=false) "
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
                r[0]
                for r in self.con.execute(
                    "SELECT data_completeness_status FROM read_parquet('"
                    f"{self.root}/gold/{table}/date=2026-01-15/hour=12/data.parquet',"
                    " hive_partitioning=false)"
                ).fetchall()
            }
            self.assertEqual(statuses, {"incomplete"}, table)
        safe = self.con.execute(
            "SELECT safe_for_dashboard FROM read_parquet('"
            f"{self.root}/gold/hourly_data_quality/date=2026-01-15/hour=12/"
            "data.parquet', hive_partitioning=false)"
        ).fetchone()[0]
        self.assertIs(safe, False)

    def test_quarantined_duplicates_do_not_inflate_silver_metrics(self):
        self.write_quarantine(["duplicate"] * 5)
        self.build()
        totals = self.con.execute(
            "SELECT SUM(active_vehicles), SUM(position_events), SUM(silver_records) "
            "FROM read_parquet('"
            f"{self.root}/gold/hourly_network_metrics/date=2026-01-15/hour=12/"
            "data.parquet', hive_partitioning=false)"
        ).fetchone()
        self.assertEqual(totals, (5, 6, 6))
        quality = self.con.execute(
            "SELECT bronze_records, silver_position_events, duplicate_records "
            "FROM read_parquet('"
            f"{self.root}/gold/hourly_data_quality/date=2026-01-15/hour=12/"
            "data.parquet', hive_partitioning=false)"
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

    def test_copy_failure_during_rerun_removes_commit_marker(self):
        self.build()
        quality_path = (
            f"{self.root}/gold/hourly_data_quality/date=2026-01-15/hour=12/data.parquet"
        )
        self.assertTrue(os.path.exists(quality_path))

        self.silver = make_silver(
            self.tmp.name, SILVER_SQL.replace("('tram', '2000'", "('tram', '2001'")
        )
        self.copy_calls = 0
        self.fail_copy_on = 2
        with self.assertRaisesRegex(OSError, "forced Gold object copy failure"):
            self.build()

        # The data files may now span generations, but consumers can detect that
        # the hour is not committed because its quality marker is absent.
        self.assertFalse(os.path.exists(quality_path))


class GoldSilverContractTests(LocalPipelineTestCase):
    def assert_rejected_without_publication(self, sql):
        self.silver = make_silver(self.tmp.name, sql)
        with self.assertRaises(ValueError):
            self.build()
        self.assertEqual(self.published(), [])

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
                "SELECT line, SUM(position_events) FROM read_parquet('"
                f"{self.root}/gold/hourly_line_metrics/date=2026-01-15/hour=12/"
                "data.parquet', hive_partitioning=false) GROUP BY line"
            ).fetchall()
        )
        self.assertEqual(lines["unknown"], 2)
        self.assertEqual(sum(lines.values()), 6)


if __name__ == "__main__":
    unittest.main()
