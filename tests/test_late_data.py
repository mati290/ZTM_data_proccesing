import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import late_data

NOW = datetime(2026, 1, 15, 13, 40, tzinfo=timezone.utc)
T0 = datetime(2026, 1, 15, 12, 30, tzinfo=timezone.utc)


class FakeS3:
    """Maps (bucket, prefix) -> list of LastModified for objects under that prefix."""

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


def manifest(hour, when):
    return (
        ("bronze", f"_manifests/date=2026-01-15/hour={hour}/cycle.json"),
        when,
    )


def gold_commit(hour, when):
    return (
        ("gold", f"hourly_data_quality/date=2026-01-15/hour={hour}/data.parquet"),
        when,
    )


class StaleHourTests(unittest.TestCase):
    def stale(self, *items, window=3):
        return late_data.find_stale_hours(NOW, window, FakeS3(dict(items)))

    def test_late_bronze_after_silver_is_stale(self):
        result = self.stale(
            bronze("12", T0 + timedelta(minutes=40)),
            silver("12", T0 + timedelta(minutes=35)),
            gold_commit("12", T0 + timedelta(minutes=36)),
        )
        self.assertEqual(result, [("2026-01-15", "12")])

    def test_silver_newer_than_bronze_and_gold_commit_is_not_stale(self):
        result = self.stale(
            bronze("12", T0),
            silver("12", T0 + timedelta(minutes=35)),
            gold_commit("12", T0 + timedelta(minutes=36)),
        )
        self.assertEqual(result, [])

    def test_missing_gold_commit_triggers_reprocessing_even_when_silver_is_current(
        self,
    ):
        result = self.stale(
            bronze("12", T0),
            silver("12", T0 + timedelta(minutes=35)),
        )
        self.assertEqual(result, [("2026-01-15", "12")])

    def test_gold_commit_older_than_silver_triggers_reprocessing(self):
        result = self.stale(
            bronze("12", T0),
            silver("12", T0 + timedelta(minutes=40)),
            gold_commit("12", T0 + timedelta(minutes=35)),
        )
        self.assertEqual(result, [("2026-01-15", "12")])

    def test_manifest_written_after_silver_triggers_reprocessing(self):
        result = self.stale(
            bronze("12", T0),
            silver("12", T0 + timedelta(minutes=35)),
            manifest("12", T0 + timedelta(minutes=40)),
            gold_commit("12", T0 + timedelta(minutes=36)),
        )
        self.assertEqual(result, [("2026-01-15", "12")])

    def test_manifest_older_than_silver_does_not_trigger_reprocessing(self):
        result = self.stale(
            bronze("12", T0),
            silver("12", T0 + timedelta(minutes=40)),
            manifest("12", T0 + timedelta(minutes=35)),
            gold_commit("12", T0 + timedelta(minutes=41)),
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

    def test_hours_outside_window_are_frozen(self):
        self.assertEqual(self.stale(bronze("08", T0), window=3), [])


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
