import unittest
from unittest.mock import Mock, patch

import duckdb

from analysis import explore


class ExplorationTests(unittest.TestCase):
    def test_delay_buckets_separate_invalid_future_and_day_boundary(self):
        actual = duckdb.connect()
        actual.execute("SET TimeZone='UTC'")
        self.addCleanup(actual.close)
        connection = Mock()
        # Exercise every real analysis query against a local partition fixture.
        connection.execute.side_effect = lambda *args: actual.execute("""
            CREATE TEMP TABLE bronze AS
            SELECT 'bus' AS vehicle_type, '100' AS VehicleNumber, '1' AS Lines,
                52.2e0 AS Lat, 21.0e0 AS Lon,
                '2026-01-15T12:00:00+00:00' AS _ingested_at, gps AS "Time"
            FROM (VALUES ('invalid'), ('2026-01-15 13:02:00'),
                         ('2026-01-14 13:00:00')) t(gps)
        """)
        connection.sql.side_effect = actual.sql
        connection.close.side_effect = actual.close
        buckets = {}

        def capture(relation):
            if "bucket" in relation.columns:
                buckets.update(relation.fetchall())

        with (
            patch.object(explore, "get_connection", return_value=connection),
            patch("builtins.print", side_effect=capture),
        ):
            explore.explore("2026-01-15", "12")

        self.assertEqual(
            buckets,
            {
                "0: invalid/missing time": 1,
                "0: future GPS time": 1,
                "5: >= 1 day": 1,
            },
        )
        connection.close.assert_called_once_with()
