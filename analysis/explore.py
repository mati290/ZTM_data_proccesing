"""Explore one explicit Bronze partition without loading the entire history."""

import argparse
import logging

import duckdb

from bronze_to_silver import BRONZE_JSON_COLUMNS, get_connection, validate_partition

logger = logging.getLogger(__name__)


def explore(date_str: str, hour_str: str) -> None:
    date_str, hour_str = validate_partition(date_str, hour_str)
    con = get_connection()
    close_error = None
    try:
        con.execute(
            "CREATE OR REPLACE TEMP TABLE bronze AS SELECT * REPLACE "
            "(TRY_CAST(Lat AS DOUBLE) AS Lat, TRY_CAST(Lon AS DOUBLE) AS Lon) "
            "FROM read_json_auto(?, columns=?, format='newline_delimited', hive_partitioning=true)",
            [
                f"s3://bronze/vehicle_type=*/date={date_str}/hour={hour_str}/*.jsonl",
                BRONZE_JSON_COLUMNS,
            ],
        )

        print(con.sql("DESCRIBE bronze"))

        print(con.sql("SELECT COUNT(*) AS all_records FROM bronze"))

        print(
            con.sql("""
            SELECT
                COUNT(*) AS all_records,
                COUNT(DISTINCT (vehicle_type, VehicleNumber, "Time")) AS unique_records,
                COUNT(*) - COUNT(DISTINCT (vehicle_type, VehicleNumber, "Time")) AS duplicates
            FROM bronze
        """)
        )

        print(
            con.sql("""
            SELECT
                MIN(delay) AS min_delay,
                MEDIAN(delay) AS median_delay,
                MAX(delay) AS max_delay
            FROM (
                SELECT
                    TRY_CAST(_ingested_at AS TIMESTAMPTZ)
                      - (TRY_CAST("Time" AS TIMESTAMP) AT TIME ZONE 'Europe/Warsaw') AS delay
                FROM bronze
            )
        """)
        )

        print(
            con.sql("""
            SELECT COUNT(*) AS outside_warsaw
            FROM bronze
            WHERE NOT (Lat BETWEEN 51.9 AND 52.6 AND Lon BETWEEN 20.5 AND 21.6)
        """)
        )

        print(
            con.sql("""
            WITH d AS (
                SELECT TRY_CAST(_ingested_at AS TIMESTAMPTZ)
                     - (TRY_CAST("Time" AS TIMESTAMP) AT TIME ZONE 'Europe/Warsaw') AS delay
                FROM bronze
            )
            SELECT
                CASE
                    WHEN delay < INTERVAL 1 MINUTE  THEN '1: < 1 min'
                    WHEN delay < INTERVAL 5 MINUTE  THEN '2: 1-5 min'
                    WHEN delay < INTERVAL 1 HOUR    THEN '3: 5-60 min'
                    WHEN delay < INTERVAL 1 DAY     THEN '4: 1-24 h'
                    ELSE                                 '5: > 1 day'
                END AS bucket,
                COUNT(*) AS records
            FROM d
            GROUP BY bucket
            ORDER BY bucket
        """)
        )

        print(
            con.sql("""
            SELECT
                vehicle_type,
                _ingested_at,
                COUNT(*) AS records,
                MAX("Time") AS newest_gps_signal
            FROM bronze
            GROUP BY vehicle_type, _ingested_at
            ORDER BY _ingested_at DESC
            LIMIT 20
        """)
        )

        print(
            con.sql("""
            SELECT
                (Lat = 0 OR Lon = 0) AS zero_coords,
                COUNT(*) AS records,
                COUNT(DISTINCT Lines) AS lines,
                MIN(Lat), MAX(Lat), MIN(Lon), MAX(Lon)
            FROM bronze
            WHERE NOT (Lat BETWEEN 51.9 AND 52.6 AND Lon BETWEEN 20.5 AND 21.6)
            GROUP BY zero_coords
        """)
        )
    finally:
        try:
            con.close()
        except duckdb.Error as exc:
            close_error = exc
            logger.error("Could not close analysis connection")
    if close_error is not None:
        raise close_error


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)
    args = parser.parse_args()
    explore(args.date, args.hour)


if __name__ == "__main__":
    main()
