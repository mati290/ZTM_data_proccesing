import argparse
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone

import duckdb
from dotenv import load_dotenv

logging.Formatter.converter = time.gmtime
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
load_dotenv()

BASE_CTES = """
WITH source AS (
    SELECT *
    FROM read_json_auto('{bronze_path}', hive_partitioning = true)
),
typed AS (
    SELECT
        vehicle_type,
        VehicleNumber AS vehicle_number,
        Lines         AS line,
        Brigade       AS brigade,
        Lat           AS lat,
        Lon           AS lon,
        CAST("Time" AS TIMESTAMP) AT TIME ZONE 'Europe/Warsaw' AS gps_time_utc,
        CAST(_ingested_at AS TIMESTAMPTZ)                     AS ingested_at
    FROM source
),
valid_coordinates AS (
    SELECT *
    FROM typed
    WHERE lat BETWEEN 51.9 AND 52.6
    AND lon BETWEEN 20.5 AND 21.6
),
valid_gps_time AS (
    SELECT *
    FROM valid_coordinates
    WHERE ingested_at - gps_time_utc >= INTERVAL 0 SECOND
    AND ingested_at - gps_time_utc <= INTERVAL 5 MINUTE
),
deduplicated AS (
    SELECT *
    FROM valid_gps_time
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY vehicle_type, vehicle_number, gps_time_utc
        ORDER BY ingested_at
    ) = 1
)
"""

TRANSFORM_SQL = BASE_CTES + "SELECT * FROM deduplicated"

QUALITY_SQL = (
    BASE_CTES
    + """
SELECT
    (SELECT COUNT(*) FROM source) AS bronze_count,
    (SELECT COUNT(*) FROM valid_coordinates) AS after_coordinates_count,
    (SELECT COUNT(*) FROM valid_gps_time) AS after_time_count,
    (
        SELECT COUNT(*)
        FROM valid_coordinates
        WHERE gps_time_utc > ingested_at
    ) AS future_gps_time_count,
    (
        SELECT COUNT(*)
        FROM valid_coordinates
        WHERE ingested_at - gps_time_utc > INTERVAL 5 MINUTE
    ) AS stale_gps_time_count,
    (
        SELECT COUNT(*)
        FROM valid_coordinates
        WHERE ingested_at - gps_time_utc > INTERVAL 5 MINUTE
        AND ingested_at - gps_time_utc <= INTERVAL 10 MINUTE
    ) AS stale_5_to_10_minutes_count,
    (
        SELECT COUNT(*)
        FROM valid_coordinates
        WHERE ingested_at - gps_time_utc > INTERVAL 10 MINUTE
        AND ingested_at - gps_time_utc <= INTERVAL 30 MINUTE
    ) AS stale_10_to_30_minutes_count,
    (
        SELECT COUNT(*)
        FROM valid_coordinates
        WHERE ingested_at - gps_time_utc > INTERVAL 30 MINUTE
    ) AS stale_over_30_minutes_count,
    (
        SELECT COUNT(*)
        FROM valid_coordinates
        WHERE gps_time_utc IS NULL OR ingested_at IS NULL
    ) AS missing_gps_time_count,
    (SELECT COUNT(*) FROM deduplicated) AS silver_count
"""
)


def get_connection():
    endpoint = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000").replace(
        "http://", ""
    )

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET TimeZone = 'UTC';")
    con.execute(f"""
        CREATE SECRET minio (
            TYPE s3,
            KEY_ID '{os.getenv("MINIO_USER")}',
            SECRET '{os.getenv("MINIO_PASSWORD")}',
            ENDPOINT '{endpoint}',
            URL_STYLE 'path',
            USE_SSL false
        );
    """)
    return con


def validate_partition(date_str: str, hour_str: str) -> tuple[str, str]:
    try:
        parsed_date = date.fromisoformat(date_str)
    except ValueError as exc:
        raise ValueError("date must use YYYY-MM-DD format") from exc

    if parsed_date.strftime("%Y-%m-%d") != date_str:
        raise ValueError("date must use YYYY-MM-DD format")

    if not hour_str.isascii() or not hour_str.isdigit() or len(hour_str) not in (1, 2):
        raise ValueError("hour must be between 0 and 23")

    hour = int(hour_str)
    if not 0 <= hour <= 23:
        raise ValueError("hour must be between 0 and 23")

    return date_str, f"{hour:02d}"


def transform_hour(con, date_str, hour_str):
    date_str, hour_str = validate_partition(date_str, hour_str)
    bronze_path = f"s3://bronze/vehicle_type=*/date={date_str}/hour={hour_str}/*.jsonl"
    silver_path = (
        f"s3://silver/vehicle_positions/date={date_str}/hour={hour_str}/data.parquet"
    )
    bronze_file = con.execute(
        "SELECT file FROM glob(?) LIMIT 1",
        [bronze_path],
    ).fetchone()
    if bronze_file is None:
        logger.warning(f"No bronze data for {date_str} hour {hour_str}, skipping")
        return
    quality_sql = QUALITY_SQL.format(bronze_path=bronze_path)
    (
        before,
        after_coordinates,
        after_time,
        future_gps_time,
        stale_gps_time,
        stale_5_to_10,
        stale_10_to_30,
        stale_over_30,
        missing_gps_time,
        after,
    ) = con.execute(quality_sql).fetchone()
    sql = TRANSFORM_SQL.format(bronze_path=bronze_path)
    con.execute(f"COPY ({sql}) TO '{silver_path}' (FORMAT parquet)")

    rejected_coordinates = before - after_coordinates
    rejected_gps_time = after_coordinates - after_time
    duplicates = after_time - after
    dropped_pct = (before - after) / before if before else 0

    logger.info(
        f"{date_str} hour {hour_str}: "
        f"bronze={before}, "
        f"rejected_coordinates={rejected_coordinates}, "
        f"rejected_gps_time={rejected_gps_time} "
        f"(future={future_gps_time}, stale_total={stale_gps_time}, "
        f"stale_5_to_10={stale_5_to_10}, "
        f"stale_10_to_30={stale_10_to_30}, "
        f"stale_over_30={stale_over_30}, "
        f"missing={missing_gps_time}), "
        f"duplicates={duplicates}, "
        f"silver={after}, "
        f"dropped={dropped_pct:.1%} → {silver_path}"
    )


def main():
    # domyślnie: poprzednia pełna godzina w UTC
    previous_hour = datetime.now(timezone.utc) - timedelta(hours=1)

    parser = argparse.ArgumentParser(description="Bronze → silver for one hour")
    parser.add_argument("--date", default=previous_hour.strftime("%Y-%m-%d"))
    parser.add_argument("--hour", default=previous_hour.strftime("%H"))
    args = parser.parse_args()

    con = get_connection()
    transform_hour(con, args.date, args.hour)


if __name__ == "__main__":
    main()
