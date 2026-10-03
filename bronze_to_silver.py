import argparse
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import duckdb
from dotenv import load_dotenv

logging.Formatter.converter = time.gmtime
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
load_dotenv()

TRANSFORM_SQL = """
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
filtered AS (
    SELECT *
    FROM typed
    WHERE ingested_at - gps_time_utc <= INTERVAL 5 MINUTE
      AND lat BETWEEN 51.9 AND 52.6
      AND lon BETWEEN 20.5 AND 21.6
),
deduplicated AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY vehicle_type, vehicle_number, gps_time_utc
        ORDER BY ingested_at
    ) = 1
)
SELECT * FROM deduplicated
"""


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


def transform_hour(con, date_str, hour_str):
    hour_str = hour_str.zfill(2)
    bronze_path = f"s3://bronze/vehicle_type=*/date={date_str}/hour={hour_str}/*.jsonl"
    silver_path = (
        f"s3://silver/vehicle_positions/date={date_str}/hour={hour_str}/data.parquet"
    )

    # liczba rekordów w bronze; jeśli nie ma plików, DuckDB rzuca IOException
    try:
        before = con.sql(
            f"SELECT COUNT(*) FROM read_json_auto('{bronze_path}')"
        ).fetchone()[0]
    except duckdb.IOException:
        logger.warning(f"No bronze data for {date_str} hour {hour_str}, skipping")
        return

    # wypełniamy szablon prawdziwą ścieżką
    sql = TRANSFORM_SQL.format(bronze_path=bronze_path)

    after = con.sql(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]

    con.execute(f"COPY ({sql}) TO '{silver_path}' (FORMAT parquet)")

    dropped_pct = (before - after) / before if before else 0
    logger.info(
        f"{date_str} hour {hour_str}: bronze={before}, silver={after}, "
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
