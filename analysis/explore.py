import os

import duckdb
from dotenv import load_dotenv

load_dotenv()

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET TimeZone = 'UTC';")  # wszystkie obliczenia czasu w UTC

con.execute(f"""
    CREATE SECRET minio (
        TYPE s3,
        KEY_ID '{os.getenv("MINIO_USER")}',
        SECRET '{os.getenv("MINIO_PASSWORD")}',
        ENDPOINT 'localhost:9000',
        URL_STYLE 'path',
        USE_SSL false
    );
""")

# Wczytujemy bronze RAZ do tabeli w pamięci
con.execute("""
    CREATE TABLE bronze AS
    SELECT *
    FROM read_json_auto('s3://bronze/**/*.jsonl', hive_partitioning = true)
""")

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
            CAST(_ingested_at AS TIMESTAMPTZ)
              - (CAST("Time" AS TIMESTAMP) AT TIME ZONE 'Europe/Warsaw') AS delay
        FROM bronze
    )
""")
)

print(
    con.sql("""
    SELECT COUNT(*) AS outside_warsaw
    FROM bronze
    WHERE NOT (Lat BETWEEN 52.0 AND 52.4 AND Lon BETWEEN 20.8 AND 21.3)
""")
)

print(
    con.sql("""
    WITH d AS (
        SELECT CAST(_ingested_at AS TIMESTAMPTZ)
             - (CAST("Time" AS TIMESTAMP) AT TIME ZONE 'Europe/Warsaw') AS delay
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
    WHERE NOT (Lat BETWEEN 52.0 AND 52.4 AND Lon BETWEEN 20.8 AND 21.3)
    GROUP BY zero_coords
""")
)
