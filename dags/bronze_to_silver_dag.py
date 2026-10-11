from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.timetables.interval import CronDataIntervalTimetable


@dag(
    dag_id="bronze_to_silver_hourly",
    description="Transforms one hour of ZTM bronze data into silver Parquet and builds the gold layer",
    schedule=CronDataIntervalTimetable("0 * * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 10, 10, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["ztm", "bronze-to-silver"],
)
def bronze_to_silver_hourly():
    @task
    def transform_bronze_to_silver():
        import bronze_to_silver

        context = get_current_context()
        interval_start = context["data_interval_start"].in_timezone("UTC")

        connection = bronze_to_silver.get_connection()
        try:
            bronze_to_silver.transform_hour(
                connection,
                interval_start.strftime("%Y-%m-%d"),
                interval_start.strftime("%H"),
            )
        except bronze_to_silver.BronzeDataMissingError as exc:
            # Hours before the first ever Bronze object are expected (backfill
            # before ingestion started); any later gap is an outage and must fail.
            if bronze_to_silver.is_before_first_bronze_hour(
                interval_start.strftime("%Y-%m-%d"), interval_start.strftime("%H")
            ):
                raise AirflowSkipException(str(exc)) from exc
            raise
        finally:
            connection.close()

    transform_bronze_to_silver()


bronze_to_silver_hourly()
