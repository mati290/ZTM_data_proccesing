import logging
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.timetables.interval import CronDataIntervalTimetable

logger = logging.getLogger(__name__)


@dag(
    dag_id="bronze_to_silver_hourly",
    description=(
        "Transforms one hour of ZTM bronze data into silver Parquet "
        "(Gold is built by silver_to_gold_hourly)"
    ),
    schedule=CronDataIntervalTimetable("0 * * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 10, 12, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["ztm", "bronze-to-silver"],
)
def bronze_to_silver_hourly():
    @task(pool="ztm_silver_gold_writer")
    def transform_bronze_to_silver():
        import bronze_to_silver

        context = get_current_context()
        interval_start = context["data_interval_start"].in_timezone("UTC")
        date_str = interval_start.strftime("%Y-%m-%d")
        hour_str = interval_start.strftime("%H")

        connection = bronze_to_silver.get_connection()
        try:
            bronze_to_silver.transform_hour(connection, date_str, hour_str)
        except bronze_to_silver.BronzeDataMissingError as exc:
            if bronze_to_silver.is_before_first_bronze_hour(date_str, hour_str):
                raise AirflowSkipException(str(exc)) from exc

            try:
                import silver_to_gold

                try:
                    silver_to_gold.build_gold_hour(connection, date_str, hour_str)
                except silver_to_gold.SilverDataMissingError:
                    pass
            except Exception:
                logger.exception(
                    "Could not publish Gold data-quality record for missing Bronze "
                    "partition %s hour %s",
                    date_str,
                    hour_str,
                )

            raise
        finally:
            connection.close()

    transform_bronze_to_silver()


bronze_to_silver_hourly()
