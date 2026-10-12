from datetime import timedelta

import pendulum
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.timetables.interval import CronDataIntervalTimetable


@dag(
    dag_id="silver_to_gold_hourly",
    description="Builds the hourly Gold tables from the Silver partition of the same hour",
    # Same timetable as bronze_to_silver_hourly, so both DAGs share logical dates.
    schedule=CronDataIntervalTimetable("0 * * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 10, 12, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["ztm", "silver-to-gold"],
)
def silver_to_gold_hourly():
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="bronze_to_silver_hourly",
        external_task_id="transform_bronze_to_silver",
        allowed_states=["success"],
        # Silver skips only hours before ingestion started; Gold skips them too.
        skipped_states=["skipped"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=int(timedelta(hours=6).total_seconds()),
        retries=0,
    )

    @task(pool="ztm_silver_gold_writer")
    def build_gold():
        import bronze_to_silver
        import silver_to_gold

        context = get_current_context()
        interval_start = context["data_interval_start"].in_timezone("UTC")
        date_str = interval_start.strftime("%Y-%m-%d")
        hour_str = interval_start.strftime("%H")

        connection = silver_to_gold.get_connection()
        try:
            result = silver_to_gold.build_gold_hour(connection, date_str, hour_str)
        except silver_to_gold.SilverDataMissingError as exc:
            if bronze_to_silver.is_before_first_bronze_hour(date_str, hour_str):
                raise AirflowSkipException(str(exc)) from exc
            raise
        finally:
            connection.close()

        print(
            f"Gold {date_str} hour {hour_str}: completeness={result['status']}, "
            f"rows={result['rows']}"
        )
        return result

    wait_for_silver >> build_gold()


silver_to_gold_hourly()
