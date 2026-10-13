from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="reprocess_late_data_hourly",
    description="Recomputes Silver and Gold for closed hours of the last 24h that received late data",
    schedule="30 * * * *",
    start_date=pendulum.datetime(2026, 10, 10, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ztm", "late-data"],
)
def reprocess_late_data_hourly():
    @task(pool="ztm_silver_gold_writer")
    def reprocess():
        import late_data

        result = late_data.reprocess_late_data()
        print(f"Recomputed hours: {result['recomputed']}")
        return result

    reprocess()


reprocess_late_data_hourly()
