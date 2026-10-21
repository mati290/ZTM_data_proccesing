import os
import unittest
from unittest.mock import Mock, patch

import duckdb

try:
    from airflow.models import DagBag
except ImportError:  # Airflow is only installed in the container image.
    DagBag = None

DAGS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dags")


@unittest.skipIf(DagBag is None, "apache-airflow is not installed")
class DagDefinitionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag(dag_folder=DAGS_DIR, include_examples=False)

    def test_dags_import_without_errors(self):
        self.assertEqual(self.bag.import_errors, {})

    def test_gold_dag_waits_for_silver_then_builds(self):
        dag = self.bag.get_dag("silver_to_gold_hourly")
        sensor = dag.get_task("wait_for_silver")
        self.assertEqual(sensor.external_dag_id, "bronze_to_silver_hourly")
        self.assertEqual(sensor.external_task_id, "transform_bronze_to_silver")
        self.assertIn("skipped", sensor.skipped_states)
        self.assertIn("failed", sensor.failed_states)
        self.assertEqual(
            dag.get_task("build_gold").upstream_task_ids, {"wait_for_silver"}
        )

    def test_silver_task_reports_close_failure_after_success(self):
        import bronze_to_silver

        dag = self.bag.dags["bronze_to_silver_hourly"]
        task_function = dag.get_task("transform_bronze_to_silver").python_callable

        interval_start = Mock()
        interval_start.in_timezone.return_value = interval_start
        interval_start.strftime.side_effect = ["2026-01-15", "12"]

        connection = Mock()
        close_error = duckdb.Error("Simulated close failure")
        connection.close.side_effect = close_error

        with (
            patch.dict(
                task_function.__globals__,
                {
                    "get_current_context": lambda: {
                        "data_interval_start": interval_start
                    }
                },
            ),
            patch.object(
                bronze_to_silver,
                "get_connection",
                return_value=connection,
            ),
            patch.object(bronze_to_silver, "transform_hour") as transform,
            self.assertRaises(duckdb.Error) as raised,
        ):
            task_function()

        self.assertIs(raised.exception, close_error)
        transform.assert_called_once_with(connection, "2026-01-15", "12")
        connection.close.assert_called_once_with()

    def test_silver_task_close_failure_preserves_transform_failure(self):
        import bronze_to_silver

        dag = self.bag.dags["bronze_to_silver_hourly"]
        task_function = dag.get_task("transform_bronze_to_silver").python_callable

        interval_start = Mock()
        interval_start.in_timezone.return_value = interval_start
        interval_start.strftime.side_effect = ["2026-01-15", "12"]

        connection = Mock()
        transform_error = ValueError("Simulated transform failure")
        connection.close.side_effect = duckdb.Error("Simulated close failure")

        with (
            patch.dict(
                task_function.__globals__,
                {
                    "get_current_context": lambda: {
                        "data_interval_start": interval_start
                    }
                },
            ),
            patch.object(
                bronze_to_silver,
                "get_connection",
                return_value=connection,
            ),
            patch.object(
                bronze_to_silver,
                "transform_hour",
                side_effect=transform_error,
            ),
            self.assertRaises(ValueError) as raised,
        ):
            task_function()

        self.assertIs(raised.exception, transform_error)
        connection.close.assert_called_once_with()

    def test_gold_dag_has_retries_and_shares_silver_schedule(self):
        gold = self.bag.get_dag("silver_to_gold_hourly")
        silver = self.bag.get_dag("bronze_to_silver_hourly")
        self.assertGreaterEqual(gold.default_args["retries"], 1)
        self.assertEqual(str(gold.timetable.summary), str(silver.timetable.summary))
        self.assertEqual(gold.max_active_runs, 1)

    def test_hourly_dags_do_not_catch_up(self):
        for dag_id in ("bronze_to_silver_hourly", "silver_to_gold_hourly"):
            with self.subTest(dag_id=dag_id):
                self.assertFalse(self.bag.get_dag(dag_id).catchup)

    def test_late_data_dag_is_single_run_with_retries(self):
        dag = self.bag.get_dag("reprocess_late_data_hourly")
        self.assertIsNotNone(dag)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertFalse(dag.catchup)
        self.assertGreaterEqual(dag.default_args["retries"], 1)
        self.assertIn("reprocess", dag.task_ids)

    def test_all_partition_writers_share_one_pool(self):
        expected_pool = "ztm_silver_gold_writer"
        writers = (
            ("bronze_to_silver_hourly", "transform_bronze_to_silver"),
            ("silver_to_gold_hourly", "build_gold"),
            ("reprocess_late_data_hourly", "reprocess"),
        )

        for dag_id, task_id in writers:
            task = self.bag.get_dag(dag_id).get_task(task_id)
            self.assertEqual(task.pool, expected_pool)


if __name__ == "__main__":
    unittest.main()
