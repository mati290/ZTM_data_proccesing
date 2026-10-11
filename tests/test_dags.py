import os
import unittest

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

    def test_gold_dag_has_retries_and_shares_silver_schedule(self):
        gold = self.bag.get_dag("silver_to_gold_hourly")
        silver = self.bag.get_dag("bronze_to_silver_hourly")
        self.assertGreaterEqual(gold.default_args["retries"], 1)
        self.assertEqual(str(gold.timetable.summary), str(silver.timetable.summary))
        self.assertEqual(gold.max_active_runs, 1)


if __name__ == "__main__":
    unittest.main()
