import json
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

from check_airflow_health import check_airflow_health, main


class AirflowHealthTests(unittest.TestCase):
    def test_http_success_with_unhealthy_scheduler_is_unhealthy(self):
        payload = {
            "metadatabase": {"status": "healthy"},
            "scheduler": {"status": "unhealthy"},
            "triggerer": {"status": "healthy"},
            "dag_processor": {"status": "healthy"},
        }
        response = Mock()
        response.read.return_value = json.dumps(payload).encode("utf-8")

        with patch("check_airflow_health.urlopen") as open_url:
            open_url.return_value.__enter__.return_value = response

            healthy, message = check_airflow_health()

        self.assertFalse(healthy)
        self.assertIn("scheduler", message.lower())

    def test_all_required_components_healthy(self):
        payload = {
            name: {"status": "healthy"}
            for name in (
                "metadatabase",
                "scheduler",
                "triggerer",
                "dag_processor",
            )
        }
        response = Mock()
        response.read.return_value = json.dumps(payload).encode("utf-8")

        with patch("check_airflow_health.urlopen") as open_url:
            open_url.return_value.__enter__.return_value = response
            healthy, message = check_airflow_health()

        self.assertTrue(healthy)
        self.assertIn("healthy", message.lower())
        open_url.assert_called_once_with(
            "http://localhost:8080/api/v2/monitor/health",
            timeout=5,
        )

    def test_missing_or_invalid_component_is_unhealthy(self):
        names = (
            "metadatabase",
            "scheduler",
            "triggerer",
            "dag_processor",
        )

        for name in names:
            for value in (None, {}, {"status": None}, {"status": "unhealthy"}):
                with self.subTest(component=name, value=value):
                    payload = {component: {"status": "healthy"} for component in names}
                    if value is None:
                        del payload[name]
                    else:
                        payload[name] = value

                    response = Mock()
                    response.read.return_value = json.dumps(payload).encode("utf-8")

                    with patch("check_airflow_health.urlopen") as open_url:
                        open_url.return_value.__enter__.return_value = response
                        healthy, message = check_airflow_health()

                    self.assertFalse(healthy)
                    self.assertIn(name, message)

    def test_invalid_response_is_unhealthy(self):
        for body in (b"not-json", b"[]", b"null"):
            with self.subTest(body=body):
                response = Mock()
                response.read.return_value = body

                with patch("check_airflow_health.urlopen") as open_url:
                    open_url.return_value.__enter__.return_value = response
                    healthy, message = check_airflow_health()

                self.assertFalse(healthy)
                self.assertIn("Airflow", message)

    def test_request_failure_is_unhealthy(self):
        errors = (
            URLError("Connection refused"),
            TimeoutError("Request timed out"),
            HTTPError(
                "http://localhost:8080/api/v2/monitor/health",
                503,
                "Service unavailable",
                None,
                None,
            ),
        )

        for error in errors:
            with self.subTest(error=type(error).__name__):
                with patch(
                    "check_airflow_health.urlopen",
                    side_effect=error,
                ):
                    healthy, message = check_airflow_health()

                self.assertFalse(healthy)
                self.assertIn("request failed", message)

    def test_main_returns_healthcheck_exit_code(self):
        for healthy, expected_code in ((True, 0), (False, 1)):
            with (
                self.subTest(healthy=healthy),
                patch(
                    "check_airflow_health.check_airflow_health",
                    return_value=(healthy, "Test health result"),
                ),
                patch("builtins.print") as print_message,
                self.assertRaises(SystemExit) as raised,
            ):
                main()

            self.assertEqual(raised.exception.code, expected_code)
            prefix = "HEALTHY" if healthy else "UNHEALTHY"
            print_message.assert_called_once_with(f"{prefix}: Test health result")


if __name__ == "__main__":
    unittest.main()
