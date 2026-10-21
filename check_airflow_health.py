"""Check component health for the project's Airflow standalone deployment."""

import json
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

HEALTH_URL = "http://localhost:8080/api/v2/monitor/health"
REQUIRED_COMPONENTS = (
    "metadatabase",
    "scheduler",
    "triggerer",
    "dag_processor",
)


def check_airflow_health() -> tuple[bool, str]:
    try:
        with urlopen(HEALTH_URL, timeout=5) as response:
            payload = json.loads(response.read())
    except (URLError, OSError, ValueError) as exc:
        if isinstance(exc, HTTPError):
            exc.close()
        return False, f"Airflow health request failed: {type(exc).__name__}"

    if not isinstance(payload, dict):
        return False, "Airflow health response must be a JSON object"

    unhealthy = []
    for name in REQUIRED_COMPONENTS:
        component = payload.get(name)
        if not isinstance(component, dict) or component.get("status") != "healthy":
            unhealthy.append(name)

    if unhealthy:
        return False, f"Airflow components unhealthy or missing: {', '.join(unhealthy)}"

    return True, "All required Airflow components are healthy"


def main() -> None:
    healthy, message = check_airflow_health()
    print(f"{'HEALTHY' if healthy else 'UNHEALTHY'}: {message}")
    raise SystemExit(0 if healthy else 1)


if __name__ == "__main__":
    main()
