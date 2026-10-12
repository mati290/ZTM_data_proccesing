import json
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, call, patch

from botocore.exceptions import BotoCoreError, ClientError
from requests import HTTPError, Timeout

from ingest import (
    ZTMApiError,
    ensure_bucket,
    ensure_bucket_with_retry,
    fetch_positions,
    main,
    run_once,
    save_to_bronze,
)


class SaveToBronzeTests(unittest.TestCase):
    def test_saves_unique_files_without_mutating_source_records(self):
        record = {"VehicleNumber": "123"}
        original_record = record.copy()
        s3 = Mock()
        fixed_time = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)

        with patch("ingest.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fixed_time
            save_to_bronze(s3, [record], "bus")
            save_to_bronze(s3, [record], "bus")

        calls = s3.put_object.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertNotEqual(calls[0].kwargs["Key"], calls[1].kwargs["Key"])
        self.assertEqual(record, original_record)

        saved_record = json.loads(calls[0].kwargs["Body"].decode("utf-8"))
        self.assertEqual(saved_record["VehicleNumber"], "123")
        self.assertEqual(saved_record["_ingested_at"], fixed_time.isoformat())

    def test_fetch_positions_returns_api_records(self):
        response = Mock()
        response.json.return_value = {"result": [{"VehicleNumber": "123"}]}

        with patch("ingest.requests.get", return_value=response) as request:
            records = fetch_positions(1, "test-key", max_attempts=1)

        self.assertEqual(records, [{"VehicleNumber": "123"}])
        self.assertEqual(request.call_args.kwargs["params"]["type"], 1)

    def test_fetch_positions_rejects_api_error_result(self):
        response = Mock()
        response.json.return_value = {"result": "Invalid API key"}

        with (
            patch("ingest.requests.get", return_value=response) as request,
            self.assertRaises(ZTMApiError),
        ):
            fetch_positions(1, "test-key", max_attempts=1)
        request.assert_called_once()

    def test_fetch_positions_rejects_malformed_api_payload_without_retry(self):
        malformed_payloads = ({}, {"result": {"VehicleNumber": "123"}})
        for payload in malformed_payloads:
            with self.subTest(payload=payload):
                response = Mock()
                response.json.return_value = payload
                with (
                    patch("ingest.requests.get", return_value=response) as request,
                    patch("ingest.time.sleep") as sleep,
                    self.assertRaises(ZTMApiError),
                ):
                    fetch_positions(1, "test-key", max_attempts=3)
                request.assert_called_once()
                sleep.assert_not_called()

    def test_fetch_positions_does_not_log_api_key_on_http_error(self):
        api_key = "test-secret-key"
        response = Mock()
        response.raise_for_status.side_effect = HTTPError(
            f"401 Client Error for url: https://example.test/?apikey={api_key}",
            response=Mock(status_code=401),
        )

        with (
            patch("ingest.requests.get", return_value=response),
            patch("ingest.time.sleep"),
            self.assertLogs("ingest", level="WARNING") as captured,
            self.assertRaises(HTTPError),
        ):
            fetch_positions(1, api_key, max_attempts=2)

        output = "\n".join(captured.output)
        self.assertNotIn(api_key, output)
        self.assertEqual(output.count("HTTPError (status=401)"), 2)

    def test_fetch_positions_retries_after_timeout(self):
        response = Mock()
        response.json.return_value = {"result": []}

        with (
            patch("ingest.requests.get", side_effect=[Timeout(), response]) as request,
            patch("ingest.time.sleep") as sleep,
        ):
            records = fetch_positions(2, "test-key", max_attempts=2)

        self.assertEqual(records, [])
        self.assertEqual(request.call_count, 2)
        sleep.assert_called_once_with(2)

    def test_run_once_reports_empty_successful_api_responses(self):
        with (
            patch("ingest.fetch_positions", side_effect=[[], []]),
            patch("ingest.save_to_bronze") as save_mock,
        ):
            result = run_once("test-key", Mock())

        self.assertEqual(
            result,
            {
                "buses": 0,
                "trams": 0,
                "total": 0,
                "failed_sources": [],
                "empty_sources": ["bus", "tram"],
            },
        )
        save_mock.assert_not_called()

    def test_run_once_returns_counts_for_buses_and_trams(self):
        with (
            patch(
                "ingest.fetch_positions",
                side_effect=[[{"VehicleNumber": "1"}], [{"VehicleNumber": "2"}]],
            ),
            patch("ingest.save_to_bronze") as save_mock,
        ):
            result = run_once("test-key", Mock())

        self.assertEqual(
            result,
            {
                "buses": 1,
                "trams": 1,
                "total": 2,
                "failed_sources": [],
                "empty_sources": [],
            },
        )
        self.assertEqual(save_mock.call_count, 2)

    def test_run_once_saves_successful_vehicle_type_when_other_source_fails(self):
        bus_records = [{"VehicleNumber": "1"}]
        s3 = Mock()
        with (
            patch("ingest.fetch_positions", side_effect=[bus_records, Timeout()]),
            patch("ingest.save_to_bronze") as save_mock,
        ):
            result = run_once("test-key", s3)

        self.assertEqual(
            result,
            {
                "buses": 1,
                "trams": 0,
                "total": 1,
                "failed_sources": ["tram"],
                "empty_sources": [],
            },
        )
        save_mock.assert_called_once_with(s3, bus_records, "bus")
        manifest_call = s3.put_object.call_args
        self.assertTrue(manifest_call.kwargs["Key"].startswith("_manifests/"))
        manifest = json.loads(manifest_call.kwargs["Body"])
        self.assertEqual(manifest["status"], "partial")
        self.assertEqual(manifest["failed_sources"], ["tram"])


    def test_run_once_raises_when_cycle_manifest_write_fails(self):
        s3 = Mock()
        s3.put_object.side_effect = ClientError(
            {
                "Error": {
                    "Code": "InternalError",
                    "Message": "manifest write failed",
                }
            },
            "PutObject",
        )

        with (
            patch(
                "ingest.fetch_positions",
                side_effect=[
                    [{"VehicleNumber": "1"}],
                    [{"VehicleNumber": "2"}],
                ],
            ),
            patch("ingest.save_to_bronze"),
            self.assertRaises(ClientError),
        ):
            run_once("test-key", s3)


    def test_run_once_manifest_marks_complete_cycle(self):
        s3 = Mock()
        with (
            patch(
                "ingest.fetch_positions",
                side_effect=[[{"VehicleNumber": "1"}], [{"VehicleNumber": "2"}]],
            ),
            patch("ingest.save_to_bronze"),
        ):
            run_once("test-key", s3)

        manifest = json.loads(s3.put_object.call_args.kwargs["Body"])
        self.assertEqual(manifest["status"], "complete")

    def test_main_alerts_after_consecutive_degraded_cycles(self):
        degraded = {
            "buses": 1,
            "trams": 0,
            "total": 1,
            "failed_sources": ["tram"],
            "empty_sources": [],
        }
        with (
            patch.dict(
                "os.environ",
                {
                    "ZTM_API_KEY": "test-key",
                    "POLL_INTERVAL_SECONDS": "1",
                    "DEGRADED_CYCLES_ALERT_THRESHOLD": "2",
                },
            ),
            patch("ingest.load_dotenv"),
            patch("ingest.boto3.client", return_value=Mock()),
            patch("ingest.ensure_bucket_with_retry"),
            patch("ingest.run_once", return_value=degraded),
            patch("ingest.time.sleep", side_effect=[None, KeyboardInterrupt]),
            self.assertLogs("ingest", level="CRITICAL") as captured,
            self.assertRaises(KeyboardInterrupt),
        ):
            main()

        self.assertIn("ALERT", "\n".join(captured.output))

    def test_main_alerts_when_api_successfully_returns_empty_windows(self):
        empty = {
            "buses": 0,
            "trams": 0,
            "total": 0,
            "failed_sources": [],
            "empty_sources": ["bus", "tram"],
        }
        with (
            patch.dict(
                "os.environ",
                {
                    "ZTM_API_KEY": "test-key",
                    "POLL_INTERVAL_SECONDS": "1",
                    "DEGRADED_CYCLES_ALERT_THRESHOLD": "1",
                },
            ),
            patch("ingest.load_dotenv"),
            patch("ingest.boto3.client", return_value=Mock()),
            patch("ingest.ensure_bucket_with_retry"),
            patch("ingest.run_once", return_value=empty),
            patch("ingest.time.sleep", side_effect=KeyboardInterrupt),
            self.assertLogs("ingest", level="CRITICAL") as captured,
            self.assertRaises(KeyboardInterrupt),
        ):
            main()

        self.assertIn("ALERT", "\n".join(captured.output))

    def test_main_retries_next_cycle_after_empty_source_window(self):
        with (
            patch.dict(
                "os.environ",
                {"ZTM_API_KEY": "test-key", "POLL_INTERVAL_SECONDS": "1"},
            ),
            patch("ingest.load_dotenv"),
            patch("ingest.boto3.client", return_value=Mock()),
            patch("ingest.ensure_bucket_with_retry"),
            patch("ingest.run_once", side_effect=RuntimeError("empty source window")),
            patch("ingest.time.sleep", side_effect=KeyboardInterrupt),
            self.assertLogs("ingest", level="ERROR") as captured,
            self.assertRaises(KeyboardInterrupt),
        ):
            main()

        self.assertIn("retrying in next cycle", "\n".join(captured.output))

    def test_ensure_bucket_creates_missing_bucket(self):
        s3 = Mock()
        s3.head_bucket.side_effect = ClientError(
            {
                "Error": {"Code": "404"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            },
            "HeadBucket",
        )

        ensure_bucket(s3)

        s3.create_bucket.assert_has_calls(
            [call(Bucket="bronze"), call(Bucket="silver"), call(Bucket="gold")]
        )
        self.assertEqual(s3.create_bucket.call_count, 3)

    def test_ensure_bucket_does_not_hide_permission_error(self):
        s3 = Mock()
        s3.head_bucket.side_effect = ClientError(
            {
                "Error": {"Code": "403"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            },
            "HeadBucket",
        )

        with self.assertRaises(ClientError):
            ensure_bucket(s3)

        s3.create_bucket.assert_not_called()

    def test_ensure_bucket_retries_transient_storage_connection_error(self):
        s3 = Mock()
        s3.head_bucket.side_effect = [
            BotoCoreError(error_msg="not ready"),
            None,
            None,
            None,
        ]

        with patch("ingest.time.sleep") as sleep:
            ensure_bucket_with_retry(s3, max_attempts=3)

        self.assertEqual(s3.head_bucket.call_count, 4)
        sleep.assert_called_once_with(2)


if __name__ == "__main__":
    unittest.main()
