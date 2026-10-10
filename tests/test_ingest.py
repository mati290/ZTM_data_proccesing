import json
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError
from requests import Timeout

from ingest import ZTMApiError, ensure_bucket, fetch_positions, save_to_bronze


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
            patch("ingest.requests.get", return_value=response),
            self.assertRaises(ZTMApiError),
        ):
            fetch_positions(1, "test-key", max_attempts=1)

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

        s3.create_bucket.assert_called_once_with(Bucket="bronze")

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


if __name__ == "__main__":
    unittest.main()
