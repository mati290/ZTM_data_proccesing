import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

from botocore.exceptions import ClientError

from check_ingestion_health import check_ingestion_health


class IngestionHealthTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 13, 10, 0, tzinfo=timezone.utc)
        self.s3 = Mock()
        self.paginator = self.s3.get_paginator.return_value

    def set_manifest(self, age_seconds, status="complete"):
        self.paginator.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": "_manifests/test.json",
                        "LastModified": self.now - timedelta(seconds=age_seconds),
                    }
                ]
            }
        ]

        self.s3.get_object.return_value = {
            "Body": Mock(
                read=Mock(return_value=json.dumps({"status": status}).encode("utf-8"))
            )
        }

    def test_non_object_manifest_is_unhealthy(self):
        for payload in ([], None, "complete", 123, True):
            with self.subTest(payload=payload):
                self.set_manifest(30)
                self.s3.get_object.return_value = {
                    "Body": Mock(
                        read=Mock(return_value=json.dumps(payload).encode("utf-8"))
                    )
                }

                healthy, message = check_ingestion_health(
                    self.s3,
                    max_age_seconds=120,
                    now=self.now,
                )

                self.assertFalse(healthy)
                self.assertIn("manifest", message.lower())

    def test_fresh_manifest_is_healthy(self):
        self.set_manifest(30)

        healthy, message = check_ingestion_health(
            self.s3, max_age_seconds=120, now=self.now
        )

        self.assertTrue(healthy)
        self.assertIn("fresh", message)

    def test_fresh_partial_manifest_is_unhealthy(self):
        self.set_manifest(30, status="partial")

        healthy, message = check_ingestion_health(
            self.s3, max_age_seconds=120, now=self.now
        )

        self.assertFalse(healthy)
        self.assertIn("partial", message.lower())

    def test_uses_latest_manifest_by_last_modified(self):
        older = self.now - timedelta(seconds=60)
        newer = self.now - timedelta(seconds=10)

        self.paginator.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": "_manifests/older.json",
                        "LastModified": older,
                    },
                    {
                        "Key": "_manifests/newer.json",
                        "LastModified": newer,
                    },
                ]
            }
        ]

        self.s3.get_object.return_value = {
            "Body": Mock(
                read=Mock(
                    return_value=json.dumps({"status": "complete"}).encode("utf-8")
                )
            )
        }

        healthy, _ = check_ingestion_health(self.s3, max_age_seconds=120, now=self.now)

        self.assertTrue(healthy)
        self.s3.get_object.assert_called_once_with(
            Bucket="bronze", Key="_manifests/newer.json"
        )

    def test_stale_manifest_is_unhealthy(self):
        self.set_manifest(121)

        healthy, message = check_ingestion_health(
            self.s3, max_age_seconds=120, now=self.now
        )

        self.assertFalse(healthy)
        self.assertIn("stale", message)

    def test_missing_manifest_is_unhealthy(self):
        self.paginator.paginate.return_value = [{"Contents": []}]

        healthy, message = check_ingestion_health(
            self.s3, max_age_seconds=120, now=self.now
        )

        self.assertFalse(healthy)
        self.assertIn("No Bronze cycle manifests", message)

    def test_s3_error_is_unhealthy(self):
        self.paginator.paginate.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "test"}},
            "ListObjectsV2",
        )

        healthy, message = check_ingestion_health(
            self.s3, max_age_seconds=120, now=self.now
        )

        self.assertFalse(healthy)
        self.assertIn("S3 manifest check failed", message)

    def test_non_positive_limit_is_unhealthy(self):
        healthy, message = check_ingestion_health(
            self.s3, max_age_seconds=0, now=self.now
        )

        self.assertFalse(healthy)
        self.assertIn("must be positive", message)
        self.s3.get_paginator.assert_not_called()


if __name__ == "__main__":
    unittest.main()
