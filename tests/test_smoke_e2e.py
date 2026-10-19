import os
import unittest
from unittest.mock import Mock, patch

import duckdb
from botocore.exceptions import ClientError

from tests import smoke_e2e


class SmokeCleanupTests(unittest.TestCase):
    def test_existing_partition_is_not_deleted(self):
        s3 = Mock()
        s3.list_buckets.return_value = {
            "Buckets": [
                {"Name": "bronze"},
                {"Name": "silver"},
                {"Name": "gold"},
            ]
        }
        existing_key = "vehicle_type=bus/date=2020-01-15/hour=12/existing.jsonl"
        s3.list_objects_v2.return_value = {"Contents": [{"Key": existing_key}]}
        s3.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": existing_key}]}
        ]

        with (
            patch.dict(
                os.environ,
                {
                    "S3_ENDPOINT_URL": "http://unused.invalid",
                    "MINIO_USER": "test-user",
                    "MINIO_PASSWORD": "test-password",
                },
            ),
            patch.object(smoke_e2e.boto3, "client", return_value=s3),
            patch.object(smoke_e2e.bronze_to_silver, "get_connection") as connect,
            self.assertRaises(FileExistsError),
        ):
            smoke_e2e.main()

        connect.assert_not_called()
        s3.put_object.assert_called_once_with(
            Bucket="bronze",
            Key=smoke_e2e.SMOKE_LOCK_KEY,
            Body=b"smoke-test",
            IfNoneMatch="*",
        )
        s3.delete_object.assert_called_once_with(
            Bucket="bronze",
            Key=smoke_e2e.SMOKE_LOCK_KEY,
        )
        s3.delete_objects.assert_not_called()
        s3.get_paginator.assert_not_called()

    def test_existing_silver_versions_block_smoke_test(self):
        for directory in ("_versions", "_staging"):
            with self.subTest(directory=directory):
                s3 = Mock()
                occupied_prefix = f"{directory}/date=2020-01-15/hour=12/"

                def list_objects(
                    Bucket, Prefix, MaxKeys, occupied_prefix=occupied_prefix
                ):
                    if Bucket == "silver" and Prefix == occupied_prefix:
                        return {
                            "Contents": [
                                {"Key": f"{occupied_prefix}existing/data.parquet"}
                            ]
                        }
                    return {}

                s3.list_objects_v2.side_effect = list_objects

                with self.assertRaises(FileExistsError):
                    smoke_e2e.assert_test_partition_empty(
                        s3, smoke_e2e.DATE, smoke_e2e.HOUR
                    )

                s3.delete_object.assert_not_called()

    def test_failed_transform_cleans_up_created_silver_version(self):
        s3 = Mock()
        s3.list_buckets.return_value = {
            "Buckets": [
                {"Name": "bronze"},
                {"Name": "silver"},
                {"Name": "gold"},
            ]
        }
        s3.list_objects_v2.return_value = {}
        created_key = "_versions/date=2020-01-15/hour=12/test-publication/data.parquet"
        objects = []

        def fail_transform(*args, **kwargs):
            objects.append(created_key)
            raise RuntimeError("Simulated transform failure")

        def paginate(Bucket, Prefix):
            return [
                {
                    "Contents": [
                        {"Key": key}
                        for key in objects
                        if Bucket == "silver" and key.startswith(Prefix)
                    ]
                }
            ]

        s3.get_paginator.return_value.paginate.side_effect = paginate

        with (
            patch.dict(
                os.environ,
                {
                    "S3_ENDPOINT_URL": "http://unused.invalid",
                    "MINIO_USER": "test-user",
                    "MINIO_PASSWORD": "test-password",
                },
            ),
            patch.object(smoke_e2e.boto3, "client", return_value=s3),
            patch.object(smoke_e2e.bronze_to_silver, "get_connection"),
            patch.object(
                smoke_e2e.bronze_to_silver,
                "transform_hour",
                side_effect=fail_transform,
            ),
            self.assertRaisesRegex(RuntimeError, "Simulated transform failure"),
        ):
            smoke_e2e.main()

        s3.delete_object.assert_any_call(Bucket="silver", Key=created_key)

    def test_existing_metadata_blocks_smoke_test(self):
        cases = (
            ("bronze", "_manifests"),
            ("gold", "_diagnostics"),
        )
        for bucket, directory in cases:
            with self.subTest(bucket=bucket, directory=directory):
                s3 = Mock()
                occupied_prefix = f"{directory}/date=2020-01-15/hour=12/"

                def list_objects(
                    Bucket,
                    Prefix,
                    MaxKeys,
                    expected_bucket=bucket,
                    expected_prefix=occupied_prefix,
                ):
                    if Bucket == expected_bucket and Prefix == expected_prefix:
                        return {
                            "Contents": [{"Key": f"{expected_prefix}existing.json"}]
                        }
                    return {}

                s3.list_objects_v2.side_effect = list_objects

                with self.assertRaises(FileExistsError):
                    smoke_e2e.assert_test_partition_empty(
                        s3, smoke_e2e.DATE, smoke_e2e.HOUR
                    )

                s3.delete_object.assert_not_called()

    def test_existing_lock_blocks_smoke_test(self):
        s3 = Mock()
        s3.put_object.side_effect = ClientError(
            {
                "Error": {"Code": "PreconditionFailed"},
                "ResponseMetadata": {"HTTPStatusCode": 412},
            },
            "PutObject",
        )

        with self.assertRaisesRegex(RuntimeError, "already running"):
            smoke_e2e.acquire_smoke_lock(s3)

        self.assertEqual(s3.put_object.call_count, 1)
        self.assertEqual(s3.put_object.call_args.kwargs["IfNoneMatch"], "*")
        s3.delete_object.assert_not_called()

    def test_main_stops_when_lock_is_unavailable(self):
        s3 = Mock()
        s3.list_buckets.return_value = {
            "Buckets": [
                {"Name": "bronze"},
                {"Name": "silver"},
                {"Name": "gold"},
            ]
        }

        with (
            patch.dict(
                os.environ,
                {
                    "S3_ENDPOINT_URL": "http://unused.invalid",
                    "MINIO_USER": "test-user",
                    "MINIO_PASSWORD": "test-password",
                },
            ),
            patch.object(smoke_e2e.boto3, "client", return_value=s3),
            patch.object(
                smoke_e2e,
                "acquire_smoke_lock",
                side_effect=RuntimeError("Smoke test is already running"),
            ) as acquire,
            self.assertRaisesRegex(RuntimeError, "already running"),
        ):
            smoke_e2e.main()

        acquire.assert_called_once_with(s3)
        s3.list_objects_v2.assert_not_called()
        s3.put_object.assert_not_called()
        s3.get_paginator.assert_not_called()
        s3.delete_object.assert_not_called()

    def test_main_releases_lock_when_smoke_test_fails(self):
        s3 = Mock()
        s3.list_buckets.return_value = {
            "Buckets": [
                {"Name": "bronze"},
                {"Name": "silver"},
                {"Name": "gold"},
            ]
        }

        with (
            patch.dict(
                os.environ,
                {
                    "S3_ENDPOINT_URL": "http://unused.invalid",
                    "MINIO_USER": "test-user",
                    "MINIO_PASSWORD": "test-password",
                },
            ),
            patch.object(smoke_e2e.boto3, "client", return_value=s3),
            patch.object(smoke_e2e, "acquire_smoke_lock") as acquire,
            patch.object(
                smoke_e2e,
                "run_smoke_test",
                side_effect=RuntimeError("Simulated smoke failure"),
            ),
            self.assertRaisesRegex(RuntimeError, "Simulated smoke failure"),
        ):
            smoke_e2e.main()

        acquire.assert_called_once_with(s3)
        s3.delete_object.assert_called_once_with(
            Bucket="bronze",
            Key=smoke_e2e.SMOKE_LOCK_KEY,
        )

    def test_close_failure_does_not_hide_transform_failure_or_skip_cleanup(self):
        s3 = Mock()
        s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]
        con = Mock()
        con.close.side_effect = duckdb.Error("Simulated close failure")

        with (
            patch.object(smoke_e2e, "assert_test_partition_empty"),
            patch.object(
                smoke_e2e.bronze_to_silver,
                "get_connection",
                return_value=con,
            ),
            patch.object(
                smoke_e2e.bronze_to_silver,
                "transform_hour",
                side_effect=RuntimeError("Simulated transform failure"),
            ),
            self.assertRaisesRegex(RuntimeError, "Simulated transform failure"),
        ):
            smoke_e2e.run_smoke_test(s3)

        con.close.assert_called_once_with()
        s3.get_paginator.assert_called()

    def test_delete_failure_does_not_stop_cleanup_or_hide_transform_failure(self):
        s3 = Mock()
        prefix = "_versions/date=2020-01-15/hour=12/"
        first_key = f"{prefix}test-publication/data.parquet"
        second_key = f"{prefix}test-publication/quarantine.parquet"

        def paginate(Bucket, Prefix):
            if Bucket == "silver" and Prefix == prefix:
                return [{"Contents": [{"Key": first_key}, {"Key": second_key}]}]
            return [{"Contents": []}]

        def delete_object(Bucket, Key):
            if Key == first_key:
                raise ClientError(
                    {
                        "Error": {"Code": "AccessDenied"},
                        "ResponseMetadata": {"HTTPStatusCode": 403},
                    },
                    "DeleteObject",
                )

        s3.get_paginator.return_value.paginate.side_effect = paginate
        s3.delete_object.side_effect = delete_object

        with (
            patch.object(smoke_e2e, "assert_test_partition_empty"),
            patch.object(smoke_e2e.bronze_to_silver, "get_connection"),
            patch.object(
                smoke_e2e.bronze_to_silver,
                "transform_hour",
                side_effect=RuntimeError("Simulated transform failure"),
            ),
            self.assertRaisesRegex(RuntimeError, "Simulated transform failure"),
        ):
            smoke_e2e.run_smoke_test(s3)

        s3.delete_object.assert_any_call(Bucket="silver", Key=first_key)
        s3.delete_object.assert_any_call(Bucket="silver", Key=second_key)

    def test_lock_release_failure_does_not_hide_smoke_failure(self):
        s3 = Mock()
        s3.list_buckets.return_value = {
            "Buckets": [
                {"Name": "bronze"},
                {"Name": "silver"},
                {"Name": "gold"},
            ]
        }
        s3.delete_object.side_effect = ClientError(
            {
                "Error": {"Code": "AccessDenied"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            },
            "DeleteObject",
        )

        with (
            patch.dict(
                os.environ,
                {
                    "S3_ENDPOINT_URL": "http://unused.invalid",
                    "MINIO_USER": "test-user",
                    "MINIO_PASSWORD": "test-password",
                },
            ),
            patch.object(smoke_e2e.boto3, "client", return_value=s3),
            patch.object(smoke_e2e, "acquire_smoke_lock"),
            patch.object(
                smoke_e2e,
                "run_smoke_test",
                side_effect=RuntimeError("Simulated smoke failure"),
            ),
            self.assertRaisesRegex(RuntimeError, "Simulated smoke failure"),
        ):
            smoke_e2e.main()
