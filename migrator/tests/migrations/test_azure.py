from azure import storage
from migrator.migrations.azure import ToAzureFromAWS
from migrator.providers.aws import Bucket
from migrator.providers.aws import BucketObject

import unittest
import time
import os

class TestAttributes(object):
    def setup_attributes(self):
        self.azure_account_name = os.environ.get('AZURE_TEST_ACCOUNT_NAME')
        self.azure_storage_key = os.environ.get('AZURE_TEST_STORAGE_KEY')
        self.aws_region_name = os.environ.get('AWS_TEST_REGION_NAME')
        self.aws_access_key = os.environ.get('AWS_TEST_ACCESS_KEY')
        self.aws_secret = os.environ.get('AWS_TEST_ACCESS_SECRET')

class TestToAzureFromAWS(unittest.TestCase, TestAttributes):
    def setUp(self):
        self.setup_attributes()
        self.aws_to_azure = ToAzureFromAWS(azure_storage_account_name=self.azure_account_name,
                                           azure_storage_key=self.azure_storage_key,
                                           aws_region_name=self.aws_region_name,
                                           aws_access_key=self.aws_access_key,
                                           aws_secret_key=self.aws_secret)

    def test__create_container(self):
        container_name = 'test-container'
        self.aws_to_azure.blob_storage.service.delete_container(container_name=container_name)
        time.sleep(10)
        self.aws_to_azure._create_container(name=container_name)
        time.sleep(10)
        container_exists = self.aws_to_azure.blob_storage.service.exists(container_name=container_name)
        self.assertTrue(container_exists)
        self.aws_to_azure.blob_storage.service.delete_container(container_name=container_name)
        time.sleep(10)

    def test_get_s3_buckets(self):
        buckets = self.aws_to_azure.get_s3_buckets()
        self.assertGreater(len(buckets), 0)

    def test__translate_to_azure_friendly_name(self):
        aws_name = "aws.friendly099-name.com"
        azure_name  = "aws-friendly099-name-com"
        translated_name = self.aws_to_azure._translate_to_azure_friendly_name(name=aws_name)
        self.assertEqual(azure_name, translated_name)
    
    def test__copy_bucket_from_s3_to_azure(self):
        bucket = Bucket(s3=None, name='azure.test.container.name')
        container_name = self.aws_to_azure._translate_to_azure_friendly_name(name=bucket.name)
        self.aws_to_azure._copy_bucket_from_s3_to_azure(bucket=bucket)
        time.sleep(10)
        container_exists = self.aws_to_azure.blob_storage.service.exists(container_name=container_name)
        self.assertTrue(container_exists)
        self.aws_to_azure.blob_storage.service.delete_container(container_name=container_name)
        
    def test_copy_buckets_from_s3_to_azure(self):
        bucket_names = ['testcontainer1', 'testcontainer2', 'testcontainer3', 'testcontaine4']
        buckets = []
        for bucket_name in bucket_names:
            buckets.append(Bucket(s3=None, name=bucket_name))
        self.aws_to_azure.copy_buckets_from_s3_to_azure(buckets=buckets)
        for bucket_name in bucket_names:
            container_name = self.aws_to_azure._translate_to_azure_friendly_name(name=bucket_name)
            container_exists = self.aws_to_azure.blob_storage.service.exists(container_name=container_name)
            self.assertTrue(container_exists)
            self.aws_to_azure.blob_storage.service.delete_container(container_name=container_name)

    def test__finished_copying_to_container(self):
        pass
    
    def test__move_to_azure(self):
        pass
#        self.aws_to_azure._move_to_azure()
