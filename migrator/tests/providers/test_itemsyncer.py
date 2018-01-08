from migrator.migrations.azure import ItemSyncer
from migrator.migrations.azure import ToAzureFromAWS
from migrator.providers.aws import Bucket
from migrator.providers.aws import BucketObject
from migrator.providers.aws import Boto3Session
from migrator.providers.aws import S3

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

        
class TestItemSyncer(unittest.TestCase, TestAttributes):
    
    def setUp(self):
        self.setup_attributes()

    def test_sync(self):
        session = Boto3Session(
            region_name=self.aws_region_name,
            access_key=self.aws_access_key,
            secret=self.aws_secret).create_session()
        s3 = S3(session=session)
        bucket = Bucket(s3=s3, name='tm-test-bucket-1')
        item  = BucketObject(bucket=bucket, key='out.mp4')
        try:
            container_save_path = "/tmp/aws_azure_copy/aws/%s" % (bucket.name)
            os.makedirs(container_save_path)
        except OSError:
            pass
        migrator = ToAzureFromAWS(
            azure_storage_account_name=self.azure_account_name,
            azure_storage_key=self.azure_storage_key,
            aws_region_name=self.aws_region_name,
            aws_access_key=self.aws_access_key,
            aws_secret_key=self.aws_secret)
        migrator.buckets_to_container_translation = {'tm-test-bucket-1': 'tm-test-bucket-1' }
        migrator._copy_bucket_from_s3_to_azure(bucket=bucket)
        item_sync = ItemSyncer(
            item=item,
            container_save_path=container_save_path,
            migrator=migrator)
        item_sync.start()
        item_sync.join()
        time.sleep(10)
        item_exists = migrator.blob_storage.service.exists(container_name=bucket.name, blob_name='out.mp4')
        self.assertTrue(item_exists)
        migrator.blob_storage.service.delete_container(container_name=bucket.name)

