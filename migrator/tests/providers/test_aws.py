from boto3 import s3
from providers.aws import Boto3Session
from providers.aws import S3
from providers.aws import Bucket
from providers.aws import BucketOwner
from providers.aws import BucketObject

import unittest
import os
import time

class TestAttributes(object):
    def setup_attributes(self):
        self.region_name = os.environ.get('AWS_TEST_REGION_NAME')
        self.access_key = os.environ.get('AWS_TEST_ACCESS_KEY')
        self.secret = os.environ.get('AWS_TEST_ACCESS_SECRET')
        self.boto3_session = Boto3Session(region_name=self.region_name,
                                          access_key=self.access_key,
                                          secret=self.secret).create_session()

class TestS3Container(unittest.TestCase, TestAttributes):
    def setUp(self):
        self.setup_attributes()
        
    def test_list_buckets(self):
        s3 = S3(session=self.boto3_session)
        buckets = s3.list_buckets()
        self.assertGreater(len(buckets), 0)
        self.assertItemsEqual(type(buckets[0]).__name__, 'Bucket')

class TestBucket(unittest.TestCase, TestAttributes):
    def setUp(self):
        self.setup_attributes()
        self.s3 = S3(session=self.boto3_session)

    def test_list_objects_no_objects(self):
        bucket_name = 'migrator-test-bucket'
        bucket = Bucket(s3=self.s3, name=bucket_name)
        objects = bucket.list_objects()
        self.assertEqual(len(objects), 0)

    def test_list_objects_with_objects(self):
        bucket_name = 'migrator-test-bucket-with-objects'
        bucket = Bucket(s3=self.s3, name=bucket_name)
        objects = bucket.list_objects()
        self.assertGreater(len(objects), 0)

class TestBucketObject(unittest.TestCase, TestAttributes):
    def setUp(self):
        self.setup_attributes()
        self.s3 = S3(session=self.boto3_session)
    
    def test_download(self):
        bucket_name = 'migrator-test-bucket-with-objects'
        bucket = Bucket(s3=self.s3, name=bucket_name)
        objects = bucket.list_objects()
        bucket_object = objects[0]
        self.filepath = "/tmp/"+bucket_object.key
        bucket_object.download(filepath=self.filepath)
        self.assertTrue(os.path.isfile(self.filepath))
        size = os.path.getsize(self.filepath)
        self.assertEqual(size, bucket_object.size)

    def tearDown(self):
        os.remove(self.filepath)
