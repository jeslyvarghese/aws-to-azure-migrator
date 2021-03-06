from __future__ import absolute_import
import boto3
from boto3 import s3
from boto3.s3.transfer import TransferConfig

class Boto3Session(object):
    S3 = 's3'
    def __init__(self, region_name, access_key, secret):
        self.region_name = region_name
        self.access_key = access_key
        self.secret = secret

    def create_session(self):
        return boto3.Session(region_name=self.region_name,
                             aws_access_key_id=self.access_key,
                             aws_secret_access_key=self.secret)

class S3(object):
    OWNER_KEY = 'Owner'
    BUCKETS_KEY = 'Buckets'
    BUCKET_NAME_KEY = 'Name'
    BUCKET_CREATION_DATE_KEY = 'CreationDate'
    def __init__(self, session):
        self.session = session
        self.resource = self.session.resource(Boto3Session.S3)
        self.client = self.session.client(Boto3Session.S3)

    def list_buckets(self):
        buckets = []
        b_resp_dict = self.client.list_buckets()
        owner = BucketOwner(name=b_resp_dict[S3.OWNER_KEY].get('DisplayName', None), identifier=b_resp_dict[S3.OWNER_KEY].get('ID', None))
        bucket_list = b_resp_dict[S3.BUCKETS_KEY]
        for b in bucket_list:
            buckets.append(Bucket(
                s3=self,
                name=b[S3.BUCKET_NAME_KEY],
                creation_date=b[S3.BUCKET_CREATION_DATE_KEY],
                owner=owner))
        return buckets
            


class Bucket(object):
    def __init__(self, s3, name, creation_date=None, owner=None):
        self.s3 = s3
        self.name = name
        self.creation_date = creation_date
        self.owner = owner
    
    def list_objects(self):
        list_response = self.s3.client.list_objects_v2(Bucket=self.name)
        is_list_truncated = list_response['IsTruncated']
        contents = list_response.get('Contents', [])
        if is_list_truncated:
            next_marker = list_response.get('NextContinuationToken', None)
            while next_marker is not None:
                list_response = self.s3.client.list_objects_v2(Bucket=self.name, ContinuationToken=next_marker)
                next_marker = list_response.get('NextContinuationToken', None)
                contents = contents + list_response['Contents']
        bucket_objects = []
        for item in contents:
            bucket_objects.append(BucketObject(
                bucket=self,
                key=item['Key'],
                last_modified=item['LastModified'],
                etag=item['ETag'],
                size=item['Size'],
                storage_class=item['StorageClass'],
                owner=self.owner))
        return bucket_objects
                
        
class BucketOwner(object):
    def __init__(self, name, identifier):
        self.name = name
        self.identifier = identifier


class BucketObject(object):
    def __init__(self, bucket, key, last_modified=None, etag=None, size=None, storage_class=None, owner=None):
        self.bucket = bucket
        self.key = key
        self.last_modified = last_modified
        self.etag = etag
        self.size = size
        self.storage_class=storage_class
        self.owner = owner

    def download(self, filepath, callback=None, use_threads=False):
        transfer_config = TransferConfig(use_threads=use_threads)
        self.bucket.s3.client.download_file(Bucket=self.bucket.name, Key=self.key, Filename=filepath, Callback=callback, Config=transfer_config)
