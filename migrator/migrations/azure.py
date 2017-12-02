from __future__ import absolute_import

from migrator.providers.azure import AzureBlobStorage
from migrator.providers.azure import AzureContainer
from migrator.providers.azure import AzureContainerCreationFailedException

from migrator.providers.aws import Boto3Session
from migrator.providers.aws import S3
from migrator.providers.aws import Bucket
from migrator.providers.aws import BucketObject

import logging

from pythonjsonlogger import jsonlogger

from tqdm import trange
from tqdm import tqdm 

import os
import sys
import threading
import re
import time

logger = logging.getLogger()
formatter = jsonlogger.JsonFormatter()
fileHandler = logging.FileHandler("azure-migrations.json")
fileHandler.setFormatter (formatter)
logger.addHandler(fileHandler)

class ToAzureFromAWS(object):
    def __init__(
            self,
            azure_storage_account_name,
            azure_storage_key,
            aws_region_name,
            aws_access_key,
            aws_secret_key):
        self.blob_storage = AzureBlobStorage(storage_account_name=azure_storage_account_name,
                                             storage_key=azure_storage_key)
        self.s3 = S3(session=Boto3Session(region_name=aws_region_name,
                                          access_key=aws_access_key,
                                          secret=aws_secret_key).create_session())
        self.buckets = []
        self.buckets_copied = []
        self.buckets_to_container_translation = {}
        self.num_of_containers_to_copy = 0
        self.num_of_containers_copied = 0
        self._bucket_download_progress = None
        self._bucket_item_download_progress = {}
        self._container_upload_progress = None
        self._container_item_upload_progress = {}
        self.bucket_count = 0
        self.buckets_downloaded_count = 0
        self.bucket_items_count = {}
        self.bucket_items_downloaded_count = {}
        self.container_uploaded_count = 0
        self.container_items_upload_count = {}

    def _create_container(self, name):
        return self.blob_storage.create_container(container_name=name, public_access=AzureBlobStorage.PUBLIC_ACCESS_BLOB)

    
    def get_s3_buckets(self):
        return self.s3.list_buckets()
    
    def _translate_to_azure_friendly_name(self, name):
        # based on docs: https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
        friendly_name = name.lower()
        return re.sub(r"[^a-z0-9\-]+", "-", friendly_name)

    def copy_buckets_from_s3_to_azure(self, buckets):
        pbar = tqdm(total=len(buckets), desc="Creating containers")
        for bucket in buckets:
            self._copy_bucket_from_s3_to_azure(bucket=bucket)
            pbar.update(1)
        pbar.close()

    def _copy_bucket_from_s3_to_azure(self, bucket):
        try:
            container_name = self._translate_to_azure_friendly_name(name=bucket.name)
            self.buckets_to_container_translation[bucket.name] = container_name
            if not self.blob_storage.container_exists(container_name=container_name):
                self.blob_storage.create_container(container_name=container_name,
                                               public_access=AzureBlobStorage.PUBLIC_ACCESS_BLOB)
            self.buckets_copied.append(bucket)
        except AzureContainerCreationFailedException as e:
            logger.error({"operation": "container creation", "status": "failed", "container-name": container_name, "bucket-name": bucket.name})
    
    
    def _finished_copying_to_container(self, filepath, container):
        logger.info({"operation": "uploading to container", "status": "success", "filepath": filepath, "container-name": container.name})
        try:
            self._container_item_upload_progress[container.name].update(1)
        except KeyError:
            pass
        try:
            if  self._container_item_upload_progress[container.name].n >= self.container_items_upload_count[container.name]:
                if self._container_upload_progress is not None:
                    self._container_upload_progress.update(1)
        except KeyError:
            pass
        os.remove(filepath)

    def _copy_item_from_system_to_container(self, filepath, bucket_item):
        container_name = self.buckets_to_container_translation[bucket_item.bucket.name]
        container = AzureContainer(name=container_name, blob_storage=self.blob_storage)
        logger.info({"operation": "uploading to container", "status": "started", "filepath": filepath, "bucket-name": bucket_item.bucket.name, "item-key": bucket_item.key, "container-name": container_name})
        container.create_blob_from_filepath(filepath=filepath,
                                            progress_callback=AzureBlobUploadProgress(filepath=filepath,
                                                                                      container=container,
                                                                                      finish_callback=self._finished_copying_to_container))


    def _copy_items_from_bucket_to_system(self, bucket):
        items = bucket.list_objects()
        
        try:
            container_save_path = "/tmp/aws_azure_copy/aws/%s" % (bucket.name)
            os.makedirs(container_save_path)
        except OSError:
            logger.warn({"operation": "file creation", "status": "os_error", "path": container_save_path, "bucket-name": bucket.name})
        container_name = self.buckets_to_container_translation[bucket.name]
        if self.blob_storage.service.exists(container_name=container_name):
            self._bucket_item_download_progress[bucket.name] = trange(len(items), desc="%s Item Download"%(bucket.name))
            self._container_item_upload_progress[container_name] = trange(len(items), desc="%s Items Copied"%(container_name))
            self.container_items_upload_count[container_name] = len(items)
            container = AzureContainer(name=container_name, blob_storage=self.blob_storage)
            for item in items:
                if not container.blob_exists(name=item.key):
                    self._copy_item_from_bucket_to_system(item=item,
                                                          save_path=container_save_path)
                else:
                    self._container_item_upload_progress[container_name].update(1)
                    if self._container_item_upload_progress[container_name].n >= self.container_items_upload_count[container_name]:
                        if self._container_upload_progress is not None:
                            self._container_upload_progress.update(1)
                self._bucket_item_download_progress[bucket.name].update(1)
        else:
            raise AzureContainerNotCreatedException()
            
    def _copy_item_from_bucket_to_system(self, item, save_path):
        filepath = "%s/%s" % (save_path, item.key)
        logger.info({"operation": "downloading item from bucket", "status": "started", "item-key": item.key, "bucket-name": item.bucket.name})
        item.download(filepath=filepath)
        logger.info({"operation": "downloading item from bucket", "status": "success", "item-key": item.key, "bucket-name": item.bucket.name})
        self._copy_item_from_system_to_container(filepath=filepath, bucket_item=item)

    def _move_to_azure(self):
        buckets = self.get_s3_buckets()
        self.copy_buckets_from_s3_to_azure(buckets=buckets)
        self._bucket_download_progress = trange(len(buckets), desc="Buckets Download Progress")
        self._container_upload_progress = trange(len(buckets), desc="Container Creation Progress")
        for bucket in buckets:
            try:
                self._copy_items_from_bucket_to_system(bucket=bucket)
                self._bucket_download_progress.update(1)
            except AzureContainerNotCreatedException as e:
                logger.error({"operation": "container not created", "status": "failed", "bucket-name": bucket.name, "container-name": self.buckets_to_container_translation[bucket.name]})
                continue

    def migrate(self):
        self._move_to_azure()

class AzureContainerNotCreatedException(Exception):
    pass
class S3BucketObjectNotCopiedException(Exception):
    pass

class S3DownloadProgress(object):
    def __init__(self, filepath, bucket_item, finish_callback=None):
        self._filepath = filepath
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._bucket_item = bucket_item
        self._finish_callback = finish_callback

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            if self._seen_so_far >= self._bucket_item.size:
                # download has finished
                if self._finish_callback is not None:
                    # assert file exists
                    if os.path.isfile(self._filepath):
                        self._finish_callback(self._filepath, self._bucket_item)
                    else:
                        raise S3BucketObjectNotCopiedException()
                    
class AzureBlobUploadProgress(object):
    def __init__(self, filepath, container, finish_callback=None):
        self._lock = threading.Lock()
        self._filepath = filepath
        self._container = container
        self._finish_callback = finish_callback
    
    
    def __call__(self, current, total):
        with self._lock:
            if current >= total:
                if self._finish_callback is not None:
                    time.sleep(10)
                    self._finish_callback(self._filepath, self._container)
            
