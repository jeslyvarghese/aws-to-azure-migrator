from __future__ import absolute_import
from azure import storage
import os

class AzureContainerCreationFailedException(Exception):
    pass

class AzureBlobStorage(object):
    PUBLIC_ACCESS_BLOB = 'blob'
    PUBLIC_ACCESS_CONTAINER = 'container'
    PUBLIC_ACCESS_OFF = 'off'
    def __init__(self, storage_account_name, storage_key):
        self.client = storage.CloudStorageAccount(storage_account_name, storage_key)
        self.service = self.client.create_block_blob_service()
    
    def create_container(self, container_name, public_access):
        if not self.service.create_container(container_name, public_access=public_access):
            raise AzureContainerCreationFailedException()
    
    def container_exists(self, container_name):
        return self.service.exists(container_name=container_name)

class AzureContainer(object):
    def __init__(self, name, blob_storage):
        self.storage = blob_storage
        self.name = name
    
    def list_blobs(self):
        return self.storage.service.list_blobs(self.name)
    
    def create_blob_from_filepath(self, filepath, blob_name=None, progress_callback=None):
        blob_name = os.path.basename(filepath) if blob_name is None else blob_name
        self.storage.service.create_blob_from_path(container_name=self.name,
                                                   blob_name=blob_name,
                                                   file_path=filepath,
                                                   progress_callback=progress_callback)
    
    def blob_exists(self, name):
        return self.storage.service.exists(container_name=self.name, blob_name=name)
