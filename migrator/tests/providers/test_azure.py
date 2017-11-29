from azure import storage
from providers.azure import AzureBlobStorage
from providers.azure import AzureContainer
import unittest
import os
import time

class TestAttributes(object):
    def setup_attributes(self):
        self.account_name = os.environ.get('AZURE_TEST_ACCOUNT_NAME')
        self.storage_key = os.environ.get('AZURE_TEST_STORAGE_KEY')
        self.container_name = 'testcontainer'
        
class TestAzureBlobStorage(unittest.TestCase, TestAttributes):
    def setUp(self):
        self.setup_attributes()
        self.blob_storage = AzureBlobStorage(
            storage_account_name=self.account_name,
            storage_key=self.storage_key
        )
    def test_create_container(self):
        self.blob_storage.create_container(
            container_name = self.container_name,
            public_access=AzureBlobStorage.PUBLIC_ACCESS_CONTAINER)
        time.sleep(10)
        container_exists = self.blob_storage.service.exists(container_name=self.container_name)
        self.assertTrue(container_exists)

    def tearDown(self):
        self.blob_storage.service.delete_container(container_name=self.container_name)
        time.sleep(20)

class TestAzureContainer(unittest.TestCase, TestAttributes):        
    def setUp(self):
        self.setup_attributes()
        self.blob_storage = AzureBlobStorage(
            storage_account_name=self.account_name,
            storage_key=self.storage_key)
        self.blob_storage.create_container(container_name=self.container_name, public_access=AzureBlobStorage.PUBLIC_ACCESS_CONTAINER)
        time.sleep(10)
        self.filepath = "/tmp/testfile.txt"
        file = open(self.filepath, "w")
        file.write("Sample File")
        file.close()

    def test_create_blob_from_filepath(self):
        container = AzureContainer(name=self.container_name, blob_storage=self.blob_storage)
        container.create_blob_from_filepath(filepath=self.filepath)
        time.sleep(10)
        file_exists = self.blob_storage.service.exists(container_name=self.container_name, blob_name=os.path.basename(self.filepath))
        self.assertTrue(file_exists)

    def tearDown(self):
        self.blob_storage.service.delete_container(container_name=self.container_name)
        time.sleep(20)
