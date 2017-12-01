from __future__ import absolute_import

import click
from migrator.migrations.azure import ToAzureFromAWS

@click.command()
@click.option('--azure-storage-account', default=None, help='Azure Storage Account Name (Required)')
@click.option('--azure-storage-key', default=None, help='Azure Storage Account Key (Required)')
@click.option('--aws-region-name', default='us-west-2', help='AWS Authentication Region Defaults us-west-2')
@click.option('--aws-access-key', default=None, help='AWS Access Key (Required)')
@click.option('--aws-secret-key', default=None, help='AWS Secret Key (Required)')
def run(azure_storage_account, azure_storage_key, aws_region_name, aws_access_key, aws_secret_key):
    ToAzureFromAWS(azure_storage_account_name=azure_storage_account,
                   azure_storage_key=azure_storage_key,
                   aws_region_name=aws_region_name,
                   aws_access_key=aws_access_key,
                   aws_secret_key=aws_secret_key).migrate()

