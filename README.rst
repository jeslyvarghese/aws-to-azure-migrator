Azure Migrator
==============

This is a command line utility for bulk migration of contents to azure storage. At the moment this includes only an S3-Azure Storage movement utility

Usage::

  migrate-to-azure --azure-storage-account=<> --azure-storage-key=<> --aws-region-name=us-west-2 --aws-access-key=<> --aws-secret-key=<> --thread-count<>

By default the thread count is set to 100.

Usage::

  migrate-to-azure --help

============
Installation
============
::
   pip install  git+https://github.com/jeslyvarghese/aws-to-azure-migrator.git


Pull requests are welcome. If you want more providers added or in case of bugs please create an issue.
