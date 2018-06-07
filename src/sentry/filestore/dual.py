from __future__ import absolute_import

from botocore.exceptions import ClientError
from google.cloud.exceptions import NotFound
from django.core.files.storage import Storage

from . import s3
from . import gcs


class DualStorageException(Exception):
    pass


class DualStorage(Storage):

    def __init__(self, **settings):
        storages = {
            's3': s3.S3Boto3Storage(
                access_key=settings['s3_access_key'],
                secret_key=settings['s3_secret_key'],
                bucket_name=settings['s3_bucket_name'],
                **settings
            ),
            'gcs': gcs.GoogleCloudStorage(bucket_name=settings['gcs_bucket_name'], **settings),
        }

        self.storage_read_primary = storages[settings['read'][0]]
        try:
            self.storage_read_secondary = storages[settings['read'][1]]
        except IndexError:
            self.storage_read_secondary = None

        self.storage_write_primary = storages[settings['write'][0]]
        try:
            self.storage_write_secondary = storages[settings['write'][1]]
        except IndexError:
            self.storage_write_secondary = None

        self.storage_read = self.storage_read_primary
        self.storage_write = self.storage_write_primary

    def _open(self, name, mode='rb'):
        # always ensure primary runs
        self.storage_read = self.storage_read_primary
        try:
            stuff = self.storage_read._open(name, mode)
            stuff.seek(0)  # use network to detect existence of file
            return stuff
        except (ClientError, NotFound) as e:
            print('WARNING: trying fallback storage; primary storage failed:', e)
            # fallback to switching secondary to active
            if self.storage_read_secondary is not None:
                self.storage_read = self.storage_read_secondary
                return self.storage_read._open(name, mode)
            raise DualStorageException('FATAL: no fallback storage was specified')

    def _save(self, name, content):
        self.storage_write = self.storage_write_primary
        self.storage_write._save(name, content)
        self.storage_write = self.storage_write_secondary
        self.storage_write._save(name, content)
        return name  # apparently this needs to be cleaned?

    def delete(self, name):
        raise NotImplementedError()

    def exists(self, name):
        return self.storage_read.exists(name)
