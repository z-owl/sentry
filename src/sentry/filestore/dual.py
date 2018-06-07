from __future__ import absolute_import

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
            return self.storage_read._open(name, mode)
            # TODO: look up exception via storages
            # boto exceptions are generated via factory; cannot be caught even with bare except
            # https://stackoverflow.com/questions/46174385/properly-catch-boto3-errors
            # https://stackoverflow.com/questions/33068055/boto3-python-and-how-to-handle-errors
            # https://stackoverflow.com/questions/42975609/how-to-capture-botocores-nosuchkey-exception/44811870
        except BaseException:
            # fallback to switching secondary to active
            if self.storage_read_secondary is not None:
                self.storage_read = self.storage_read_secondary
                return self.storage_read._open(name, mode)
            raise DualStorageException('primary storage failed and no fallback was specified')

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
