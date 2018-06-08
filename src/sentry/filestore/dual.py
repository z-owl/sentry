from __future__ import absolute_import

from botocore.exceptions import ClientError
from google.cloud.exceptions import NotFound
from django.core.files.base import File
from django.core.files.storage import Storage

from . import s3
from . import gcs


class DualStorageException(Exception):
    pass


class DualStorageFile(File):

    def __init__(self, name, mode, storage_s3, storage_gcs, read_priority, write_priority):
        files = {
            's3': s3.S3Boto3StorageFile(name, mode, storage_s3),
            'gcs': gcs.GoogleCloudFile(name, mode, storage_gcs),
        }

        self.file_read_primary = files[read_priority[0]]
        try:
            self.file_read_secondary = files[read_priority[1]]
        except IndexError:
            self.file_read_secondary = None

        self.file_write_primary = files[write_priority[0]]
        try:
            self.file_write_secondary = files[write_priority[1]]
        except IndexError:
            self.file_write_secondary = None

        self.file_read = self.file_read_primary
        self.file_write = self.file_write_primary

    @property
    def size(self):
        return self.file_read.size()

    def _get_file(self):
        try:
            return self.file_read._get_file()
        except (ClientError, NotFound) as e:
            print('WARNING: trying fallback storage; primary storage failed:', e)
            # fallback to switching secondary to active
            if self.file_read_secondary is not None:
                self.file_read = self.file_read_secondary
                return self.file_read._get_file()
            raise DualStorageException('FATAL: no fallback storage was specified')

    def _set_file(self, value):
        self.file_write._set_file(value)

    file = property(_get_file, _set_file)

    def read(self, num_bytes=None):
        return self.file_read.read(num_bytes)

    def write(self, content):
        return self.file_write.write(content)

    def close(self):
        return self.file_write.close()


class DualStorage(Storage):

    def __init__(self, **settings):
        self.read_priority = settings['read']
        self.write_priority = settings['write']
        self.storage_s3 = s3.S3Boto3Storage(access_key=settings['s3_access_key'], secret_key=settings['s3_secret_key'], bucket_name=settings['s3_bucket_name'], **settings)
        self.storage_gcs = gcs.GoogleCloudStorage(bucket_name=settings['gcs_bucket_name'], **settings)

    def _open(self, name, mode='rb'):
        return DualStorageFile(name, mode, self.storage_s3, self.storage_gcs, self.read_priority, self.write_priority)

    def _save(self, name, content):
        self.storage_s3._save(name, content)
        self.storage_gcs._save(name, content)
        return name  # apparently this needs to be cleaned?

    def delete(self, name):
        raise NotImplementedError()

    def exists(self, name):
        raise NotImplementedError()
        #return self.storage_read.exists(name)
