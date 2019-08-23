import boto3
import threading
import sys
from botocore.client import Config
from dataclasses import dataclass
import os


@dataclass()
class S3Params(object):
    file_name: str = "/Users/jingnicole-kong/Desktop/ecomdatascience-np/BSDSub/subdata0820/"
    bucket_name: str = "ecomdatascience-np"
    key: str = "BSDSub/subdata0820/"
    # kms_key: str = "arn:aws:kms:us-east-1:729964090428:key/3a67dc71-c759-48c9-803a-22ac51fee24d"


class DownloadProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            sys.stdout.write(
                "\r%s --> %s bytes transferred" % (
                    self._filename, self._seen_so_far))
            sys.stdout.flush()
def download_dir(client, resource, dist, local, bucket):
    paginator = client.get_paginator('list_objects')
    for result in paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=dist):
        if result.get('CommonPrefixes') is not None:
            for subdir in result.get('CommonPrefixes'):
                download_dir(client, resource, subdir.get('Prefix'), local, bucket)
        for file in result.get('Contents', []):
            dest_pathname = os.path.join(local, file.get('Key'))
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
            resource.meta.client.download_file(bucket, file.get('Key'), dest_pathname)
s3 = boto3.client('s3', config=Config(signature_version='s3v4'))
resource = boto3.resource('s3')
download_dir(s3,resource,S3Params.key,S3Params.file_name,S3Params.bucket_name)
print("Download complete")

# s3 = boto3.client('s3', config=Config(signature_version='s3v4'))
# s3.download_file(
#     Bucket=S3Params.bucket_name,
#     Key=S3Params.key,
#     Callback=DownloadProgressPercentage(S3Params.file_name),
#     Filename=S3Params.file_name
#     # ExtraArgs={
#     #     "ServerSideEncryption": "aws:kms",
#     #     "SSEKMSKeyId": S3Params.kms_key
#     # }
# )
