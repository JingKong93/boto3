import boto3
import threading
import sys
from botocore.client import Config
from dataclasses import dataclass


@dataclass()
class S3Params(object):
    file_name: str = "/Users/lu-wang/Documents/GitHub/recrnn/historySample.csv.gz"
    bucket_name: str = "ecomdatascience-np"
    key: str = "lu/recrnn0328/history/data_0_0_0.csv.gz"


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


s3 = boto3.client('s3', config=Config(signature_version='s3v4'))
s3.download_file(
    Bucket=S3Params.bucket_name,
    Key=S3Params.key,
    Callback=DownloadProgressPercentage(S3Params.file_name),
    Filename=S3Params.file_name
)
