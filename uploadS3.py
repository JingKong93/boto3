import boto3
import sys
import os
import threading
from botocore.client import Config
from dataclasses import dataclass


@dataclass()
class S3Params(object):
    file_name: str = "/Users/lu-wang/Documents/GitHub/NeuralEmbedding/target/scala-2.11/neuralembedding_2.11-0.1.jar"
    bucket_name: str = "ecomdatascience-np"
    key: str = "lu/neuralEmbedding.jar"
    kms_key: str = "arn:aws:kms:us-east-1:729964090428:key/3a67dc71-c759-48c9-803a-22ac51fee24d"


class UploadProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


s3 = boto3.client('s3', config=Config(signature_version='s3v4'), verify=False)
s3.upload_file(
    Filename=S3Params.file_name,
    Bucket=S3Params.bucket_name,
    Key=S3Params.key,
    Callback=UploadProgressPercentage(S3Params.file_name),
    ExtraArgs={
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": S3Params.kms_key
    }
)
