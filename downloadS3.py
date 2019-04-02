import boto3
from botocore.client import Config
from dataclasses import dataclass


@dataclass()
class S3Params(object):
    file_name: str = "/Users/lu-wang/Documents/GitHub/boto3EMR/createEMR2.py"
    bucket_name: str = "ecomdatascience-np"
    key: str = "lu/testUpload.py"


s3 = boto3.client('s3', config=Config(signature_version='s3v4'))
s3.download_file(
    Bucket=S3Params.bucket_name,
    Key=S3Params.key,
    Filename=S3Params.file_name
)
