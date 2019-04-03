import boto3
from botocore.client import Config
from dataclasses import dataclass


@dataclass()
class S3Params(object):
    file_name: str = "/Users/lu-wang/Documents/GitHub/boto3Script/createEMR.py"
    bucket_name: str = "ecomdatascience-np"
    key: str = "lu/testUpload.py"
    kms_key: str = "arn:aws:kms:us-east-1:729964090428:key/3a67dc71-c759-48c9-803a-22ac51fee24d"


s3 = boto3.client('s3', config=Config(signature_version='s3v4'))
s3.upload_file(
    Filename=S3Params.file_name,
    Bucket=S3Params.bucket_name,
    Key=S3Params.key,
    ExtraArgs={
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": S3Params.kms_key
    }
)
