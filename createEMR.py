import boto3
import shlex
from dataclasses import dataclass


@dataclass()
class EMRParams(object):
    name: str = "neuralEmbedding"
    emr_version: str = "emr-5.21.0"
    emr_log_url: str = "s3://ecomdatascience-logs-np/emr/"
    aws_ec2_key: str = None
    aws_secret_key: str = None
    kms_key: str = "arn:aws:kms:us-east-1:729964090428:key/3a67dc71-c759-48c9-803a-22ac51fee24d"
    region_name: str = "us-east-1"
    subnet_id: str = "subnet-f0d61bb9"
    spark_submit: str = """
            spark-submit
            --class NeuralEmbedding
            --conf spark.dynamicAllocation.enabled=false
            s3://ecomdatascience-np/lu/neuralEmbedding.jar
    """


connection = boto3.client(
    "emr",
    region_name=EMRParams.region_name,
    aws_access_key_id=EMRParams.aws_ec2_key,
    aws_secret_access_key=EMRParams.aws_secret_key
)


cluster_id = connection.run_job_flow(
    Name=EMRParams.name,
    LogUri=EMRParams.emr_log_url,
    ReleaseLabel=EMRParams.emr_version,
    Applications=[
        {
            'Name': 'Spark'
        },
    ],
    Instances={
        'InstanceGroups': [
            {
                'Name': "Master nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': "Slave nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.2xlarge',
                'InstanceCount': 4
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-f0d61bb9',
    },
    # BootstrapActions=[
    #     {
    #         'Name': 'Maximize Spark Default Config',
    #         'ScriptBootstrapAction': {
    #             'Path': 's3://support.elasticmapreduce/spark/maximize-spark-default-config',
    #         }
    #     },
    # ],
    Steps=[
        {
            'Name': 'spark',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': shlex.split(EMRParams.spark_submit)
            }
        }
    ],
    VisibleToAllUsers=True,
    JobFlowRole='EMR-EC2-Default-Role-np',
    ServiceRole='EMR-Default-Role-np',
    Tags=[
        {
            'Key': 'dept',
            'Value': 'digital-analytics',
        },
        {
            'Key': 'owner',
            'Value': 'lu.wang@officedepot.com',
        },
        {
            'Key': 'costcenter',
            'Value': '47002',
        },
        {
            'Key': 'appname',
            'Value': 'spark',
        },
    ],
    Configurations=[
        {
            "Classification": "emrfs-site",
            "Properties": {
                "fs.s3.serverSideEncryption.kms.keyId": EMRParams.kms_key,
                "fs.s3.enableServerSideEncryption": "true"
            }
        },
        {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            }
        }
    ]
)

print('cluster created with the step...', cluster_id['JobFlowId'])
