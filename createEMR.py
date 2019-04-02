import boto3
import shlex


class EMR(object):

    @staticmethod
    def __init__(self):
        self.name = "test-emr"
        self.emr_version = "emr-5.21.0"
        self.emr_log_url = "s3://ecomdatascience-logs-np/emr/"
        self.aws_ec2_key = None
        self.aws_access_key = None
        self.aws_secret_key = None
        self.client = boto3.client(
            'emr',
            region_name='us-east-1',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key
        )
        self.job_flow_id = None
        self.subnet_id = "subnet-f0d61bb9"
        self.spark_submit = """
            spark-submit
            --class NeuralEmbedding
            s3://ecomdatascience-np/lu/neuralEmbedding.jar
        """


connection = EMR().client

cluster_id = connection.run_job_flow(
    Name=EMR().name,
    LogUri=EMR().emr_log_url,
    ReleaseLabel=EMR().emr_version,
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
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-f0d61bb9',
    },
    Steps=[
        {
            'Name': 'spark',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': shlex.split(cmd)
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
    ],
    Configurations=[
        {
            "Classification": "emrfs-site",
            "Properties": {
                "fs.s3.serverSideEncryption.kms.keyId": "arn:aws:kms:us-east-1:729964090428:key/3a67dc71-c759-48c9-803a-22ac51fee24d",
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
