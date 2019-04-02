# -*- encoding:utf8 -*-
"""
    author: quanbin_zhu
    time  : 2017/11/13 15:19
"""

import re
import boto3

class EmrClient(object):

    def __init__(self):
        self.emr_version = "emr-5.9.0"
        self.emr_log_url = "s3://dev-xxx/borey-zhu/sparklog"
        self.aws_ec2_key = "borey.zhu"
        self.aws_access_key = "xxxxxxxxxxxxxxxxxxxxxxxx"
        self.aws_secret_key = "xxxxxxxxxxxxxxxxxxxxxxxx+xxxxxxxxxx"
        self.client = boto3.client('emr', region_name='cn-north-1', aws_access_key_id=self.aws_access_key,
                                   aws_secret_access_key=self.aws_secret_key)
        self.job_flow_id = None

    def generate_step(self, step_name, step_command):
        cmds = re.split('\\s+', step_command)
        if not cmds:
            raise ValueError
        return {
            'Name': step_name,
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': cmds
            }
        }

    def add_job_flow_steps(self, steps, flow_id=None):
        if flow_id:
            return self.client.add_job_flow_steps(JobFlowId=flow_id, Steps=steps)

        elif self.job_flow_id:
            return self.client.add_job_flow_steps(JobFlowId=self.job_flow_id, Steps=steps)

        else:
            return self.run_job_flow(steps)

    def run_job_flow(self, steps=None):
        response = self.client.run_job_flow(
            # 集群名称
            Name="SnifferAnalyse",
            # 集群版本号 http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-components.html
            ReleaseLabel=self.emr_version,
            # 集群日志存储Amazon S3的路径
            LogUri=self.emr_log_url,
            Instances={
                # 实例类型
                # https://amazonaws-china.com/cn/ec2/purchasing-options/dedicated-instances/
                'MasterInstanceType': 'm3.xlarge',
                'SlaveInstanceType':  'm3.xlarge',

                # 实例数
                'InstanceCount': 9,

                # 保持EMR集群运行当没有步骤时
                # True  - 切换为 WAITING
                # False - 关闭集群
                'KeepJobFlowAliveWhenNoSteps': False,

                # 终止保护
                # True  - 锁定集群和防止被API调用终止,用户干预,或在工作时流错误
                'TerminationProtected': False,

                # 使用现有 EC2 密钥对以 SSH 方式连接到 Amazon EMR 集群的主节点
                'Ec2KeyName': self.aws_ec2_key,

                # 指定子网 ID,  集群虚拟私有云 Amazon Virtual Private Cloud (亚马逊 VPC)
                'Ec2SubnetId': 'subnet-xxxxxxx'
            },
            Applications=[
                {
                    'Name': 'Hadoop'
                },
                {
                    'Name': 'Spark'
                },
            ],
            BootstrapActions=[
                {
                    # YARN 资源最大化配置 for Spark
                    'Name': 'Spark Default Config',
                    'ScriptBootstrapAction': {
                        # Initialization shell for spark, you will find the doc in
                        # https://github.com/awslabs/emr-bootstrap-actions/tree/master/spark
                        'Path': 's3://support.elasticmapreduce/spark/maximize-spark-default-config',
                    }
                },
            ],
            Steps=steps,
            # 对自己账号可见
            VisibleToAllUsers=False,
            # EC2 实例配置
            JobFlowRole='EMR_EC2_DefaultRole',
            # EMR 角色
            ServiceRole='EMR_DefaultRole'
        )
        self.job_flow_id = response['JobFlowId']

        return response

    def describe_cluster(self, jobFlowId=None):
        job_id = jobFlowId if jobFlowId else self.job_flow_id
        if job_id:
            return self.client.describe_cluster(
                ClusterId=jobFlowId
            )

if __name__ == "__main__":

    emr = EmrClient()

    steps = []

    steps.append(
        emr.generate_step("Load spark project from S3", "hadoop fs -get s3://dev-xxx/borey-zhu/spark/example.jar /home/hadoop/spark-example.jar")
    )

    spark_submit_cmd = """
            spark-submit --master yarn  
                --packages org.apache.hbase:hbase-hadoop-compat:1.3.0,org.apache.hbase:hbase-server:1.3.0,org.apache.hbase:hbase-common:1.3.0,org.apache.hbase:hbase-client:1.3.0 --repositories http://maven.aliyun.com/nexus/content/groups/public/ 
                --driver-memory 2G 
                --executor-cores 2 
                --num-executors 32 
                --executor-memory 2200M 
                --conf spark.memory.fraction=0.75
                --conf spark.memory.storageFraction=0.1
                --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
                --conf spark.driver.extraJavaOptions=-XX:+UseCompressedOops
                --conf spark.executor.extraJavaOptions=-XX:+UseCompressedOops
                --conf mapreduce.input.fileinputformat.list-status.num-threads=2
                --class com.borey.spark.SparkTest
                /home/hadoop/spark-example.jar
"""

    steps.append(
        emr.generate_step("Spark Example", spark_submit_cmd),
    )


    response = emr.add_job_flow_steps(steps)
    print (response)