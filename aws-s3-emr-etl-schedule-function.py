import json
import boto3
import os

def lambda_handler(event, context):
    
    # Reading configurations from the environment variables
    AWS_ACCESS_KEY_ID = os.environ["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = os.environ["aws_secret_access_key"]
    REGION = os.environ["aws_region"]
    CODE_S3_PATH = os.environ["spark_code_s3_path"]
    
    # Creating an Emr client for creating a transient cluster and running the spark code
    emr_client = boto3.client("emr",region_name=REGION,aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    # Creating the spark submit command
    spark_submit = ['spark-submit',
                    '--deploy-mode','cluster',
                   CODE_S3_PATH]
    
    # Defining EMR cluster parameters
    emr_step_name = os.environ["emr_step_name"]
    ec2_subnet_id = os.environ["ec2_subnet_id"]
    emr_log_uri_loc = os.environ["log_uri_location"]
    bootstrap_script_loc = os.environ["bootstrap_script_s3_location"]
    emr_version = "emr-5.36.0"
    emr_cluster_name = "emr_json_parquet_etl_drift_cluster"
    hadoop_setup_step = "Setup hadoop debugging"
    
    
    # Creating and starting transient cluster along with running spark code via spark submit command
    try:
        print("Starting transient cluster and running the spark backend code")
        emr_client.run_job_flow(
            Name=emr_cluster_name,
            Instances={
            'InstanceGroups': [
            {
            'Name': "Master",
            'Market': 'ON_DEMAND',
            'InstanceRole': 'MASTER',
            'InstanceType': 'm5.xlarge',
            'InstanceCount': 1,
            },
            {
            'Name': "Slave",
            'Market': 'ON_DEMAND',
            'InstanceRole': 'CORE',
            'InstanceType': 'm5.xlarge',
            'InstanceCount': 2,
            }
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': ec2_subnet_id,
            },
            LogUri=emr_log_uri_loc,
            ReleaseLabel= emr_version,
            Steps=[{"Name":hadoop_setup_step,
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['state-pusher-script']
            }
            },
            {"Name": emr_step_name,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': spark_submit
            }
            }],
            BootstrapActions=[{'Name':"InstallDependencies",
                'ScriptBootstrapAction': {
                'Path': bootstrap_script_loc
                }
            }],
            VisibleToAllUsers=True,
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
            Applications = [ {'Name': 'Spark'},{'Name':'Hive'}])
        
    except Exception as error:
            print("Failed starting transient cluster and running the spark backend code")
            raise Exception("Error : {}".format(error))
                   
    
    
    