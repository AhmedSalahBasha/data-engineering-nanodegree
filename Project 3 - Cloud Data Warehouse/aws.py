import base64
import configparser
import boto3
from cryptography.fernet import Fernet
import json
import psycopg2
import aws_helper as helper
import time


# the following AWS KEY & SECRET are encrypted
AWS_CREDENTIALS = {
    'AWS_KEY': b'gAAAAABi5Hx77lADGiO9Bw1I2ycPPnFnXCeKDRP-6uS8mf7Zh3UCSUBj5eTQCzgA0blnx4aAurWv32y--NzAzgDd60PgC0viL2Ml1AKKHinjc7iOGPwXLhw=',
    'AWS_SECRET': b'gAAAAABi5Hyas-Z5bzrqvG_GxGGYQAKK5KFFSUxaUNORWEGY2aPU3TNbOWe32pt9UQuZi0FYfw_3nmbgLUxzrONxAqz6QoyIboS6gK0_ePle6qgZGTup3yDQeUgXD7VzzF9eQNszW8Yq',
    'ENC_KEY': b'grzUi5AwQIw5wqGJGSh_WCElek9Yr5cSZgCNizfXY0A='
}

def create_aws_infrastructure():
    """
    This function is creating the whole AWS infrastructure services 
    that are required to satisfy all the requirements of this project
    """
    config = helper.read_config_file('dwh.cfg')
    

    S3_REGION = config.get('S3', 'REGION')
    S3_BUCKET = config.get('S3', 'BUCKET')
    SONG_DATA = config.get('S3', 'SONG_DATA')
    LOG_DATA = config.get('S3', 'LOG_DATA')
    IAM_ROLE_NAME = config.get('IAM_ROLE', 'ROLE_NAME')
    S3_POLICY_ARN = config.get('S3', 'POLICY_ARN')

    AWS_KEY = helper.decode_credential(AWS_CREDENTIALS['AWS_KEY'], AWS_CREDENTIALS['ENC_KEY'])
    AWS_SECRET = helper.decode_credential(AWS_CREDENTIALS['AWS_SECRET'], AWS_CREDENTIALS['ENC_KEY'])
    
    # get song data files & log data files from the s3 bucket
    s3_client = helper.create_aws_client('s3', AWS_KEY, AWS_SECRET, S3_REGION)
#     song_files_list = helper.get_list_of_files_from_s3(s3_client, S3_BUCKET, prefix=SONG_DATA)
#     log_files_list = helper.get_list_of_files_from_s3(s3_client, S3_BUCKET, prefix=LOG_DATA)
#     helper.download_file_from_s3(s3_client, S3_BUCKET, song_files_list[5])
#     helper.download_file_from_s3(s3_client, S3_BUCKET, log_files_list[5])

    
    # create iam role to access redshift
    iam_client = helper.create_aws_client('iam', AWS_KEY, AWS_SECRET, S3_REGION)
    helper.create_iam_role(iam_client, IAM_ROLE_NAME)
    
    # attach policy to read from s3
    helper.attach_policy_to_role(iam_client, IAM_ROLE_NAME, S3_POLICY_ARN)
    
    s3_role = helper.get_aws_role(iam_client, IAM_ROLE_NAME)
    helper.add_attr_to_config(config, "IAM_ROLE", "ROLE_ARN", s3_role)
    
    # create reshift cluster
    redshift_client = helper.create_aws_client('redshift', AWS_KEY, AWS_SECRET, S3_REGION)
    ec2_resource = helper.create_aws_resource('ec2', AWS_KEY, AWS_SECRET, S3_REGION)
    
    helper.create_redshift_cluster(redshift_client, config)
    print("sleeping for 60 seconds waiting for the cluster to be created...")
    time.sleep(60)  # sleep a bit for the cluster to be created
    cluster_info = helper.get_cluster_info(redshift_client, config)
    CLUSTER_HOST = cluster_info['Endpoint']['Address']
    helper.add_attr_to_config(config, "CLUSTER", "HOST", CLUSTER_HOST)
    helper.open_cluster_tcp_port(ec2_resource, cluster_info, config)
    

