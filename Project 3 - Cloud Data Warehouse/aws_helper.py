import base64
import configparser
import boto3
from cryptography.fernet import Fernet
import json
import psycopg2


def read_config_file(filename):
    """
    Function to read config file and parse it
    @param filename: name of the config file
    return: configParser object
    """
    config = configparser.ConfigParser()
    config.read(filename)
    return config


def add_attr_to_config(config, section, attr, value):
    """
    Function to add or replace attribute to the config file
    @param config: configParser object
    @param section: section name of in the config file
    @param attr: attribute name that needs to be added/replaced
    @param value: the value that needs to be assigned to the attribute
    return: void
    """
    cluster_info = config[section]
    cluster_info[attr] = value
    with open('dwh.cfg', 'w') as conf:
        config.write(conf)
    print("Attribute {0} with value: {1} has been add to Section {2} to the config file Successfully!"
         .format(attr, value, section))


def create_aws_client(service, KEY, SECRET, REGION):
    """
    Function to create AWS client for any service
    @param service: service name
    @param KEY: AWS account key
    @param SECRET: AWS account secret
    @param REGION: the aws region
    return: aws client object
    """
    client = boto3.client(service,
                          region_name=REGION,
                          aws_access_key_id=KEY,
                          aws_secret_access_key=SECRET)
    print("AWS Client {0} has been created on region {1} successfully!".format(service, REGION))
    return client


def create_aws_resource(service, KEY, SECRET, REGION):
    """
    Function to create AWS resource for any service
    @param service: service name
    @param KEY: AWS account key
    @param SECRET: AWS account secret
    @param REGION: the aws region
    return: aws resource object
    """
    resource = boto3.resource(service,
                              region_name=REGION,
                              aws_access_key_id=KEY,
                              aws_secret_access_key=SECRET)
    print("AWS Resource {0} has been created on region {1} successfully!".format(service, REGION))
    return resource
    

def get_list_of_files_from_s3(s3_client, bucket, prefix):
    """
    Function to get the whole list of files in a AWS S3 Bucket.
    The 'get_paginator()' is used here to get all files even if they are greater than 1000.
    @param s3_client: S3 client object
    @param bucket: S3 bucket name
    @param prefix: the prefix to filter files
    return: list of files paths on the bucket
    """
    s3_files_list = []
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    for page in pages:
        for obj in page['Contents']:
            s3_files_list.append(obj.get('Key'))
    
    print("Length of the files in the {0} Bucket with Prefix {1} is: {2}".format(bucket, prefix, len(s3_files_list)))
    return s3_files_list


def download_file_from_s3(s3_client, bucket, object_name):
    """
    Function to download file from s3 bucket
    @param s3_client: S3 client object
    @param bucket: S3 bucket name
    @param object_name: the file path
    return: void
    """
    s3_client.download_file(bucket, object_name, object_name.split('/')[-1])
    print("Object {0} has been downloaded successfully!".format(object_name))


def decode_credential(secret, enc_key):
    """
    Function to decrypt the encrypted secret & key of aws credentials
    @param secret: the encrypted text as a binary string
    @param enc_key: the encryption key as a binary string
    return: decrypted text as a string
    """
    fernet = Fernet(enc_key)
    return fernet.decrypt(secret).decode()


def create_iam_role(iam_obj, role_name):
    """
    Function to create IAM role on aws
    @param iam_obj: AWS iam service object
    @param role_name: the iam role name
    return: void
    """
    try:
        iam_role = iam_obj.create_role(
            Path='/',
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'}),
            Description='Allows Redshift clusters to call AWS services on your behalf.',
        )
        print("Role {0} hass been created successfully!".format(role_name))
    except Exception as err:
        print(err)


def attach_policy_to_role(iam_obj, role_name, policy_arn):
    """
    Function to attach a policy to a role
    @param iam_obj: AWS IAM object
    @param role_name: the name of the IAM role
    @param policy_arn: the ARN of the policy the would be attached to the Role
    return: void
    """
    try:
        iam_obj.attach_role_policy(RoleName=role_name,
                                   PolicyArn=policy_arn
                                  )['ResponseMetadata']['HTTPStatusCode']
        print("Policy {0} has been attached to Role {1} successfully!".format(policy_arn, role_name))
    except Exception as err:
        print(err)


def get_aws_role(iam_obj, role_name):
    """
    Function to get the created AWS Role
    @param iam_obj: AWS iam service object
    @param role_name: the name of the IAM role
    return: the ARN of the IAM Role
    """
    return iam_obj.get_role(RoleName=role_name)['Role']['Arn']


def create_redshift_cluster(redshift, config):
    """
    Function to create a Redshift cluster
    @param redshift: redshift client object
    @param config: configParser object
    return: void
    """
    try:
        redshift.create_cluster(        
            #Cluster Info
            ClusterType=config.get('CLUSTER', 'CLUSTER_TYPE'),
            NodeType=config.get('CLUSTER', 'NODE_TYPE'),
            NumberOfNodes=int(config.get('CLUSTER', 'NUM_NODES')),

            #Identifiers & Credentials
            DBName=config.get('CLUSTER', 'DB_NAME'),
            ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER'),
            MasterUsername=config.get('CLUSTER', 'DB_USER'),
            MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD'),

            #Roles (for s3 access)
            IamRoles=[config.get('IAM_ROLE', 'ROLE_ARN')]  
         
        )
        print("Redshift Cluster has been created successfully!")
    except Exception as err:
        print(err)
    

def get_cluster_info(redshift, config):
    """
    Function to get the redshift cluster information
    @param redshift: redshift client object
    @param config: the configParser object
    return: cluster info json object
    """
    cluster_info = redshift.describe_clusters(ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER'))['Clusters'][0]
    
    print(cluster_info)
    print("DWH_ENDPOINT :: ", cluster_info['Endpoint']['Address'])
    print("DWH_ROLE_ARN :: ", cluster_info['IamRoles'][0]['IamRoleArn'])
    return cluster_info
    

def open_cluster_tcp_port(ec2, cluster_info, config):
    """
    Function to open a TCP port for the redshift cluster
    @param: EC2 resource object
    @param cluster_info: cluster information json object
    @param config: configParser object
    return: void
    """
    try:
        vpc = ec2.Vpc(id=cluster_info['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name ,  
            CidrIp='0.0.0.0/0',  
            IpProtocol='TCP',  
            FromPort=int(config.get('CLUSTER', 'DB_PORT')),
            ToPort=int(config.get('CLUSTER', 'DB_PORT'))
        )
        print("TCP Port has been opened for the Redshift Cluster!")
    except Exception as err:
        print(err)

        
def create_redshift_conn(config):
    """
    Function to create the connection to the redshift cluster
    @param config: configParser object
    return: connection object
    """
    try:
        conn = psycopg2.connect("host={0} dbname={1} user={2} password={3} port={4}"
                                .format(config.get('CLUSTER', 'HOST'),
                                        config.get('CLUSTER', 'DB_NAME'),
                                        config.get('CLUSTER', 'DB_USER'),
                                        config.get('CLUSTER', 'DB_PASSWORD'),
                                        config.get('CLUSTER', 'DB_PORT')))
        print("Connection with redshift has been created successfully!")
        return conn
    except Exception as err:
        print(err)
        

def delete_redshift_cluster(redshift, config):
    """
    Function to delete the redshift cluster
    @param redshift: redshift client object
    @param config: configParaser object
    return: void
    """
    try:
        redshift.delete_cluster(ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER'),
                                       SkipFinalClusterSnapshot=True)
        print("Redshift Cluster has been deleted successfully!")
    except Exception as err:
        print(err)   
        
        