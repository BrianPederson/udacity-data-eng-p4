# cluster_create.py 
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 03/20/2020
# PURPOSE: Script to set up Redshift cluster for Data Pipelines Project 4.

import boto3
import configparser

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS','KEY')
SECRET= config.get('AWS','SECRET')

DWH_DB_NAME = config.get("CLUSTER","DB_NAME")
DWH_DB_USER = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT = config.get("CLUSTER","DB_PORT")
DWH_CLUSTER_TYPE = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_ID = config.get("DWH","DWH_CLUSTER_ID") # redshift-cluster
#DWH_IAM_ROLE_NAME = config.get("DWH","DWH_IAM_ROLE_NAME")   # myRedshiftRole - not sure if necessary since I'm reusing this one

DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')  # reusing existing role so this is ok

print(f"DB_NAME: {DWH_DB_NAME}")
print(f"DB_USER: {DWH_DB_USER}")
print(f"DB_PASSWORD: {DWH_DB_PASSWORD}")
print(f"PORT: {DWH_PORT}")
print(f"CLUSTER_TYPE: {DWH_CLUSTER_TYPE}")
print(f"NUM_NODES: {DWH_NUM_NODES}")
print(f"NODE_TYPE: {DWH_NODE_TYPE}")
print(f"CLUSTER_ID: {DWH_CLUSTER_ID}")

# Set up access classes
ec2 = boto3.resource('ec2',
                      region_name="us-west-2",
                      aws_access_key_id=KEY,
                      aws_secret_access_key=SECRET)

s3 = boto3.resource('s3',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET)

iam = boto3.client('iam', region_name="us-west-2",
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET)

redshift = boto3.client('redshift', region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

# Create cluster for project
try:
    response = redshift.create_cluster(
                        ClusterType = DWH_CLUSTER_TYPE,
                        NodeType = DWH_NODE_TYPE,
                        NumberOfNodes = int(DWH_NUM_NODES),
                        DBName = DWH_DB_NAME,
                        ClusterIdentifier = DWH_CLUSTER_ID,
                        MasterUsername = DWH_DB_USER,
                        MasterUserPassword = DWH_DB_PASSWORD,
                        IamRoles = [DWH_ROLE_ARN]
                       )
except Exception as e:
    print(e)

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_ID)['Clusters'][0]
print('Cluster Status: ' + myClusterProps['ClusterStatus'])


########
## delete the created resources
#redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_ID,  SkipFinalClusterSnapshot=True)
