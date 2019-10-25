import boto3
import json
import configparser
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DB_NAME                 = config.get("CLUSTER","DB_NAME")
DB_USER            = config.get("CLUSTER","DB_USER")
DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DB_PORT               = config.get("CLUSTER","DB_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

iam = boto3.client('iam', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

redshift = boto3.client('redshift', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
endpoint = myClusterProps['Endpoint']['Address']
roleArn = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", endpoint)
print("DWH_ROLE_ARN :: ", roleArn)
