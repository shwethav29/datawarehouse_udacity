import pandas as pd
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


#create iam role
def create_iam_role():
    iam = boto3.client('iam', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    try:
        print('1.1 Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allow Redshift Cluster to call AWS service on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [
                    {'Action': 'sts:AssumeRole', 'Effect': 'Allow', 'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'}))


    except Exception as e:
        print(e)
    # Attach policy AmazonS3ReadOnlyAccess
    print('1.2 Attaching Policy')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")[
        'ResponseMetadata']['HTTPStatusCode']
    return iam

#Create Readshift cluster
def create_redshift_cluster(iam):
    redshift = boto3.client('redshift', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,

            # Roles (for s3 access)
            IamRoles=[roleArn]
        )
    except Exception as e:
        print(e)
    #myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    #endpoint = myClusterProps['Endpoint']['Address']
    #roleArn = myClusterProps['IamRoles'][0]['IamRoleArn']
    #print("DWH_ENDPOINT :: ", endpoint)
    #print("DWH_ROLE_ARN :: ", roleArn)

def main():
    iam = create_iam_role()
    create_redshift_cluster(iam)


if __name__ == "__main__":
    main()