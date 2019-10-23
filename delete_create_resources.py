import boto3
import configparser
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))


KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

#### CAREFUL!!
#-- Uncomment & run to delete the created resources

iam = boto3.client('iam', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
#### CAREFUL!!