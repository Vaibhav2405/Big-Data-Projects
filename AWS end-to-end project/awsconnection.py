import boto3
import json
import pandas as pd
import psycopg2
import configparser

config = configparser.ConfigParser()
config.read_file(open('cluster.config'))

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

# print(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

pddataframe = pd.DataFrame(
    {"Param":["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
     "Value":[DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
     }
)

ec2 = boto3.resource('ec2',
                     region_name="eu-north-1",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)

s3 = boto3.resource('s3',
                     region_name="eu-north-1",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)


iam = boto3.client('iam',
                     region_name="global",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)

redshift = boto3.client('redshift',
                     region_name="us-east-1",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)

bucket=s3.Bucket("vaibhavk-test-bucket")
log_data_files = [filename.key for filename in bucket.objects.filter(Prefix='all')]

#roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
roleArn = ['arn:aws:iam::654654581874:role/redshift-s3-access']


# try:
#     responce = redshift.create_cluster(
#         ClusterType=DWH_CLUSTER_TYPE,
#         NodeType=DWH_NODE_TYPE,
#
#         DBName=DWH_DB,
#         ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
#         MasterUsername=DWH_DB_USER,
#         MasterUserPassword=DWH_DB_PASSWORD,
#
#         IamRoles=roleArn
#     )
# except Exception as e:
#     print(e)

print(redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0])

# fuction to display cluster info in data frame format
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ['ClusterIdentifier', 'NodeType', 'ClusterStatus', 'MasterUsername', 'DBName', 'EndPoint', 'VpacId']
    x = [(k,v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data = x, columns=['key', 'value'])

myClusterProps = redshift.describe_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Cluster'][0]
print(prettyRedshiftProps(myClusterProps))

# Assigning the value from cluster info to valiables to use it further
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
DB_NAME = myClusterProps['DBName']
DB_USER = myClusterProps['MasterUsername']

# We have created a EC2 instace to allow VPC to cluster
try:
    vpc = ec2.Vpc(id=myClusterProps['VpcID'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)

    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)

# Connecting to the database using psycopg2 library
try:
    conn = psycopg2.connect(host=DWH_ENDPOINT, dbname=DB_NAME, user=DB_USER, password="Passw0rd123", port=5439)
except psycopg2.Error as e:
    print(e)

conn.set_session(autocommit=True)

#Creating a curser to execute Query to database or Redshift
try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print(e)

# After this we can create tables by using SQL commands and cur.execute() method
try:
    cur.execute("""
     create table users(
     name varchar(10),
     add varchar(20),
     ....
     ...
     ..
     .
        """)
except psycopg2.Error as e:
    print(e)

# Now copying the textfile data from s3 bucket to redshift
try:
    cur.execute("""
    copy users from 's3://<bucket_name>/<file_name>'
    crendentials 'aws_iam_role=arn:aws:iam::<redshift_access_arn_url'
    delimiter '|'
    region 'eu-north-1'
    """)
except psycopg2.Error as e:
    print(e)

# Excuting the command to creted table
try:
    cur.execute("""
    select * from users
    """)
except psycopg2.Error as e:
    print(e)

# to display the records from the table
row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

# to close the connection
try:
    conn.close()
except psycopg2.Error as e:
    print(e)

redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)

