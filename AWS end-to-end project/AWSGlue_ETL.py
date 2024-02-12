import time
import boto3
import pandas as pd
from io import StringIO

AWS_ACCESS_KEY="********"
AWS_SECRET_KEY = "*********"
AWS_REGION = "au-south-1"
SCHEMA_NAME = "coid19db"
S3_STAGING_DIR = "s3://vaibhavk-result-dataset-bucket/output/"
S3_BUCKET_NAME = "vaibhavk-result-dataset-bucket"
S3_OUTPUT_DIRECTORY = 'output'

athena_client = boto3.client("athena",
                     region_name=AWS_REGION,
                     aws_access_key_id=AWS_ACCESS_KEY,
                     aws_secret_access_key=AWS_SECRET_KEY)

Dict = {}
def download_and_load_query_result(client:boto3.client, query_response:Dict) -> pd.DataFrame:
    while True:
        try:
            client.get_query_result(QueryExecutionID=query_response["QueryExecutionID"])
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.001)
            else:
                raise err

    temp_file_location: str = "athena_query_result.csv"
    s3_client = boto3.client("s3",
                     region_name= AWS_REGION,
                     aws_access_key_id=AWS_ACCESS_KEY,
                     aws_secret_access_key=AWS_SECRET_KEY)

    s3_client.download_file(S3_BUCKET_NAME, f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionID']}.csv", temp_file_location,)
    return pd.read_csv(temp_file_location)


response = athena_client.start_query_execution(
    QueryString = "select * from enigma_jhud",
    QueryExecutionContext={"Database":SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption" : "SSE_S3"},
    },
)



#print(response)
df_data = download_and_load_query_result(athena_client, response)
