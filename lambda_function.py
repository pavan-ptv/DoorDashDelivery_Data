import json
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from io import StringIO

def lambda_handler(event, context):

    print("----*-S3 upload file event triggered-*----")

    #S3 file read
    s3_client = boto3.client('s3')
    bucket_list = s3_client.list_buckets()

    Records = event['Records']

    for record in Records:
        region_name = record['awsRegion']
        event_name = record['eventName']
        bucket_name = record['s3']['bucket']['name']
        bucket_arn = record['s3']['bucket']['arn']
        object_name = record['s3']['object']['key']
        object_size = record['s3']['object']['size']

        response = s3_client.get_object(
            Bucket = bucket_name,
            Key = object_name
            )
        file_content = response['Body'].read().decode('windows-1252')
        data = pd.read_csv(StringIO(file_content))
        print(data.head())
        
    return json.dumps({'Status':200,
                       'Response':'Function call executed'})
