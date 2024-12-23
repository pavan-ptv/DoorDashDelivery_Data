import json
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from io import StringIO
import io
import os
from datetime import date,datetime


def lambda_handler(event, context):

    print("EVENT RECEIVED: ",event)
    print("----*-S3 upload file event triggered-*----")

    #S3 file read
    s3_client = boto3.client('s3')
    sns_client = boto3.client('sns')
    bucket_list = s3_client.list_buckets()

    record = event['Records'][0]
    region_name = record['awsRegion']
    event_name = record['eventName']
    bucket_name = record['s3']['bucket']['name']
    bucket_arn = record['s3']['bucket']['arn']
    object_name = record['s3']['object']['key']
    object_size = record['s3']['object']['size']
    print(bucket_name)
    print(object_name)
    response = s3_client.get_object(
        Bucket = bucket_name,
        Key = object_name
        )
    file_content = response['Body'].read().decode('windows-1252')
    data = pd.read_csv(StringIO(file_content))
    print(data.head())
    country_list = list(data['Country_Name'].unique())
    for country in country_list:
        df = data[data['Country_Name'] == country]
        df.to_csv(f'/tmp/{country}_df.csv',index = False)

        for file in os.listdir('/tmp/'):
            if os.path.isfile(os.path.join('/tmp/',file)) and os.path.splitext(file)[1] == '.csv':
                with open(os.path.join('/tmp/',file),'rb') as file_object:
                    s3_client.upload_fileobj(
                        Fileobj = file_object,
                        Bucket = bucket_name,
                        Key = f"output/{file}"
                    )
    list_response = s3_client.list_objects(
        Bucket = bucket_name,
        Prefix = 'output/'
    )
    contents = list_response['Contents']
    today = datetime.strftime(datetime.today(),'%Y%m%d')
    files_modified_today = 0
    for content in contents[1:]:
        if datetime.strftime(content['LastModified'],'%Y%m%d') == today:
            files_modified_today += 1
    if files_modified_today == len(country_list):
        sns_client.publish(TopicArn = 'arn:aws:sns:us-east-1:891612543241:DoorDashDelivery',
                        Message = f'DoorDashDelivery data is processed. Number of files Created {len(country_list)}.',
                        Subject = "Delivery Data Processed")
    else:
        sns_client.publish(TopicArn = 'arn:aws:sns:us-east-1:891612543241:DoorDashDelivery',
                        Message = f'DoorDashDelivery data is processed Partially . Number of files Created {files_modified_today}.',
                        Subject = "Delivery Data Processed")
    return {
        'statusCode' : 200,
        'body' : json.dumps({'event type':'data flow completed'})
    }
# event = {"Records": [{"eventVersion": "2.0","eventSource": "aws:s3","awsRegion": "us-east-1","eventTime": "1970-01-01T00:00:00.000Z","eventName": "ObjectCreated:Put","userIdentity": {"principalId": "EXAMPLE"},"requestParameters": {"sourceIPAddress": "127.0.0.1"},"responseElements": {"x-amz-request-id": "EXAMPLE123456789","x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"},"s3": {"s3SchemaVersion": "1.0","configurationId": "testConfigRule","bucket": {"name": "lambda-notify-bucket-1306","ownerIdentity": {"principalId": "EXAMPLE"},"arn": "arn:aws:s3:::example-bucket"},"object": {"key": "input/Player.csv","size": 1024,"eTag": "0123456789abcdef0123456789abcdef","sequencer": "0A1B2C3D4E5F678901"}}}]}
# lambda_handler(event,context = 'b')