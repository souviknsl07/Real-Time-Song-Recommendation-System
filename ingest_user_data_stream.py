from datetime import datetime
from faker import Faker
import random
import boto3
import json
import time
import pandas as pd
import io
import os

kinesis = boto3.client('kinesis')
stream_name = os.environ['stream_name']

s3 = boto3.client('s3')
fake = Faker()
bucket = os.environ['bucket']
key = os.environ['key']

def lambda_handler(event, context):
    #s3://souvik-spark-streaming/music_data/track_ids/track_ids.csv

    # Get object from S3
    response = s3.get_object(Bucket=bucket, Key=key)

    # Read CSV from S3 object body
    df = pd.read_csv(io.BytesIO(response['Body'].read()))
    track_ids = df['track_id'].tolist()

    def generate_user_data():
        return {
            'user_id': random.randint(0,5000),
            'user_name': fake.name(),
            'track_id': random.choice(track_ids),
            'like': random.randint(0,1),
            'timestamp': time.time()
        }




    for i in range(5):
        user_data = generate_user_data()
        response = kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps(user_data),
            PartitionKey=str(user_data['user_id'])
        )
        # print(user_data)
        # print(response)
        time.sleep(2)

    return {
        'statusCode': 200,
        'body': f"Record put to shard ID: {response['ShardId']}"
    }
