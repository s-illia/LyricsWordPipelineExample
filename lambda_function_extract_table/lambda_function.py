import boto3
import pandas
import sqlite3
from sqlite3 import Error
import uuid
from urllib.parse import unquote_plus
            
s3_client = boto3.client('s3')

def convert_to_parquet(database_path,converted_path):
    #create a database connection to the SQLite database
    with sqlite3.connect(database_path) as conn:
        #create a pandas dataframe with required columns
        df = pandas.read_sql('select track_id, word, count from lyrics',conn)
        #write the dataframe as a parquet file
        df.to_parquet(converted_path, index=False)
            
def lambda_handler(event, context):
  for record in event['Records']:
    bucket = record['s3']['bucket']['name']
    key = unquote_plus(record['s3']['object']['key'])
    tmpkey = key.replace('/', '')
    download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
    upload_path = '/tmp/converted-{}.parquet'.format(tmpkey)
    s3_client.download_file(bucket, key, download_path)
    convert_to_parquet(download_path, upload_path)
    s3_client.upload_file(upload_path, '{}-converted'.format(bucket), '{}.parquet'.format(key))
