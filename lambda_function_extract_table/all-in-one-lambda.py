import boto3
import pandas
import sqlite3
import uuid
from urllib.parse import unquote_plus
from urllib.request import urlretrieve
import os
from zipfile import ZipFile


s3_client = boto3.client("s3")
ssm_client = boto3.client("ssm")


class SourceFileError(Exception):
    def __init__(self, message="Source file is incorrect or damaged."):
        self.message = message
        super().__init__(self.message)


def extract_to_parquet(database_path, query):
    # Create a database connection to the SQLite database
    with sqlite3.connect(database_path) as conn:
        # Create a pandas dataframe with required columns
        df = pandas.read_sql(query, conn)
        # Write the dataframe as a parquet file
        df.to_parquet(f"{database_path}.parquet", index=False)
    print(f"Successfully extracted from DB {database_path} to Parquet")
    return f"{database_path}.parquet"


def convert_tsv_to_parquet(file_path, skiprows, column_names, separator, header=None):
    # Create a pandas dataframe
    df = pandas.read_csv(
        file_path, sep=separator, skiprows=skiprows, header=header, names=column_names
    )
    # Write the dataframe as a parquet file
    df.to_parquet(f"{file_path}.parquet", index=False)
    print(f"Successfully formatted file {file_path} to Parquet")
    return f"{file_path}.parquet"


def download_and_unzip(url, download_path=".", file_name=uuid.uuid4()):
    # Download the file using urllib.request.urlretrieve
    file_path, headers = urlretrieve(url, f"{download_path}/{file_name}")
    # Check if the file is a ZIP file
    if ("Content-Type", "application/zip") in headers._headers:
        # Create a ZipFile object and extract contents
        with ZipFile(file_path, "r") as zip_file:
            # Get a list of archive members
            zip_contents = zip_file.filelist
            # we are expecting only one file in the archive
            if len(zip_contents) == 1:
                extracted_file = zip_contents[0].filename
                zip_file.extract(extracted_file, download_path)
            else:
                raise SourceFileError
        # Remove downloaded archive
        os.remove(file_path)
        # Rename extracted file to file_name param
        os.rename(f"{download_path}/{extracted_file}", f"{download_path}/{file_name}")
        print(
            f"Successfully downloaded and unzipped the file {file_name} to {download_path}/"
        )
    else:
        print(f"Successfully downloaded the file {file_name} to {download_path}/")
    return f"{download_path}/{file_name}"



'''    
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        tmpkey = key.replace("/", "")
        download_path = "/tmp/{}{}".format(uuid.uuid4(), tmpkey)
        upload_path = "/tmp/converted-{}.parquet".format(tmpkey)
        s3_client.download_file(bucket, key, download_path)
        extract_to_parquet(download_path, upload_path)
        s3_client.upload_file(
            upload_path, "{}-converted".format(bucket), "{}.parquet".format(key)
        )
'''
config = {
    "sources": [
        {
            "name": "genres.tsv",
            "url": "https://www.tagtraum.com/genres/msd_tagtraum_cd2c.cls.zip",
            "format": "csv",
            "sep": "\t",
            "skiprows": 7,
            "header": None,
            "column_names": ["track_id", "genre"],
        },
        {
            "name": "lyrycs.db",
            "url": "http://millionsongdataset.com/sites/default/files/AdditionalFiles/mxm_dataset.db",
            "format": "db",
            "query": "select track_id, word, count from lyrics",
        },
        {
            "name": "unstemmed_mapping.txt",
            "url": "http://millionsongdataset.com/sites/default/files/mxm_reverse_mapping.txt",
            "format": "csv",
            "sep": "<SEP>",
            "skiprows": 0,
            "header": None,
            "column_names": ["stemmed", "unstemmed"],
        },
    ],
    "bucket": "ARN-of-the-bucket",
    "download_path": "C:/tmp",
}


def lambda_handler(event, context):
   # get config from SSM
   #config = ssm_client.get_parameter(Name="raw-files-download-and-convert")["Parameter"]["Value"]
   sources = config["sources"]
   bucket = config["bucket"]
   download_path = config["download_path"]

   processed_raw_files = {}

   # process sources info
   for source in sources:
      source_id = source["name"]
      if source_id in processed_raw_files.keys():
         print(f"Duplicate source key definition {source_id}, skipping")
         continue
      # download raw from web
      convert_file_name = download_and_unzip(source["url"], download_path, source_id)
      # pre-process conversion
      upload_file_name = ""
      if source["format"] == "db":
         upload_file_name = extract_to_parquet(
               database_path=convert_file_name,
               query=source["query"],
         )
      elif source["format"] == "csv":
         upload_file_name = convert_tsv_to_parquet(
               file_path=convert_file_name,
               skiprows=source["skiprows"],
               header=source["header"],
               column_names=source["column_names"],
               separator=source["sep"],
         )
      else:
         print(
               f"Unsupported file format in {convert_file_name}, skipping source {source_id}"
         )
         continue
      processed_raw_files[source_id] = convert_file_name

   # upload processed Parquet files to S3
   # for source_key, file in processed_raw_files.items:
   #     s3_client.upload_file(file, bucket, source_key)
   #     print(f"Successfully uploaded {source_key} to bucket {bucket}")

