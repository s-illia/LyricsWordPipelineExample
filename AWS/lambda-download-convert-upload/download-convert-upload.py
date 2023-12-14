import boto3
import uuid
import json
from urllib.request import urlretrieve
import os
import awswrangler as wr
import pandas as pd
from zipfile import ZipFile

ssm_client = boto3.client("ssm")

class SourceFileError(Exception):
    def __init__(self, message="Source file is incorrect or damaged."):
        self.message = message
        super().__init__(self.message)


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
                # TODO error handling
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


def lambda_handler(event, context):
    # get config from SSM
    if "Debug" in event:
        config = event["Debug"]
        bucket = config["Bucket"]
    else:
        config = json.loads(
            ssm_client.get_parameter(Name="/get-convert-raw-data/lambda-config")[
                "Parameter"
            ]["Value"]
        )
        bucket_param = ssm_client.get_parameter(Name="/get-convert-raw-data/lyrics-word-bucket/name")['Parameter']['Value']
        bucket = f"s3://{bucket_param}/csv"
    sources = config["sources"]
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
        df = object
        if source["format"] == "csv":
            df = pd.read_csv(
                convert_file_name,
                sep=source["sep"],
                skiprows=source["skiprows"],
                header=None if source["header"] == "None" else source["header"],
                names=source["column_names"],
            )
        else:
            print(
                f"Unsupported file format in {convert_file_name}, skipping source {source_id}"
            )
            continue
        #use wrangler to save dataframe directly to s3
        wr.s3.to_csv(df, f"{bucket}/{source_id}.csv", index=False)
        print(f"Successfully uploaded the file {source_id}.csv to {bucket}")
    print(f"Finished")
    return True


if __name__ == "__main__":
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
                "name": "unstemmed_mapping.txt",
                "url": "http://millionsongdataset.com/sites/default/files/mxm_reverse_mapping.txt",
                "format": "csv",
                "sep": "<SEP>",
                "skiprows": 0,
                "header": None,
                "column_names": ["stemmed", "unstemmed"],
            },
            {
                "name": "id-track-artist.txt",
                "url": "http://millionsongdataset.com/sites/default/files/AdditionalFiles/mxm_779k_matches.txt.zip",
                "format": "csv",
                "sep": "<SEP>",
                "skiprows": 18,
                "header": None,
                "column_names": ["msd_track_id","msd_artist_name","msd_title","mxm_track_id","mxm_artist_name","mxm_title"],
            },
        ],
        "bucket": "s3://lyrics-word-dashboard-raw/csv",
        "download_path": "C:/tmp",
    }

    event = {"Debug": config}
    lambda_handler(event, {})
