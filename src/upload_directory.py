import logging
import boto3
import os
from botocore.exceptions import ClientError


def process_folder(input_path: str,
                   bucket_name: str,
                   aws_access_key_id:str,
                   aws_secret_access_key:str,
                   region_name:str):

    upload_files(input_path,aws_access_key_id, aws_secret_access_key,region_name,bucket_name)


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_files(path:str, aws_key_id:str, aws_secret_access_key:str, aws_region:str, bucket_name:str):
    """Uploads folder to bucket, preserving its structure
    https://www.developerfiles.com/upload-files-to-s3-with-python-keeping-the-original-folder-structure/

    :param path: path of folder to be uploaded
    :param aws_key_id: aws key id
    :param aws_secret_access_key: aws key value
    :param aws_region: aws region
    :param bucket_name: bucket name
    :return:
    """

    session = boto3.Session(
        aws_access_key_id=aws_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region,
    )
    s3 = session.resource("s3")
    bucket = s3.Bucket(bucket_name)

    for subdir, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, "rb") as data:
                s3_file_name = full_path[len(path) + 1 :].replace('\\', '/')
                bucket.put_object(Key=s3_file_name, Body=data)
