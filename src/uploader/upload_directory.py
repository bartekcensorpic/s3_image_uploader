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
    upload_counter = 0
    skipped_counter = 0

    for subdir, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(subdir, file)
            s3_file_name = full_path[len(path) + 1:].replace('\\', '/')

            #check if file exists on bucket if it goes to "image" folder
            key = s3_file_name
            objs = list(bucket.objects.filter(Prefix=key))
            first_path_element = os.path.split(key)[0]
            if len(objs) > 0 and objs[0].key == key and first_path_element == 'images':
                print(f"File {s3_file_name} exists on bucket, skipping")
                skipped_counter+=1
            else:
                #uploading to bucket
                with open(full_path, "rb") as data:
                    bucket.put_object(Key=s3_file_name, Body=data)
                    print(f'Uploaded {s3_file_name}')
                    upload_counter += 1


    print(f'Finished, uploaded {upload_counter} files')
    print(f'Skipped {skipped_counter} files')
