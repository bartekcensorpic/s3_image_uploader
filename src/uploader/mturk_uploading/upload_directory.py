import logging
import boto3
import os
from botocore.exceptions import ClientError
import pandas as pd
from pathlib import Path
from datetime import datetime

def prepare_new_name(full_path):
    image_folder, image_name =  os.path.split(full_path)
    old_name, extention = os.path.splitext(image_name)
    letter_to_remove = r"._-()[]/\\,"
    new_name = old_name
    for char in letter_to_remove:
        new_name = new_name.replace(char,"")

    return new_name + extention


def process_folder(input_path: str,
                   bucket_name: str,
                   aws_access_key_id:str,
                   aws_secret_access_key:str,
                   region_name:str):
    """
    1. check all .jpg files in the folder
    2. create their list to csv file including the folder (batch_DATE), compatible with Amazon mechanical turk
    3. create metadata file of: full path to old_image_name, new_image_name with folder (compatible with s3),

    :param input_path:
    :param bucket_name:
    :param aws_access_key_id:
    :param aws_secret_access_key:
    :param region_name:
    :return:
    """
    now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    s3_bucket = now
    list_of_images = list(Path(input_path).glob(('*.jpg')))
    df = pd.DataFrame(data={'old_path':list_of_images})
    df['new_name'] = df['old_path'].map(lambda path: s3_bucket+ r'/' + prepare_new_name(path))
    df['image_url'] = df['new_name'].map(lambda path: os.path.splitext(path)[0])

    df.to_csv(os.path.join(input_path,"amt_metadata.csv"), encoding='utf-8', index=False)
    df.to_csv(os.path.join(input_path,"amt_filelist.csv"), encoding='utf-8', index=False, columns=['image_url'])

    upload_files(aws_access_key_id, aws_secret_access_key,region_name,bucket_name, df)

def upload_files(aws_key_id:str, aws_secret_access_key:str, aws_region:str, bucket_name:str, dataframe: pd.DataFrame):
    """Uploads folder to bucket, preserving its structure
    https://www.developerfiles.com/upload-files-to-s3-with-python-keeping-the-original-folder-structure/

    :param aws_key_id:
    :param aws_secret_access_key:
    :param aws_region:
    :param bucket_name:
    :param dataframe:
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

    for index, row in dataframe.iterrows():
        old_path = row['old_path']
        new_name = row['new_name']

        s3_file_name = new_name
        with open(old_path, "rb") as data:
            bucket.put_object(Key=s3_file_name, Body=data)
            print(f'Uploaded {s3_file_name}')
            upload_counter += 1



    print(f'Finished, uploaded {upload_counter} files')

