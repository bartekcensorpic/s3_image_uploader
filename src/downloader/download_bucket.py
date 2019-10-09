import os
import boto3

def download_bucket(output_path, bucket_name, aws_access_key_id, aws_secret_access_key, region_name):
    s3_client = boto3.client('s3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )

    download_dir('', output_path, bucket_name, s3_client)


def download_dir(prefix, local, bucket, s3_client):
    """
    https://stackoverflow.com/questions/31918960/boto3-to-download-all-files-from-a-s3-bucket
    params:
    - prefix: pattern to match in s3
    - local: local path to folder in which to place files
    - bucket: s3 bucket with target contents
    - client: initialized s3 client object
    """
    downloaded_counter = 0
    skipped_counter = 0
    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
        'Bucket':bucket,
        'Prefix':prefix,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = s3_client.list_objects_v2(**kwargs)
        contents = results.get('Contents')
        for i in contents:
            k = i.get('Key')
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get('NextContinuationToken')
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    for k in keys:
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))

        #check if image exists on disk, if exists then do not download
        splitted_path = os.path.split(k)
        first_element = splitted_path[0].lower()
        does_exists = os.path.exists(dest_pathname)

        if not (first_element == 'images' and does_exists):
            s3_client.download_file(bucket, k, dest_pathname)
            print(f'Downloaded: {k}')
            downloaded_counter +=1
        else:
            print('Skipped', k)
            skipped_counter +=1

    print(f"Finished, downloaded {downloaded_counter} files")
    print(f"Skipped {skipped_counter} files")