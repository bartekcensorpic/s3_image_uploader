import argparse
from src.uploader.mturk_uploading.upload_directory import process_folder

def init(args):
    input_path = args.input_path
    bucket_name = args.bucket_name
    region_name = args.region_name
    aws_access_key_id = args.aws_access_key_id
    aws_secret_access_key = args.aws_secret_access_key

    assert input_path is not None, "input_path can not be Null"
    assert bucket_name is not None, "bucket_name can not be Null"
    assert region_name is not None, "region_name can not be Null"
    assert aws_access_key_id is not None, "aws_access_key_id can not be Null"
    assert aws_secret_access_key is not None, "aws_secret_access_key can not be Null"

    process_folder(
        input_path, bucket_name, aws_access_key_id, aws_secret_access_key, region_name
    )

def main():
    parser = argparse.ArgumentParser(description="Uploades indicated folder to S3 bucket")

    parser.add_argument(
        "--input_path",
        type=str,
        help="Path to root folder with the folders of categories. ",
    )

    parser.add_argument(
        "--bucket_name", type=str, help="Address to S3 bucket"
    )

    parser.add_argument("--region_name", type=str, help="Bucket region", default="eu-west-2")

    parser.add_argument("--aws_access_key_id", type=str, help="AWS access key ID")

    parser.add_argument(
        "--aws_secret_access_key", type=str, help="AWS access key value"
    )

    args = parser.parse_args()
    print(print(args))
    init(args)

if __name__ == '__main__':
    main()
