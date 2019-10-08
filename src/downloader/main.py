import argparse
from src.downloader.download_bucket import download_bucket


def init(args):
    output_path = args.output_path
    bucket_name = args.bucket_name
    region_name = args.region_name
    aws_access_key_id = args.aws_access_key_id
    aws_secret_access_key = args.aws_secret_access_key

    assert output_path is not None, "output_path can not be Null"
    assert bucket_name is not None, "bucket_name can not be Null"
    assert region_name is not None, "region_name can not be Null"
    assert aws_access_key_id is not None, "aws_access_key_id can not be Null"
    assert aws_secret_access_key is not None, "aws_secret_access_key can not be Null"

    download_bucket(
        output_path, bucket_name, aws_access_key_id, aws_secret_access_key, region_name
    )


def main():
    parser = argparse.ArgumentParser(description="Downloads the whole S3 bucket to specified path",)

    parser.add_argument(
        "--output_path",
        type=str,
        help="Path to output folder with the where the folder will be downloaded to",
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
