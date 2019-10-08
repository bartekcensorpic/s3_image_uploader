import argparse
from src.downloader.download_bucket import download_bucket


def init(args):
    input_path = args.output_path
    bucket_name = args.bucket_name
    region_name = args.region_name
    aws_access_key_id = args.aws_access_key_id
    aws_secret_access_key = args.aws_secret_access_key

    download_bucket(
        input_path, bucket_name, aws_access_key_id, aws_secret_access_key, region_name
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Preprocessing the images")

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
