import argparse

def init(args):
    output_path = args.output_path
    input_path = args.input_path
    resized_image_shape = (args.resize_image_width, args.resize_image_height)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Preprocessing the images')


    parser.add_argument(
        "--input_path",
        type=str,
        help="Path to root folder with the folders of categories."
    )

    parser.add_argument(
        "--bucket_address",
        type=str,
        help="Address to S3 bucket"
    )

    args = parser.parse_args()
    print(print(args))
    init(args)