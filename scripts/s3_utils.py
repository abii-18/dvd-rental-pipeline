import boto3
import os

def upload_to_s3(file_path, execution_date):
    s3 = boto3.client("s3")

    bucket_name = "abinav-dvdrental-bucket1"

    file_name = os.path.basename(file_path)

    s3_key = f"dvd_rentals/date={execution_date}/{file_name}"

    s3.upload_file(file_path, bucket_name, s3_key)

    return s3_key