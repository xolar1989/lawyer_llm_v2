
import logging

import boto3
from botocore.exceptions import NoCredentialsError



logger = logging.getLogger()

logger.setLevel(logging.INFO)

def upload_json_to_s3(json_data, bucket, object_name):
    """Upload JSON data to an S3 bucket

    :param json_data: JSON data to upload
    :param bucket: S3 bucket name
    :param object_name: S3 object name (key)
    :return: True if file was uploaded, else False
    """
    s3_client = boto3.client('s3', region_name='eu-west-1')
    try:
        # Convert JSON data to string and then to bytes

        # Upload the JSON bytes to S3
        s3_client.put_object(Bucket=bucket, Key=object_name, Body=json_data, ContentType='application/json')
        logger.info(f"JSON data uploaded to {bucket}/{object_name}")
        return True
    except NoCredentialsError:
        logger.error(f"""Credentials not available for s3 upload to {bucket}/{object_name}""")
        raise Exception(f"""Credentials not available for s3 upload to {bucket}/{object_name}""")


def extract_bucket_and_key(s3_path):
    # Remove the 's3://' prefix
    s3_path = s3_path.replace("s3://", "")

    # Split the string into bucket and key
    bucket, key = s3_path.split('/', 1)

    return bucket, key