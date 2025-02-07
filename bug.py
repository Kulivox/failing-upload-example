import base64
import hashlib
import zlib
import botocore.session
from os import environ
# Create a session using botocore
session = botocore.session.get_session()

credentials = {
    'aws_access_key_id': environ['AWS_ACCESS_KEY_ID'],
    'aws_secret_access_key': environ['AWS_SECRET_ACCESS_KEY'],
}

# Create an S3 client
# Specify the bucket name and the object key
bucket_name = 'test-ceph-bucket'
object_key = 'object'
file_path = 'file.txt'

s3_client = session.create_client(
    's3',
    aws_access_key_id=credentials['aws_access_key_id'],
    aws_secret_access_key=credentials['aws_secret_access_key'],
    aws_session_token=credentials.get('aws_session_token'),  # Optional
    endpoint_url="https://s3.be.du.cesnet.cz"  # Custom S3 endpoint
)


def calculate_sha256(file_path):
    hash_func = hashlib.new("sha256")
    with open(file_path, 'rb') as file_data:
        for chunk in iter(lambda: file_data.read(4096), b""):
            hash_func.update(chunk)
    return base64.b64encode(hash_func.digest()).decode('utf-8')

# Upload the object to the S3 bucket
def default_checksum_fails():
    try:
        with open(file_path, 'rb') as file_data:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=file_data,
            )
        print(f"File '{file_path}' uploaded to '{bucket_name}/{object_key}' successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")


def explicit_crc_checksum_passes():
    try:
        with open(file_path, 'rb') as file_data:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=file_data,
                ChecksumAlgorithm="CRC32",
                ChecksumCRC32= f"{zlib.crc32(file_data.read())}"

            )
        print(f"File '{file_path}' uploaded to '{bucket_name}/{object_key}' successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")


def explicit_sha256_checksum_passes():
    try:
        with open(file_path, 'rb') as file_data:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=file_data,
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=calculate_sha256(file_path),
            )
        print(f"File '{file_path}' uploaded to '{bucket_name}/{object_key}' successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == '__main__':
    default_checksum_fails()
    explicit_crc_checksum_passes()
    explicit_sha256_checksum_passes()
