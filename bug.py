import botocore.session

# Create a session using botocore
session = botocore.session.get_session()

credentials = {
    'aws_access_key_id': '',
    'aws_secret_access_key': ''
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

# Upload the object to the S3 bucket
try:
    with open(file_path, 'rb') as file_data:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=file_data
        )
    print(f"File '{file_path}' uploaded to '{bucket_name}/{object_key}' successfully.")
except Exception as e:
    print(f"An error occurred: {e}")