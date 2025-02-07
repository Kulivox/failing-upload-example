import botocore.session
from os import environ
import s3fs

credentials = {
    'aws_access_key_id': environ['AWS_ACCESS_KEY_ID'],
    'aws_secret_access_key': environ['AWS_SECRET_ACCESS_KEY'],
}

# Create an S3 client
# Specify the bucket name and the object key
bucket_name = 'test-ceph-bucket'
object_key = 'object'
file_path = 'file.txt'



# Upload the object to the S3 bucket
def failing_botocore_call():
    session = botocore.session.get_session()
    s3_client = session.create_client(
        's3',
        aws_access_key_id=credentials['aws_access_key_id'],
        aws_secret_access_key=credentials['aws_secret_access_key'],
        aws_session_token=credentials.get('aws_session_token'),  # Optional
        endpoint_url="https://s3.be.du.cesnet.cz"  # Custom S3 endpoint
    )

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


def failing_s3fs_call():
    fs = s3fs.S3FileSystem(
        key=credentials['aws_access_key_id'],
        secret=credentials['aws_secret_access_key'],
        client_kwargs={'endpoint_url': "https://s3.be.du.cesnet.cz"},
    )

    with open(file_path, 'rb') as local_file:
        with fs.open(f'{bucket_name}/{file_path}', 'wb') as s3_file:
            s3_file.write(local_file.read())



if __name__ == '__main__':
    failing_botocore_call()
    failing_s3fs_call() # The method above is an approximation of what is called within s3fs

