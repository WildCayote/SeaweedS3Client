import boto3, logging, os, io
from botocore.exceptions import ClientError
from io import BufferedReader

class S3Handler:
    """
    A Python class for managing interactions with an S3-compatible storage system.

    This class provides methods to interact with buckets and objects in an S3-compatible
    storage, including creating, deleting, uploading, downloading, and generating presigned URLs.

    Attributes:
        s3_url (str): The endpoint URL of the S3-compatible storage system.
        access_key (str): The access key for authentication.
        secret_key (str): The secret key for authentication.
        client: The initialized boto3 client for S3 operations.
    """

    def __init__(self, s3_url: str, access_key: str, secret_key: str):
        """
        Initializes the S3Handler with the given credentials and endpoint.

        Args:
            s3_url (str): The URL of the S3-compatible storage endpoint.
            access_key (str): Access key for S3 authentication.
            secret_key (str): Secret key for S3 authentication.
        """
        self.url = s3_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.client = self._initialize_boto_client()

    def _initialize_boto_client(self):
        """
        Initializes and returns a boto3 S3 client.

        Returns:
            boto3.Client: An S3 client for interacting with the storage.
        """
        client = boto3.client(
            service_name='s3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url=self.url
        )
        return client

    def get_buckets(self):
        """
        Retrieves a list of all buckets in the S3 storage.

        Returns:
            list: A list of bucket details.
        """
        response = self.client.list_buckets()
        return response['Buckets']

    def create_bucket(self, name: str, region: str = None):
        """
        Creates a new bucket in the S3 storage.

        Args:
            name (str): The name of the bucket to create.
            region (str, optional): The region for the bucket. Defaults to None.

        Returns:
            bool: True if the bucket was created successfully, False otherwise.
        """
        try:
            if region is None:
                self.client.create_bucket(Bucket=name)
            else:
                location = {'LocationConstraint': region}
                self.client.create_bucket(Bucket=name, CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def delete_bucket(self, name: str):
        """
        Deletes a bucket from the S3 storage.

        Args:
            name (str): The name of the bucket to delete.

        Returns:
            bool: True if the bucket was deleted successfully, False otherwise.
        """
        try:
            self.client.delete_bucket(Bucket=name)
            return True
        except Exception as e:
            logging.error(e)
            return False

    def upload_file(self, path_to_file: str, bucket_name: str, object_name: str = None):
        """
        Uploads a file to the specified bucket.

        Args:
            path_to_file (str): The local path to the file.
            bucket_name (str): The target bucket name.
            object_name (str, optional): The name of the object in the bucket. Defaults to None.

        Returns:
            tuple: (bool, str or None) Success status and S3 URI if successful.
        """
        if object_name is None:
            object_name = os.path.basename(path_to_file)

        try:
            self.client.upload_file(path_to_file, bucket_name, object_name)
            response = f's3://{bucket_name}/{object_name}'
        except ClientError as e:
            logging.error(e)
            return False, None
        return True, response

    def upload_file_binary(self, buffer: BufferedReader, bucket_name: str, object_name: str = None):
        """
        Uploads a file from a binary buffer to the specified bucket.

        Args:
            buffer (BufferedReader): The binary buffer of the file.
            bucket_name (str): The target bucket name.
            object_name (str, optional): The name of the object in the bucket. Defaults to None.

        Returns:
            tuple: (bool, str or None) Success status and S3 URI if successful.
        """
        if object_name is None:
            object_name = os.path.basename(buffer.name)

        try:
            self.client.upload_fileobj(buffer, bucket_name, object_name)
            response = f's3://{bucket_name}/{object_name}'
        except ClientError as e:
            logging.error(e)
            return False, None
        return True, response

    def delete_object(self, bucket_name: str, object_name: str):
        """
        Deletes an object from a bucket.

        Args:
            bucket_name (str): The name of the bucket.
            object_name (str): The key of the object to delete.

        Returns:
            tuple: (bool, dict or None) Success status and response details.
        """
        try:
            response = self.client.delete_object(Bucket=bucket_name, Key=object_name)
            return True, response
        except Exception as e:
            logging.error(e)
            return False, None

    def download_object(self, bucket_name: str, object_name: str):
        """
        Downloads an object as a binary buffer.

        Args:
            bucket_name (str): The name of the bucket.
            object_name (str): The key of the object to download.

        Returns:
            tuple: (bool, BytesIO or None) Success status and binary buffer of the object.
        """
        try:
            buffer = io.BytesIO()
            self.client.download_fileobj(bucket_name, object_name, buffer)
            buffer.seek(0)
            return True, buffer
        except Exception as e:
            logging.error(e)
            return False, None

    def get_presigned_download_url(self, bucket_name: str, object_name: str, expiration: int = 3600):
        """
        Generates a presigned URL for downloading an object.

        Args:
            bucket_name (str): The name of the bucket.
            object_name (str): The key of the object.
            expiration (int, optional): Time in seconds for the URL to expire. Defaults to 3600.

        Returns:
            str or None: The presigned URL or None if generation failed.
        """
        try:
            response = self.client.generate_presigned_url(
                ClientMethod='get_object',
                Params={'Bucket': bucket_name, 'Key': object_name},
                ExpiresIn=expiration
            )
        except ClientError as e:
            logging.error(e)
            return None
        return response

    def get_presigned_upload_url(self, bucket_name: str, object_name: str, expiration: int = 3600):
        """
        Generates a presigned URL for uploading an object.

        Args:
            bucket_name (str): The name of the bucket.
            object_name (str): The key of the object.
            expiration (int, optional): Time in seconds for the URL to expire. Defaults to 3600.

        Returns:
            dict or None: The presigned URL and required fields or None if generation failed.
        """
        try:
            response = self.client.generate_presigned_post(
                bucket_name,
                object_name,
                ExpiresIn=expiration
            )
        except ClientError as e:
            logging.error(e)
            return None
        return response

if __name__ == '__main__':
    handler = S3Handler(
        s3_url="http://localhost:3333",
        access_key="any",
        secret_key="any"
    )

    print(handler.create_bucket(name='testbucket'))
    print(handler.get_buckets())
    
    success, response = handler.upload_file(path_to_file='./s3_config.json', bucket_name='testbucket')
    print("Upload status: ", success)
    print("Upload response: ", response)
    
    with open('s3_config.json', 'rb') as file:
        success, response = handler.upload_file_binary(buffer=file, bucket_name='testbucket', object_name='binary_read_file.json')
        print("Upload status: ", success)
        print("Upload response: ", response)
    
    delete_success, response = handler.delete_object(bucket_name="testbucket", object_name="binary_read_file.json")
    print("Delete status: ", delete_success)
    print("Delete response: ", response)
    
    download_success, file_byte = handler.download_object(bucket_name='testbucket', object_name='s3_config.json')
    print("Download status: ", download_success)
    with open('downloaded_file.json', 'wb') as f:
        f.write(file_byte.read())

    presigned_download_response = handler.get_presigned_download_url(bucket_name='testbucket', object_name='s3_config.json', expiration=60)
    print("Presigned download URL response: ", presigned_download_response)

    presigned_upload_response = handler.get_presigned_upload_url(bucket_name='testbucket', object_name='presigned_upload.json')
    print("Presigned upload URL response: ", presigned_upload_response)

    print(handler.get_buckets())
