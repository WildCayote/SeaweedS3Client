import boto3

class S3Handler:
    def __init__(self, s3_url:str, access_key:str, secret_key:str):
        self.url = s3_url
        self.access_key = access_key
        self.secret_key = secret_key

        self.client = self._initialize_boto_client()

    def _initialize_boto_client(self):
        client = boto3.client(
            service_name='s3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url=self.url
        )
        
        return client

    def get_buckets(self):
        ...

    def create_bucket(self, name:str):
        ...
    
    def delete_bucket(self, name:str):
        ...
    
    def get_presigned_download_url(self, s3_object_url:str, expiration:int):
        ...

    def get_presigned_upload_url(self, bucket_name:str):
        ...

