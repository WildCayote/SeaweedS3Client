import boto3, logging
from botocore.exceptions import ClientError

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
        response = self.client.list_buckets()
        buckets = response['Buckets']

        return buckets

    def create_bucket(self, name:str, region:str=None):
        try:
            if region is None:
                self.client.create_bucket(Bucket=name)
            else:
                location = {'LocationConstraint': region}
                self.client.create_bucket(Bucket=name,
                                        CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    def delete_bucket(self, name:str):
        try:
            self.client.delete_bucket(Bucket=name)
            return True
        except Exception as e:
            logging.error(e)
            return True
    
    def get_presigned_download_url(self, s3_object_url:str, expiration:int):
        ...

    def get_presigned_upload_url(self, bucket_name:str):
        ...

if __name__ == '__main__':
    handler = S3Handler(
        s3_url="http://localhost:3333",
        access_key="any",
        secret_key="any"
    )

    print(handler.create_bucket(name='testbucket'))
    print(handler.get_buckets())
    print(handler.delete_bucket(name='testbucket'))
    print(handler.get_buckets())