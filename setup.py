from setuptools import setup

setup(
    name="SeaweedS3Client",
    version="0.0.1",
    description="A python package for interacting with SeaweedFS S3 API. It wraps the Boto3 module and is geared towards our AutoML Platform.",
    url="git@github.com:WildCayote/SeaweedS3Client.git",
    author="Tinsae Shemalise",
    author_email="tinsaeshemalise@gmail.com",
    license="unlicense",
    packages=['SeaweedS3Client'],
    zip_safe=False    
)
