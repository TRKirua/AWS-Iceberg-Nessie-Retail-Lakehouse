import os
from dotenv import load_dotenv
import boto3

# Charge environnement variables from .env file
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET = os.getenv("AWS_BUCKET")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

print("Region:", AWS_REGION)
print("Bucket:", AWS_BUCKET)

# Creation of a session using the credentials
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Create an S3 client
s3 = session.client("s3")

# List buckets to verify connection
response = s3.list_buckets()

print("\nBuckets accessibles :")
for bucket in response["Buckets"]:
    print("-", bucket["Name"])