import os
from pathlib import Path
import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET = os.getenv("AWS_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

s3 = session.client("s3")

files_to_upload = [
    ("data/raw/superstore_sales.csv", "raw/sales/superstore_sales.csv"),
    ("data/raw/product_catalog.xml", "raw/products/product_catalog.xml"),
]

for local_path, s3_key in files_to_upload:
    local_file = Path(local_path)

    if not local_file.exists():
        raise FileNotFoundError(f"Fichier introuvable : {local_path}")

    print(f"Upload de {local_path} vers s3://{AWS_BUCKET}/{s3_key}")
    s3.upload_file(str(local_file), AWS_BUCKET, s3_key)

print("Upload terminé avec succès.")