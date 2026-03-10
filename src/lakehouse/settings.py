import os
from pathlib import Path
from dotenv import load_dotenv

# Project root = 2 levels above this file:
# src/lakehouse/settings.py -> project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load .env from project root
load_dotenv(PROJECT_ROOT / ".env")

AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET = os.getenv("AWS_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

NESSIE_URI = os.getenv("NESSIE_URI")
NESSIE_BRANCH = os.getenv("NESSIE_BRANCH")
WAREHOUSE = os.getenv("WAREHOUSE")