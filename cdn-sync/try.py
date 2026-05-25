import boto3
from botocore.config import Config
import config

s3 = boto3.client(
    "s3",
    endpoint_url=config.ENDPOINT_URL,
    aws_access_key_id=config.ACCESS_KEY_ID,
    aws_secret_access_key=config.SECRET_KEY,
    config=Config(connect_timeout=15, read_timeout=60, retries={"max_attempts": 1}),
)

# Test the exact prefix the script uses for today
r = s3.list_objects_v2(
    Bucket=config.BUCKET_NAME,
    Prefix="veto-stream-logs/05/22/",
    MaxKeys=5
)
print("Keys found:", len(r.get("Contents", [])))
for o in r.get("Contents", []):
    print(o["Key"])