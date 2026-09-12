import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load credentials from local .env
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")


def verify_s3_topology():
    print(f"[*] Initializing S3 client for region: {AWS_REGION}")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    # 1. Test bucket accessibility
    try:
        print(f"[*] Checking access to bucket: {BUCKET_NAME}...")
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"[+] Successfully connected to {BUCKET_NAME}")
    except ClientError as e:
        print(f"[-] Access denied or bucket does not exist: {e}")
        return False

    # 2. Test write permissions (PutObject) to bronze/
    test_key = "bronze/_health_check.txt"
    try:
        print(f"[*] Testing write access to {test_key}...")
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=test_key,
            Body=b"EkLakshay baseline infrastructure check: OK",
        )
        print(f"[+] Successfully wrote health check file to S3!")
    except ClientError as e:
        print(f"[-] PutObject permission failed: {e}")
        return False

    # 3. Test list permissions
    try:
        print(f"[*] Verifying bucket prefixes...")
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Delimiter="/")
        prefixes = [p.get("Prefix") for p in response.get("CommonPrefixes", [])]
        print(f"[+] Found prefixes: {prefixes}")
    except ClientError as e:
        print(f"[-] ListObjects permission failed: {e}")
        return False

    print("\n[SUCCESS] AWS IAM and S3 Data Lake Topology are fully operational.")
    return True


if __name__ == "__main__":
    verify_s3_topology()