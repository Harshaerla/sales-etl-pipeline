import boto3
import os
from dotenv import load_dotenv

load_dotenv()

# AWS connection
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET = os.getenv('S3_BUCKET_NAME')

# Source → Destination mapping (matched to actual S3 paths)
FILES = {
    'client-landing/crm/cust_info.csv':     'landing/crm/crm_cust_info.csv',
    'client-landing/crm/prd_info.csv':      'landing/erp/erp_prd_info.csv',
    'client-landing/crm/sales_details.csv': 'landing/erp/erp_sales_details.csv',
    'client-landing/erp/CUST_AZ12.csv':     'landing/crm/crm_cust_az12.csv',
    'client-landing/erp/LOC_A101.csv':      'landing/crm/crm_loc_a101.csv',
    'client-landing/erp/PX_CAT_G1V2.csv':  'landing/erp/erp_px_cat_g1v2.csv',
}

print("🚀 Copying files from client-landing → landing...\n")

for source, destination in FILES.items():
    try:
        s3.copy_object(
            Bucket=BUCKET,
            CopySource={'Bucket': BUCKET, 'Key': source},
            Key=destination
        )
        print(f"✅ Copied: {source} → {destination}")
    except Exception as e:
        print(f"❌ Failed: {source} → {e}")

print("\n✅ Ingestion to landing complete!")