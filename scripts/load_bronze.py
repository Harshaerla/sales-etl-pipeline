import boto3
import os
import pandas as pd
import snowflake.connector
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

# AWS connection
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

# Snowflake connection
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema='bronze'
)
cursor = conn.cursor()

BUCKET = os.getenv('S3_BUCKET_NAME')

# S3 file → Snowflake Bronze table mapping
FILES = {
    'landing/crm/crm_cust_info.csv':     'bronze.crm_cust_info',
    'landing/crm/crm_cust_az12.csv':     'bronze.crm_cust_az12',
    'landing/crm/crm_loc_a101.csv':      'bronze.crm_loc_a101',
    'landing/erp/erp_prd_info.csv':      'bronze.erp_prd_info',
    'landing/erp/erp_px_cat_g1v2.csv':   'bronze.erp_px_cat_g1v2',
    'landing/erp/erp_sales_details.csv': 'bronze.erp_sales_details',
}

print("🚀 Loading files from S3 landing → Snowflake Bronze...\n")

for s3_path, table in FILES.items():
    try:
        # Read CSV from S3
        obj = s3.get_object(Bucket=BUCKET, Key=s3_path)
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

        # ✅ Replace ALL NaN/NaT with None so Snowflake accepts NULL
        df = df.astype(object).where(pd.notnull(df), None)

        # Truncate table before loading (fresh load)
        cursor.execute(f"TRUNCATE TABLE {table}")

        # Build insert query dynamically
        cols = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

        # Insert rows
        data = [tuple(row) for row in df.itertuples(index=False)]
        cursor.executemany(insert_sql, data)

        print(f"✅ Loaded {len(df)} rows → {table}")

    except Exception as e:
        print(f"❌ Failed: {table} → {e}")

conn.commit()
cursor.close()
conn.close()

print("\n✅ Bronze load complete!")