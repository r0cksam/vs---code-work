import os
import duckdb
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def test_linode_s3_connection():
    # Read from .env
    access_key = os.getenv('LINODE_ACCESS_KEY')
    secret_key = os.getenv('LINODE_SECRET_KEY')
    region = os.getenv('LINODE_REGION', 'in-maa-1')
    endpoint = os.getenv('LINODE_ENDPOINT', 'in-maa-1.linodeobjects.com')
    bucket = os.getenv('LINODE_BUCKET')
    path = os.getenv('LINODE_PATH', '')
    
    if not access_key or not secret_key:
        print("ERROR: LINODE_ACCESS_KEY and LINODE_SECRET_KEY must be set in .env")
        return False
    if not bucket:
        print("ERROR: LINODE_BUCKET must be set in .env")
        return False
    
    # Build S3 URI
    if path:
        s3_uri = f"s3://{bucket}/{path}"
    else:
        s3_uri = f"s3://{bucket}"
    
    print(f"Connecting to: {endpoint}")
    print(f"Region: {region}")
    print(f"Bucket: {bucket}")
    print(f"Path: {path or '(root)'}")
    print(f"Full S3 URI pattern: {s3_uri}/*.gz")
    
    conn = duckdb.connect()
    
    try:
        # Install & load httpfs extension
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        
        # Create secret for Linode S3
        conn.execute(f"""
            CREATE OR REPLACE SECRET linode_secret (
                TYPE S3,
                PROVIDER CONFIG,
                KEY_ID '{access_key}',
                SECRET '{secret_key}',
                REGION '{region}',
                ENDPOINT '{endpoint}'
            );
        """)
        print("✓ S3 secret created")
        
        # List .gz files (limit 10)
        result = conn.execute(f"""
            SELECT file
            FROM glob('{s3_uri}/*.gz')
            LIMIT 10
        """).fetchall()
        
        if result:
            print(f"\n✓ Found {len(result)} .gz file(s):")
            for row in result:
                print(f"  - {row[0]}")
        else:
            print("\n⚠ No .gz files found at that path.")
            print("  Check your bucket name and path in .env")
            return False
        
        # Optional: read top 2 rows from first file
        first_file = result[0][0]
        print(f"\n📖 Testing read from: {first_file}")
        sample = conn.execute(f"""
            SELECT * FROM read_csv('{first_file}', auto_detect=True, compression='gzip')
            LIMIT 2
        """).fetchdf()
        
        print("\n✅ Sample data (first 2 rows):")
        print(sample)
        print("\n✅ Connection test PASSED – everything works!")
        return True
        
    except Exception as e:
        print(f"\n❌ Connection test FAILED:\n{e}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    test_linode_s3_connection()