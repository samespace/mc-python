# mc-python/run_balancer.py
from mc import LoadBalancer
import os # Added for environment variables
import time

# --- Configuration ---
# Option 1: Hardcoded (Update these values)
# minio_endpoints = ["your-minio-server1.com:9000", "your-minio-server2.com:9000"]
# minio_access_key = "YOUR_MINIO_ACCESS_KEY"
# minio_secret_key = "YOUR_MINIO_SECRET_KEY"
# minio_secure = False # Set to True if your MinIO uses HTTPS

# Option 2: From Environment Variables (Recommended for keys)
minio_endpoints_str = os.getenv("MINIO_ENDPOINTS", "localhost:9000,localhost:9001") # Comma-separated
minio_endpoints = [ep.strip() for ep in minio_endpoints_str.split(',')]

minio_access_key = os.getenv("MINIO_ACCESS_KEY", "YOUR_DEFAULT_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY", "YOUR_DEFAULT_SECRET_KEY")
minio_secure_str = os.getenv("MINIO_SECURE", "False")
minio_secure = minio_secure_str.lower() in ['true', '1', 't']

health_check_timeout_seconds = int(os.getenv("MINIO_HEALTH_CHECK_TIMEOUT", "5"))
unhealthy_retry_minutes = int(os.getenv("MINIO_UNHEALTHY_RETRY_MINUTES", "10"))
# --- End Configuration ---

if minio_access_key == "YOUR_DEFAULT_ACCESS_KEY" or minio_secret_key == "YOUR_DEFAULT_SECRET_KEY":
    print("WARNING: Please set your MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables or update them in the script.")
    # You might want to exit here if keys are mandatory and not set
    # exit(1)


print(f"Attempting to connect to MinIO endpoints: {minio_endpoints}")
print(f"Using Access Key: {minio_access_key[:5]}...{minio_access_key[-5:] if len(minio_access_key) > 10 else ''}") # Avoid printing full key
print(f"Secure connection: {minio_secure}")

try:
    lb = LoadBalancer(
        endpoints=minio_endpoints,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=minio_secure,
        health_check_timeout_seconds=health_check_timeout_seconds,
        unhealthy_retry_minutes=unhealthy_retry_minutes
    )

    print("LoadBalancer initialized.")

    # Get a client
    print("Attempting to get a client from the load balancer...")
    for i in range(len(minio_endpoints) + 2): # Try a few times to see round robin
        original_index, client = lb.get_client()
        if client:
            print(f"Attempt {i+1}: Successfully got client for endpoint: {lb.endpoints[original_index]} (Index: {original_index})")
            # You can now use the 'client' object for MinIO operations
            # Example:
            # buckets = client.list_buckets()
            # print("Available buckets:")
            # for bucket in buckets:
            #     print(f"- {bucket.name} (created at {bucket.creation_date})")
        else:
            print(f"Attempt {i+1}: No MinIO servers available through the load balancer at this moment.")

        if i < len(minio_endpoints) +1 : # don't sleep on last iteration
            print("Sleeping for 2 seconds before next attempt...")
            time.sleep(2)


except ValueError as ve:
    print(f"Configuration Error: {ve}")
except Exception as e:
    print(f"An error occurred: {e}") 