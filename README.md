# MinIO Python Load Balancer

This project provides a Python load balancer for MinIO.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```python
from mc import LoadBalancer
import time

endpoints = ["localhost:9000", "localhost:9001"] # Replace with your MinIO server endpoints
access_key = "YOUR_ACCESS_KEY"
secret_key = "YOUR_SECRET_KEY"
secure = False # Set to True if using HTTPS
health_check_timeout_seconds = 5 # Timeout for health check in seconds

try:
    lb = LoadBalancer(
        endpoints=endpoints,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
        health_check_timeout_seconds=health_check_timeout_seconds
    )

    # Get a client
    original_index, client = lb.get_client()
    if client:
        print(f"Using client for endpoint: {endpoints[original_index]}")
        # Use the client for MinIO operations
        # Example: buckets = client.list_buckets()
        # for bucket in buckets:
        #     print(bucket.name, bucket.creation_date)
    else:
        print("No MinIO servers available.")

except Exception as e:
    print(f"Error: {e}")

``` 