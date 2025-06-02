import datetime
import threading
import time
from typing import List, Optional, Tuple

import urllib3
from minio import Minio
from minio.error import S3Error


class LoadBalancer:
    def __init__(
        self,
        endpoints: List[str],
        access_key: str,
        secret_key: str,
        secure: bool = False,
        health_check_timeout_seconds: int = 5,
        unhealthy_retry_minutes: int = 10,
    ):
        if not endpoints:
            raise ValueError("Endpoints list cannot be empty.")

        self.endpoints = endpoints
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.health_check_timeout_seconds = health_check_timeout_seconds
        self.unhealthy_retry_timedelta = datetime.timedelta(
            minutes=unhealthy_retry_minutes
        )

        self.clients: List[Optional[Minio]] = [None] * len(endpoints)
        self.failures: List[Optional[datetime.datetime]] = [None] * len(endpoints)
        self.current_index: int = 0
        self.mutex = threading.Lock()
        self._http_client = urllib3.PoolManager(
            timeout=urllib3.Timeout(connect=health_check_timeout_seconds, read=health_check_timeout_seconds)
        )


        for i, endpoint in enumerate(endpoints):
            try:
                client = Minio(
                    endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=secure,
                )
                # Perform an initial health check
                if self._is_server_healthy(client, endpoint):
                    self.clients[i] = client
                else:
                    self.failures[i] = datetime.datetime.now(datetime.timezone.utc)
                    print(f"Initial health check failed for {endpoint}")

            except Exception as e:
                print(f"Failed to initialize client for endpoint {endpoint}: {e}")
                self.failures[i] = datetime.datetime.now(datetime.timezone.utc)


        if not any(self.clients):
             raise Exception("No MinIO servers could be connected to during initialization.")


    def _is_server_healthy(self, client: Optional[Minio], endpoint_address: str) -> bool:
        """
        Checks if the MinIO server is healthy.
        Uses a lightweight check, like trying to access a known health endpoint or a simple API call.
        """
        if client is None: # Was never healthy or failed initialization
            # Try to create a new client to check if it's back online
            try:
                temp_client = Minio(
                    endpoint_address,
                    access_key=self.access_key,
                    secret_key=self.secret_key,
                    secure=self.secure,
                )
                # Use a more direct health check if available, otherwise a lightweight operation
                # For MinIO, a GET request to /minio/health/live is common
                scheme = "https" if self.secure else "http"
                health_url = f"{scheme}://{endpoint_address}/minio/health/live"
                try:
                    resp = self._http_client.request("GET", health_url, timeout=self.health_check_timeout_seconds)
                    return resp.status == 200
                except urllib3.exceptions.MaxRetryError: # Catches connection errors
                    return False
                except urllib3.exceptions.NewConnectionError:
                    return False
                except Exception: # Catch other potential errors like timeout
                    return False

            except Exception:
                 return False # Failed to create client

        # If client exists, try a lightweight operation (e.g. list_buckets)
        # This is a more robust check if the server was previously healthy
        try:
            client.list_buckets()  # This is a relatively lightweight operation.
            return True
        except S3Error as e:
            # Specific S3 errors might indicate issues, but connection errors are key
            print(f"Health check (list_buckets) failed for {endpoint_address}: {e}")
            return False
        except Exception as e: # Catch other potential errors like network issues
            print(f"Generic health check (list_buckets) failed for {endpoint_address}: {e}")
            return False


    def get_client(self) -> Tuple[Optional[int], Optional[Minio]]:
        with self.mutex:
            num_clients = len(self.endpoints)
            if num_clients == 0:
                return None, None # Should have been caught in init

            start_idx_loop_detection = self.current_index

            for _ in range(num_clients): # Iterate at most num_clients times
                current_actual_idx = self.current_index % num_clients
                endpoint_address = self.endpoints[current_actual_idx]
                client_to_check = self.clients[current_actual_idx]

                # Advance index for next attempt (round-robin)
                self.current_index = (self.current_index + 1) % num_clients

                # Case 1: Client exists and is healthy (or was recently)
                if client_to_check is not None:
                    if self._is_server_healthy(client_to_check, endpoint_address):
                        self.failures[current_actual_idx] = None # Mark as healthy
                        return current_actual_idx, client_to_check
                    else:
                        # It was healthy, but now it's not. Mark failure.
                        if self.failures[current_actual_idx] is None:
                           self.failures[current_actual_idx] = datetime.datetime.now(datetime.timezone.utc)
                        # Don't return yet, try next one.
                        self.clients[current_actual_idx] = None # Remove unhealthy client instance
                        continue

                # Case 2: Client was never initialized or previously failed
                # Check if it's time to retry a previously failed server
                if self.failures[current_actual_idx] is not None:
                    time_since_failure = datetime.datetime.now(datetime.timezone.utc) - self.failures[current_actual_idx]
                    if time_since_failure >= self.unhealthy_retry_timedelta:
                        print(f"Retrying previously unhealthy server: {endpoint_address}")
                        # Attempt to bring it back online
                        if self._is_server_healthy(None, endpoint_address): # Pass None to force new client creation attempt
                            try:
                                new_client = Minio(
                                    endpoint_address,
                                    access_key=self.access_key,
                                    secret_key=self.secret_key,
                                    secure=self.secure,
                                )
                                self.clients[current_actual_idx] = new_client
                                self.failures[current_actual_idx] = None
                                print(f"Server {endpoint_address} is back online.")
                                return current_actual_idx, new_client
                            except Exception as e:
                                print(f"Failed to re-initialize client for {endpoint_address} after retry: {e}")
                                self.failures[current_actual_idx] = datetime.datetime.now(datetime.timezone.utc) # Update failure time
                        else:
                            # Still unhealthy, update failure time
                            self.failures[current_actual_idx] = datetime.datetime.now(datetime.timezone.utc)
                            print(f"Server {endpoint_address} still unhealthy after retry.")
                # If we're here, the current_actual_idx is not usable. Continue to next.

                # Avoid infinite loop if all servers are down and not yet retryable.
                if self.current_index == start_idx_loop_detection and all(f is not None and (datetime.datetime.now(datetime.timezone.utc) - f) < self.unhealthy_retry_timedelta for f in self.failures):
                    break


            # If loop completes, no server is available
            print("No healthy MinIO servers available after checking all.")
            return None, None

    def __del__(self):
        # Clean up the urllib3 client
        if hasattr(self, '_http_client') and self._http_client:
            try:
                self._http_client.clear()
            except Exception as e:
                print(f"Error cleaning up http client: {e}") 