import aiohttp
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

class ApiClient:
    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        token_url: str,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        rate_limit: Optional[float] = None,
        timeout: int = 30,
        max_concurrent: int = 100,
    ):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.rate_limit = rate_limit
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self._access_token = None
        self._token_expiry = None

    async def _get_access_token(self) -> str:
        """Get OAuth 2.0 access token (with caching)"""
        if self._access_token and datetime.now() < self._token_expiry:
            return self._access_token

        auth = aiohttp.BasicAuth(self.client_id, self.client_secret)
        data = {"grant_type": "client_credentials"}

        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.token_url,
                auth=auth,
                data=data,
                timeout=self.timeout,
            ) as response:
                result = await response.json()
                self._access_token = result["access_token"]
                # Assume token expires in 1 hour (adjust based on your API)
                self._token_expiry = datetime.now() + timedelta(seconds=3600)
                return self._access_token

    async def _make_request(
        self,
        session: aiohttp.ClientSession,
        method: str,
        endpoint: str,
        payload: Optional[Dict[str, Any]] = None,
        attempt: int = 1,
    ) -> Dict[str, Any]:
        """Internal request method with retry logic"""
        url = f"{self.base_url}/{endpoint}"
        headers = {"Authorization": f"Bearer {await self._get_access_token()}"}

        try:
            async with self.semaphore:
                async with session.request(
                    method,
                    url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout,
                ) as response:
                    if response.status >= 400:
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=response.reason,
                        )

                    return {
                        "success": True,
                        "status": response.status,
                        "response": await response.json(),
                    }

        except Exception as e:
            if attempt < self.max_retries:
                await asyncio.sleep(self.retry_delay * attempt)
                return await self._make_request(session, method, endpoint, payload, attempt + 1)
            return {
                "success": False,
                "error": str(e),
                "attempts": attempt,
            }

    async def execute_parallel(
        self,
        requests: List[Tuple[str, str, Optional[Dict[str, Any]]]],  # (method, endpoint, payload)
        progress_callback: Optional[callable] = None,
    ) -> List[Dict[str, Any]]:
        """Execute multiple API requests in parallel"""
        start_time = datetime.now()
        results = []

        async with aiohttp.ClientSession() as session:
            tasks = []
            for i, (method, endpoint, payload) in enumerate(requests):
                task = asyncio.create_task(
                    self._make_request(session, method, endpoint, payload)
                )
                tasks.append(task)

                # Rate limiting
                if self.rate_limit and i % self.rate_limit == 0:
                    await asyncio.sleep(1)

            for i, task in enumerate(asyncio.as_completed(tasks)):
                result = await task
                results.append(result)

                if progress_callback:
                    progress = (i + 1) / len(requests) * 100
                    progress_callback(progress)

        total_time = (datetime.now() - start_time).total_seconds()
        print(f"\nProcessed {len(requests)} requests in {total_time:.2f} seconds")
        return results


# Example Usage
async def main():
    # Initialize client with OAuth 2.0 credentials
    api = ApiClient(
        base_url="https://api.example.com/v1",
        client_id="your_client_id",
        client_secret="your_client_secret",
        token_url="https://auth.example.com/oauth2/token",
        max_retries=3,
        rate_limit=10,  # 10 requests/second
    )

    # Prepare batch requests (method, endpoint, payload)
    requests = [
        ("POST", "users", {"name": "Alice", "email": "alice@example.com"}),
        ("GET", "users/123", None),
        ("PUT", "products/456", {"price": 99.99}),
        # Add more requests...
    ]

    # Progress callback
    def progress_update(pct):
        print(f"\rProgress: {pct:.1f}%", end="", flush=True)

    # Execute all requests in parallel
    results = await api.execute_parallel(requests, progress_update)

    # Print summary
    success_count = sum(1 for r in results if r["success"])
    print(f"\nSuccess rate: {success_count}/{len(results)}")

    # Print first 3 results
    for result in results[:3]:
        print(result)

if __name__ == "__main__":
    asyncio.run(main())



import aiohttp
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

class ApiClient:
    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        token_url: str,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        rate_limit: Optional[float] = None,
        timeout: int = 30,
        max_concurrent: int = 100,
    ):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.rate_limit = rate_limit
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self._access_token = None
        self._token_expiry = None

    async def _get_access_token(self) -> str:
        """Get OAuth 2.0 access token (with caching)"""
        if self._access_token and datetime.now() < self._token_expiry:
            return self._access_token

        auth = aiohttp.BasicAuth(self.client_id, self.client_secret)
        data = {"grant_type": "client_credentials"}

        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.token_url,
                auth=auth,
                data=data,
                timeout=self.timeout,
            ) as response:
                result = await response.json()
                self._access_token = result["access_token"]
                self._token_expiry = datetime.now() + timedelta(seconds=3600)
                return self._access_token

    async def _make_request(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        payload: Dict[str, Any],
        attempt: int = 1,
    ) -> Tuple[int, Dict[str, Any]]:
        """Internal request method with retry logic"""
        url = f"{self.base_url}/{endpoint}"
        headers = {"Authorization": f"Bearer {await self._get_access_token()}"}

        try:
            async with self.semaphore:
                async with session.post(
                    url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout,
                ) as response:
                    if response.status >= 400:
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=response.reason,
                        )

                    return (
                        response.status,
                        {
                            "success": True,
                            "payload_id": payload.get("id"),  # Only if payload contains ID
                            "response": await response.json(),
                        }
                    )

        except Exception as e:
            if attempt < self.max_retries:
                await asyncio.sleep(self.retry_delay * attempt)
                return await self._make_request(session, endpoint, payload, attempt + 1)
            return (
                500,
                {
                    "success": False,
                    "payload_id": payload.get("id"),
                    "error": str(e),
                    "attempts": attempt,
                }
            )

    async def post_data_batch(
        self,
        endpoint: str,
        data_batch: List[Tuple[int, Dict[str, Any]]],
        progress_callback: Optional[callable] = None,
    ) -> List[Tuple[int, Dict[str, Any]]]:
        """Post a batch of data to the specified endpoint"""
        start_time = datetime.now()
        results = []

        async with aiohttp.ClientSession() as session:
            tasks = []
            for i, (id, payload) in enumerate(data_batch):
                # Create task for each payload (ignore the ID)
                task = asyncio.create_task(
                    self._make_request(session, endpoint, payload)
                )
                tasks.append(task)

                # Rate limiting
                if self.rate_limit and i % self.rate_limit == 0:
                    await asyncio.sleep(1)

            for i, task in enumerate(asyncio.as_completed(tasks)):
                status, result = await task
                results.append((data_batch[i][0], result))  # Keep original ID with result

                if progress_callback:
                    progress = (i + 1) / len(data_batch) * 100
                    progress_callback(progress)

        total_time = (datetime.now() - start_time).total_seconds()
        print(f"\nProcessed {len(data_batch)} requests in {total_time:.2f} seconds")
        return results


# Example Usage
async def main():
    # Initialize client
    api = ApiClient(
        base_url="https://api.example.com/v1",
        client_id="your_client_id",
        client_secret="your_client_secret",
        token_url="https://auth.example.com/oauth2/token",
        rate_limit=10,
    )

    # Sample data in (id, payload) format
    sample_data = [
        (1, {"A": 1, "B": "test"}),
        (2, {"A": 2, "B": "test"}),
        (3, {"A": 3, "B": "test"})
    ]

    # Progress callback
    def progress_update(pct):
        print(f"\rProgress: {pct:.1f}%", end="", flush=True)

    # Post all data to endpoint
    results = await api.post_data_batch(
        endpoint="your_endpoint",
        data_batch=sample_data,
        progress_callback=progress_update,
    )

    # Print results with original IDs
    print("\nResults:")
    for id, result in results:
        print(f"ID: {id}, Status: {'Success' if result['success'] else 'Failed'}")
        if result["success"]:
            print(f"Response: {result['response']}")
        else:
            print(f"Error: {result['error']}")
        print("-" * 40)

if __name__ == "__main__":
    asyncio.run(main())