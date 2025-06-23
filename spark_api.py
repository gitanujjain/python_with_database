import asyncio
import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

import aiohttp
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType


class APIAuthenticator(ABC):
    """Abstract base class for API authentication"""

    @abstractmethod
    async def get_auth_headers(self) -> Dict[str, str]:
        """Return authentication headers"""
        pass


class BasicAuthAuthenticator(APIAuthenticator):
    """Handles Basic Authentication"""

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    async def get_auth_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Basic {self.username}:{self.password}"
        }


class OAuthAuthenticator(APIAuthenticator):
    """Handles OAuth 2.0 Authentication with token refresh"""

    def __init__(self, token_url: str, client_id: str, client_secret: str, scope: str = None):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.access_token = None
        self.token_expiry = None

    async def get_auth_headers(self) -> Dict[str, str]:
        if not self.access_token or await self._is_token_expired():
            await self._refresh_token()
        return {"Authorization": f"Bearer {self.access_token}"}

    async def _is_token_expired(self) -> bool:
        # Implement actual token expiry check
        return False

    async def _refresh_token(self):
        async with aiohttp.ClientSession() as session:
            payload = {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials"
            }
            if self.scope:
                payload["scope"] = self.scope

            async with session.post(self.token_url, data=payload) as resp:
                if resp.status == 200:
                    token_data = await resp.json()
                    self.access_token = token_data["access_token"]
                    self.token_expiry = token_data.get("expires_in")


class AsyncAPIClient:
    """Handles async API requests with retry logic"""

    def __init__(self, base_url: str, authenticator: APIAuthenticator, max_retries: int = 3):
        self.base_url = base_url
        self.authenticator = authenticator
        self.max_retries = max_retries
        self.logger = logging.getLogger(self.__class__.__name__)

    async def send_request(self, endpoint: str, payload: Dict, method: str = "POST") -> Dict:
        """Send a single API request with retry logic"""
        url = f"{self.base_url}/{endpoint}"

        for attempt in range(self.max_retries):
            try:
                headers = await self.authenticator.get_auth_headers()

                async with aiohttp.ClientSession() as session:
                    async with session.request(
                            method,
                            url,
                            json=payload,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        response.raise_for_status()
                        return {
                            "status": response.status,
                            "data": await response.json(),
                            "success": True
                        }

            except Exception as e:
                if attempt == self.max_retries - 1:
                    return {
                        "status": "ERROR",
                        "error": str(e),
                        "success": False
                    }
                await asyncio.sleep(2 ** attempt)  # Exponential backoff


class SparkAPIIntegrator:
    """Integrates PySpark with async API calls"""

    def __init__(self, spark: SparkSession, api_client: AsyncAPIClient, batch_size: int = 100):
        self.spark = spark
        self.api_client = api_client
        self.batch_size = batch_size
        self.response_schema = StructType([
            StructField("id", IntegerType()),
            StructField("status", StringType()),
            StructField("response", StringType()),
            StructField("success", StringType())
        ])

    async def _process_batch(self, batch: List[Dict]) -> List[Dict]:
        """Process a batch of records asynchronously"""
        tasks = [self.api_client.send_request("data", record) for record in batch]
        return await asyncio.gather(*tasks)

    def _process_partition(self, partition):
        """Spark partition processor"""
        batch = []
        results = []

        for row in partition:
            payload = {
                "id": row["id"],
                "data": row["data"]
            }
            batch.append(payload)

            if len(batch) >= self.batch_size:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                batch_results = loop.run_until_complete(self._process_batch(batch))
                results.extend(self._format_results(batch, batch_results))
                batch = []

        if batch:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            batch_results = loop.run_until_complete(self._process_batch(batch))
            results.extend(self._format_results(batch, batch_results))

        return results

    def _format_results(self, batch: List[Dict], responses: List[Dict]) -> List[Dict]:
        """Format API responses with original data"""
        return [{
            "id": batch[i]["id"],
            "status": responses[i]["status"],
            "response": json.dumps(responses[i].get("data", responses[i].get("error"))),
            "success": str(responses[i]["success"])
        } for i in range(len(batch))]

    def send_data(self, df: DataFrame) -> DataFrame:
        """Main method to send DataFrame data to API"""
        results_rdd = df.rdd.mapPartitions(self._process_partition)
        return self.spark.createDataFrame(results_rdd, self.response_schema)


class APIIntegrationJob:
    """Complete API integration job controller"""

    def __init__(self):
        self.spark = self._create_spark_session()
        self.logger = logging.getLogger(self.__class__.__name__)

    def _create_spark_session(self) -> SparkSession:
        """Configure and create Spark session"""
        return SparkSession.builder \
            .appName("API Integration Job") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

    def run(self, input_df: DataFrame) -> Dict:
        """Execute the complete job"""
        # Configure authentication
        authenticator = OAuthAuthenticator(
            token_url="https://api.example.com/auth",
            client_id="your_client_id",
            client_secret="your_client_secret"
        )

        # Create API client
        api_client = AsyncAPIClient(
            base_url="https://api.example.com",
            authenticator=authenticator
        )

        # Create Spark integrator
        integrator = SparkAPIIntegrator(self.spark, api_client)

        # Process data
        results_df = integrator.send_data(input_df)

        # Analyze results
        results_df.createOrReplaceTempView("api_results")
        stats = self.spark.sql("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN success = 'True' THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN success = 'False' THEN 1 ELSE 0 END) as failure_count
            FROM api_results
        """).collect()[0].asDict()

        self.logger.info(f"Job completed: {stats}")

        if stats["failure_count"] > 0:
            self.spark.sql("""
                SELECT * FROM api_results 
                WHERE success = 'False'
            """).write.mode("overwrite").json("failed_requests")
            self.logger.info("Saved failed requests for reprocessing")

        return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Sample data - in production, read from source
    sample_data = [(1, {'A': 1}), (2, {'A': 2}), (3, {'A': 3})]
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("data", MapType(StringType(), IntegerType()))
    ])

    job = APIIntegrationJob()
    spark_df = job.spark.createDataFrame(sample_data, schema)

    # Run the job
    job_stats = job.run(spark_df)
    print("Job Statistics:", job_stats)