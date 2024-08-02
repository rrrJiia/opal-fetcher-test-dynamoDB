from pydantic import BaseModel, Field
from typing import Optional, List, Dict
import boto3
from boto3.dynamodb.conditions import Key
from opal_common.fetcher.fetch_provider import BaseFetchProvider
from opal_common.fetcher.events import FetcherConfig, FetchEvent
from cachetools import TTLCache
import logging
import redis
import threading
import asyncio
import time
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DynamoDBFetcherConfig(FetcherConfig):
    fetcher: str = "DynamoDBFetchProvider"
    table_name: str = Field(
         ..., 
         description="The name of the DynamoDB table"
    )
    region_name: str = Field(
        "us-east-1", 
        description="The AWS region of the DynamoDB table"
    )
    fetch_one: bool = Field(
        False,
        description="whether we fetch only one row from the results of the SELECT query",
    )
    fetch_key: str = Field(
        None,
        description="column name to use as key to transform the data to Object format rather than list/array",
    )

class DynamoDBFetchEvent(FetchEvent):
    fetcher: str = "DynamoDBFetchProvider"
    config: DynamoDBFetcherConfig = None

class DynamoDBFetchProvider(BaseFetchProvider):
    # Create a cache with a maximum of 100 items and a TTL of 300 seconds (5 minutes)
    cache = TTLCache(maxsize=100, ttl=300)  

    def __init__(self, event: DynamoDBFetchEvent) -> None:
        super().__init__(event)
        self._event = event
        config = self._event.config
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=config.region_name
        )
        self.table = self.dynamodb.Table(config.table_name)

        # Initialize Redis client
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'redis'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=0
        )

        # Initialize rivian_id with a default value
        self.rivian_id = os.getenv('RIVIAN_ID', 'default_rivian_id')

        # Start a thread to periodically update rivian_id from Redis
        self.update_rivian_id_thread = threading.Thread(target=self.update_rivian_id, daemon=True)
        self.update_rivian_id_thread.start()

    def update_rivian_id(self):
        while True:
            try:
                # Fetch the rivian_id from Redis
                rivian_id = self.redis_client.get('rivian_id')
                if rivian_id:
                    self.rivian_id = rivian_id.decode('utf-8')
                    logger.info(f"Updated RIVIAN_ID to: {self.rivian_id}")
                else:
                    logger.warning("RIVIAN_ID not found in Redis, using default.")
            except redis.RedisError as e:
                logger.error(f"Redis error: {e}")

            time.sleep(10)  # Update every 10 seconds

    def parse_event(self, event: FetchEvent) -> DynamoDBFetchEvent:
        return DynamoDBFetchEvent(**event.dict(exclude={"config"}), config=event.config)

    async def _fetch_(self):
        cache_key = f"{self._event.config.table_name}:{self.rivian_id}"
        if cache_key in self.cache:
            logger.info(f"Cache hit for key: {cache_key}")
            return self.cache[cache_key]

        try:
            if self._event.config.fetch_one:
                response = self.table.query(
                    KeyConditionExpression=Key('rivian_id').eq(self.rivian_id),
                    Limit=1
                )
            else:
                response = self.table.query(
                    KeyConditionExpression=Key('rivian_id').eq(self.rivian_id)
                )
            data = response['Items']
            self.cache[cache_key] = data
            return data
        except Exception as e:
            logger.error(f"Error fetching data from DynamoDB: {e}")
            return None

    async def _process_(self, records: List[Dict]):
        self._event: DynamoDBFetchEvent  # type casting

        if self._event.config.fetch_one:
            if records and len(records) > 0:
                return records[0]
            else:
                return {}
        else:
            if self._event.config.fetch_key is None:
                return records
            else:
                res_dct = map(lambda i: (records[i][self._event.config.fetch_key], records[i]), range(len(records)))
                return dict(res_dct)