# provider.py
from typing import Optional, List, Dict
from pydantic import BaseModel, Field
import boto3
from boto3.dynamodb.conditions import Key
from opal_common.fetcher.fetch_provider import BaseFetchProvider
from opal_common.fetcher.events import FetcherConfig, FetchEvent
from cachetools import TTLCache
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

current_config = {"rivian_id": None}  # Shared state for configuration

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
    rivian_id: Optional[str] = Field(
        None,  # Changed to None to allow runtime input
        description="The distinct key for fetching data"
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
        self.update_config()

    def update_config(self):
        global current_config
        config = self._event.config
        config.rivian_id = current_config.get('rivian_id', config.rivian_id)
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=config.region_name
        )
        self.table = self.dynamodb.Table(config.table_name)

    def parse_event(self, event: FetchEvent) -> DynamoDBFetchEvent:
        return DynamoDBFetchEvent(**event.dict(exclude={"config"}), config=event.config)

    async def _fetch_(self):
        self.update_config()  # Ensure the latest config is used
        config = self._event.config
        if not config.rivian_id:
            logger.error("rivian_id is required but not provided.")
            return {"error": "rivian_id is required but not provided."}

        cache_key = f"{config.table_name}:{config.rivian_id}"
        if cache_key in self.cache:
            logger.info(f"Cache hit for key: {cache_key}")
            return self.cache[cache_key]

        try:
            if config.fetch_one:
                response = self.table.query(
                    KeyConditionExpression=Key('rivian_id').eq(config.rivian_id),
                    Limit=1
                )
            else:
                response = self.table.query(
                    KeyConditionExpression=Key('rivian_id').eq(config.rivian_id)
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
