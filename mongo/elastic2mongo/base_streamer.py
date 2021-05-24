from mongo.abstract import AbstractStreamer
from elasticsearch import AsyncElasticsearch
from motor.motor_asyncio import AsyncIOMotorClient
import logging
import sys


class BaseStreamer(AbstractStreamer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def __aenter__(self):
        # create connection to elasticsearch
        self.es = AsyncElasticsearch(self.elastic_address)

        # create connection to mongodb
        self.mongo_client = AsyncIOMotorClient(self.mongo_address)

        # add handlers for logger
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)
        return self

    async def __aexit__(self, *exc_info):
        await self.es.close()
        self.mongo_client.close()
        for handler in self.logger.handlers:
            handler.close()
