from mongo.abstract import AbstractStreamer
from elasticsearch import AsyncElasticsearch
from motor.motor_asyncio import AsyncIOMotorClient
import logging
import sys


class BaseStreamer(AbstractStreamer):

    def __init__(self, connection_pool_size: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection_pool_size = connection_pool_size

    async def __aenter__(self):
        # create connection to elasticsearch
        es_host, es_port = self.elastic_address.split(':')
        es_port = int(es_port)
        self.es = AsyncElasticsearch(self.elastic_address)
        for _ in range(self.connection_pool_size - 1):
            self.es.transport.add_connection(dict(host=es_host, port=es_port))

        # create connection to mongodb
        self.mongo_client = AsyncIOMotorClient(self.mongo_address)

        # add handlers for logger
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)

    async def __aexit__(self, *exc_info):
        await self.es.close()
        self.mongo_client.close()
        for handler in self.logger.handlers:
            handler.close()
