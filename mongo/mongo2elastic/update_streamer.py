#!/usr/bin/env python

import asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
import argparse
import logging
import sys
from collections import deque
from motor.motor_asyncio import AsyncIOMotorClient


class UpdateStreamer:
    
    def __init__(
        self, 
        mongo_address, 
        mongo_db, 
        mongo_collection, 
        elastic_address, 
        elastic_index, 
        batch_size=500,
        connection_pool_size=5
    ):
        self.mongo_address = mongo_address
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.elastic_address = elastic_address
        self.elastic_index = elastic_index
        self.connection_pool_size = connection_pool_size

        self.logger = logging.getLogger('UpdateStreamer')
        self.logger.setLevel(logging.INFO)

        self.actions = deque(maxlen=batch_size)
        self.mutex = asyncio.Lock()

    async def __aenter__(self):
        # create connection to elasticsearch
        es_host, es_port = self.elastic_address.split(':')
        es_port = int(es_port)
        self.es = AsyncElasticsearch(self.elastic_address)
        for _ in range(self.connection_pool_size - 1):
            self.es.transport.add_connection(dict(host=es_host, port=es_port))

        # create connection to mongodb
        self.mongo_client = AsyncIOMotorClient(self.mongo_address)
        self.mongo_col = self.mongo_client[self.mongo_db][self.mongo_collection]

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

    async def handle_change(self, change: dict):
        if change['operationType'] == 'delete':
            _id = change.get('documentKey', {}).get('_id')
            self.logger.info(f"New delete {_id}")
            res = await self.es.delete(index=self.elastic_index, id=_id)
        else:
            if change['operationType'] not in ['insert', 'replace', 'update']:
                return
            doc = change.get('fullDocument')
            try:
                self.logger.info(f"New {change['operationType']} {doc}")
                _id = doc.pop('_id')
                action = {
                    "_index": self.elastic_index,
                    "_id": str(_id),
                    "_source": doc
                }
                async with self.mutex:
                    self.actions.append(action)
                    if len(self.actions) == self.actions.maxlen:
                        res = await async_bulk(self.es, self.actions)
                        self.actions.clear()
                        self.logger.info(f"Bulk transaction result: {res}")
            except Exception as e:
                self.logger.error(e)

    async def periodic_push(self, period=1):
        while True:
            await asyncio.sleep(period)
            if self.actions:
                async with self.mutex:
                    res = await async_bulk(self.es, self.actions)
                    self.actions.clear()
                    self.logger.info(f"Periodic bulk transaction result: {res}")

    async def run(self):
        async with self.mongo_col.watch(full_document='updateLookup') as stream:
            self.logger.info("Listening mongo updates...")
            asyncio.create_task(self.periodic_push(period=1))
            async for change in stream:
                asyncio.create_task(self.handle_change(change))


async def main(args):
    async with UpdateStreamer(
        mongo_address=args.mongo_address,
        mongo_db=args.mongo_db,
        mongo_collection=args.mongo_collection,
        elastic_address=args.elastic_address,
        elastic_index=args.elastic_index,
        batch_size=args.batch_size,
        connection_pool_size=args.connection_pool_size
    ) as streamer:
        await streamer.run()


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mongo_address', type=str, required=True)
    parser.add_argument('--mongo_db', type=str, required=True)
    parser.add_argument('--mongo_collection', type=str, required=True)
    parser.add_argument('--elastic_address', type=str, required=True)
    parser.add_argument('--elastic_index', type=str, required=True)
    parser.add_argument('--batch_size', type=int, default=500)
    parser.add_argument('--connection_pool_size', type=int, default=5)
    args = parser.parse_args()

    asyncio.run(main(args))

if __name__ == '__main__':
    run()
