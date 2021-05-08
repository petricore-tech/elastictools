#!/usr/bin/env python

import asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
import argparse
import logging
import sys
from collections import deque
from motor.motor_asyncio import AsyncIOMotorClient
from copy import deepcopy


class DataStreamer:
    def __init__(
        self, 
        mongo_address, 
        mongo_db, 
        mongo_collection, 
        elastic_address, 
        elastic_index, 
        batch_size=500
    ):
        self.mongo_address = mongo_address
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.elastic_address = elastic_address
        self.elastic_index = elastic_index
        self.batch_size = batch_size

        self.logger = logging.getLogger('DataStreamer')
        self.logger.setLevel(logging.INFO)

    async def __aenter__(self):
        self.es = AsyncElasticsearch(hosts=self.elastic_address)
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

    async def run(self):
        col = self.mongo_client[self.mongo_db][self.mongo_collection]

        cursor = col.find()
        actions = []
        tasks = []
        async for doc in cursor:
            _id = doc.pop('_id')
            actions.append({
                "_index": self.elastic_index,
                "_id": str(_id),
                "_source": doc
            })
            if len(actions) == self.batch_size:
                tasks.append(asyncio.create_task(self.stream_batch(deepcopy(actions))))
                actions.clear()
        if actions:
            tasks.append(asyncio.create_task(self.stream_batch(deepcopy(actions))))
        await asyncio.wait(tasks)

    async def stream_batch(self, batch: list):
        result = await async_bulk(self.es, batch)
        self.logger.info(f"Periodic bulk transaction result: {result}")
            

async def main(args):
    async with DataStreamer(
        mongo_address=args.mongo_address,
        mongo_db=args.mongo_db,
        mongo_collection=args.mongo_collection,
        elastic_address=args.elastic_address,
        elastic_index=args.elastic_index,
        batch_size=args.batch_size
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
    args = parser.parse_args()

    asyncio.run(main(args))

if __name__ == '__main__':
    run()