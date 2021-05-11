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

from mongo.mongo2elastic.base_streamer import BaseStreamer


class DataStreamer(BaseStreamer):

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