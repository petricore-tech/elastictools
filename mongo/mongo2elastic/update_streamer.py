#!/usr/bin/env python

import asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
import argparse
import logging
import sys
from collections import deque
from motor.motor_asyncio import AsyncIOMotorClient

from mongo.mongo2elastic.base_streamer import BaseStreamer


class UpdateStreamer(BaseStreamer):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.actions = deque(maxlen=self.batch_size)
        self.mutex = asyncio.Lock()

    async def __aenter__(self):
        await super().__aenter__()
        self.mongo_col = self.mongo_client[self.mongo_db][self.mongo_collection]
        return self

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
