#!/usr/bin/env python

import asyncio
from elasticsearch import AsyncElasticsearch
import argparse
import logging
import sys
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId


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
        tasks = []
        match_all = {
            "size": self.batch_size,
            "query": {
                "match_all": {}
            }
        }
        scroll_id = None

        while True:
            # We obtain scroll_id on the first run, then use it to get the next page of results and a new scroll_id
            scroll_response = await self.es.search(index=self.elastic_index, body=match_all, scroll='2s')\
                if not scroll_id else await self.es.scroll(scroll_id=scroll_id, scroll='2s')
            scroll_id = scroll_response['_scroll_id']

            documents = scroll_response['hits']['hits']
            if not documents:
                break
            documents = [dict(**d['_source'], _id=ObjectId(d['_id']) if ObjectId.is_valid(d['_id']) else d['_id'])\
                for d in documents]
            tasks.append(asyncio.create_task(self.insert_to_mongo(documents)))
        tasks.append(asyncio.create_task(self.es.clear_scroll(scroll_id=scroll_id)))
        await asyncio.wait(tasks)
        
    async def insert_to_mongo(self, documents: list):
        result = await self.mongo_client[self.mongo_db][self.mongo_collection].insert_many(documents)
        self.logger.info(f'Bulk insert result: {result}')

    
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


if __name__ == "__main__":
    run()
