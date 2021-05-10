#!/usr/bin/env python

import ujson
import asyncio
import aiofiles
from typing import Optional
import argparse
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
import logging
from copy import deepcopy
import sys
import os


class Loader:
    def __init__(
        self, 
        elastic_address: str, 
        index: str, 
        input_dir: str, 
        chunk_size: int,
        limit: Optional[int] = None, 
        connection_pool_size=5
    ):
        self.elastic_address = elastic_address
        self.index = index
        self.input_dir = input_dir
        self.chunk_size = chunk_size
        self.limit = limit
        self.connection_pool_size = connection_pool_size

        self.logger = logging.getLogger('Loader')
        self.logger.setLevel(logging.INFO)

    async def __aenter__(self):
        # create connection to elasticsearch
        es_host, es_port = self.elastic_address.split(':')
        es_port = int(es_port)
        self.es = AsyncElasticsearch(self.elastic_address)
        for _ in range(self.connection_pool_size - 1):
            self.es.transport.add_connection(dict(host=es_host, port=es_port))
            
        self.logger.addHandler(logging.StreamHandler(sys.stdout))
        return self

    async def __aexit__(self, *exc_info):
        await self.es.close()
        [h.close() for h in self.logger.handlers]

    async def start(self):
        await self.create_index_with_meta()
        await self.upload_data()

    async def create_index_with_meta(self):
        async with aiofiles.open(os.path.join(self.input_dir, 'settings.json'), 'r') as sf,\
                aiofiles.open(os.path.join(self.input_dir, 'mappings.json'), 'r') as mf:
            settings, mappings = await asyncio.gather(sf.readline(), mf.readline())
        
        settings = ujson.loads(settings)['index']
        settings.pop('routing')
        settings.pop('provided_name')
        settings.pop('creation_date')
        settings.pop('uuid')
        settings.pop('version')

        mappings = ujson.loads(mappings)
        res = await self.es.indices.create(self.index, body={"settings": settings, "mappings": mappings})
        self.logger.info('Index created sucessfully!' if res.get('acknowledged') else 'Index creation failed!')

    async def upload_data(self):
        inserts = []
        async with aiofiles.open(os.path.join(self.input_dir, 'data.json'), 'r') as f:
            actions, i = [], 0
            async for line in f:
                i += 1
                obj = ujson.loads(line)
                actions.append({"_index": self.index, "_id": str(obj['_id']), "_source": obj['_source']})
                if self.limit and i >= self.limit:
                    break
                if not i%self.chunk_size:
                    inserts.append(asyncio.create_task(async_bulk(self.es, deepcopy(actions))))
                    self.logger.info(f'{i} documents loaded')
                    actions.clear()
        if actions:
            inserts.append(asyncio.create_task(async_bulk(self.es, deepcopy(actions))))
        await asyncio.wait(inserts)
        self.logger.info('Data upload finished!')


async def main(args):
    async with Loader(
        elastic_address=args.elastic_address,
        index=args.index,
        input_dir=args.input_dir,
        chunk_size=args.chunk_size,
        limit=args.limit,
        connection_pool_size=args.connection_pool_size
    ) as loader:
        await loader.start()


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--elastic_address', type=str, required=True)
    parser.add_argument('--index', type=str, required=True)
    parser.add_argument('--input_dir', type=str, required=True)
    parser.add_argument('--limit', type=int, required=False, default=None)
    parser.add_argument('--chunk_size', type=int, required=False, default=500,\
        help='Insert `chunk_size` documents in a single bulk operation')
    parser.add_argument('--connection_pool_size', type=int, default=5)
    args = parser.parse_args()

    asyncio.run(main(args))


if __name__ == '__main__':
    run()
