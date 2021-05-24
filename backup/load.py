#!/usr/bin/env python

import ujson
import asyncio
import aiofiles
import argparse
from elasticsearch.helpers import async_bulk
from copy import deepcopy
import os

from backup.base import BaseBackupWorker


class Loader(BaseBackupWorker):
    def __init__(self, mode: str, input_dir: str, connection_pool_size=5, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_dir = input_dir
        self.connection_pool_size = connection_pool_size
        self.mode = mode

    async def __aenter__(self):
        # create connection to elasticsearch
        await super().__aenter__()
        es_host, es_port = self.elastic_address.split(':')
        es_port = int(es_port)
        for _ in range(self.connection_pool_size - 1):
            self.es.transport.add_connection(dict(host=es_host, port=es_port))
        return self

    async def start(self):
        if self.mode == 'default':
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
        body = {"settings": settings, "mappings": mappings}

        res = await self.es.indices.create(self.index, body=body)
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
        connection_pool_size=args.connection_pool_size,
        mode=args.mode
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
    parser.add_argument('--mode', choices=['data', 'default'], required=False, default='default')
    args = parser.parse_args()

    asyncio.run(main(args))


if __name__ == '__main__':
    run()
