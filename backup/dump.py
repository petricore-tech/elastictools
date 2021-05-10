#!/usr/bin/env python

import asyncio
from elasticsearch import AsyncElasticsearch
import aiofiles
from typing import Optional
import ujson
import os
import argparse
import logging
import sys


class Dumper:

    def __init__(
        self, 
        elastic_address: str, 
        index: str, 
        output_dir: str, 
        chunk_size: int,
        limit: Optional[int] = None
    ):
        self.elastic_address = elastic_address
        self.index = index
        self.output_dir = output_dir
        self.chunk_size = chunk_size
        self.limit = limit

        self.logger = logging.getLogger('Dumper')
        self.logger.setLevel(logging.INFO)

    async def __aenter__(self):
        self.es = AsyncElasticsearch(hosts=self.elastic_address)
        self.logger.addHandler(logging.StreamHandler(sys.stdout))
        return self

    async def __aexit__(self, *exc_info):
        await self.es.close()
        [h.close() for h in self.logger.handlers]

    async def dump_meta(self):
        meta = (await self.es.indices.get(self.index))[self.index]
        async with aiofiles.open(os.path.join(self.output_dir, 'mappings.json'), 'w') as mf, \
                aiofiles.open(os.path.join(self.output_dir, 'settings.json'), 'w') as sf:
            await asyncio.gather(
                mf.write(ujson.dumps(meta['mappings'])),
                sf.write(ujson.dumps(meta['settings']))
            )

    async def dump_data(self):
        count = self.limit or int((await self.es.cat.count(index=self.index)).strip().split()[-1])

        async with aiofiles.open(os.path.join(self.output_dir, 'data.json'), 'w') as f:
            tasks = []

            dumped_count = 0
            match_all = {
                "size": self.chunk_size,
                "query": {
                    "match_all": {}
                }
            }
            scroll_response = await self.es.search(index=self.index, body=match_all, scroll='2s')

            # write hits into file
            num_to_insert = min(self.limit - dumped_count, len(scroll_response['hits']['hits']))\
                if self.limit else len(scroll_response['hits']['hits'])
            documents = list(map(lambda x: f'{ujson.dumps(x)}\n',\
                scroll_response['hits']['hits'][:num_to_insert]))
            tasks.append(asyncio.create_task(f.writelines(documents)))
            
            # set scroll_id and update `dumped_count`
            scroll_id = scroll_response['_scroll_id']
            dumped_count += num_to_insert
            self.logger.info(f'{dumped_count} documents dumped')

            while dumped_count < count:
                scroll_response = await self.es.scroll(scroll_id=scroll_id, scroll='2s')
                # write hits into file
                num_to_insert = min(self.limit - dumped_count, len(scroll_response['hits']['hits']))\
                    if self.limit else len(scroll_response['hits']['hits'])

                documents = list(map(lambda x: f'{ujson.dumps(x)}\n',\
                    scroll_response['hits']['hits'][:num_to_insert]))
                tasks.append(asyncio.create_task(f.writelines(documents)))
                
                # set scroll_id and update `dumped_count`
                scroll_id = scroll_response['_scroll_id']
                dumped_count += num_to_insert

                self.logger.info(f'{dumped_count} documents dumped')
            tasks.append(asyncio.create_task(self.es.clear_scroll(scroll_id=scroll_id)))
            await asyncio.wait(tasks)
                    
    async def start(self):
        meta_task = asyncio.create_task(self.dump_meta())
        data_task = asyncio.create_task(self.dump_data())
        await asyncio.wait([meta_task, data_task])


async def main(args):
    async with Dumper(
        elastic_address=args.elastic_address, 
        index=args.index,
        output_dir=args.output_dir,
        chunk_size=args.chunk_size,
        limit=args.limit
    ) as dumper:
        await dumper.start()


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--elastic_address', type=str, required=True)
    parser.add_argument('--index', type=str, required=True)
    parser.add_argument('--output_dir', type=str, required=True)
    parser.add_argument('--chunk_size', type=int, required=False, default=500,\
        help='Get `chunk_size` documents from elasticsearch in a single request')
    parser.add_argument('--limit', type=int, required=False, default=None)
    args = parser.parse_args()

    asyncio.run(main(args))


if __name__ == "__main__":
    run()
