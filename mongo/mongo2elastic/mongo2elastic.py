import logging
import sys
import argparse
import asyncio

from mongo.mongo2elastic.data_streamer import DataStreamer
from mongo.mongo2elastic.update_streamer import UpdateStreamer


class Mongo2ElasticStreamer:
    
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
        self.data_streamer = DataStreamer(
            mongo_address,
            mongo_db,
            mongo_collection,
            elastic_address,
            elastic_index,
            batch_size,
            connection_pool_size
        )
        self.update_streamer = UpdateStreamer(
            mongo_address,
            mongo_db,
            mongo_collection,
            elastic_address,
            elastic_index,
            batch_size,
            connection_pool_size
        )
        self.logger = logging.getLogger('Mongo2Elastic')
        self.logger.setLevel(logging.INFO)

    async def __aenter__(self):
        # add handlers for logger
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)
        return self

    async def __aexit__(self, *exc_info):
        for handler in self.logger.handlers:
            handler.close()

    async def run(self):
        async with self.data_streamer:
            self.logger.info('Streaming existing data...')
            await self.data_streamer.run()
            self.logger.info('Existing data streamed sucessfully')
        async with self.update_streamer:
            self.logger.info('Streaming updates...')
            await self.update_streamer.run()


async def main(args):
    async with Mongo2ElasticStreamer(
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


if __name__ == "__main__":
    run()
