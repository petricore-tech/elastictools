import asyncio
import websockets
import argparse
from motor.motor_asyncio import AsyncIOMotorClient
import logging
import sys
import ujson
from pymongo import UpdateOne, DeleteOne, InsertOne
from copy import deepcopy
from bson import ObjectId

from mongo.elastic2mongo.base_streamer import BaseStreamer


class UpdateStreamer(BaseStreamer):

    def __init__(self, elastic_updates_uri: str, *args, **kwargs):
        """
        Args:
            elastic_updates_uri (str): Websocket address where changes are pushed to by means of `es-change-feed-plugin`
            More info at https://github.com/ForgeRock/es-change-feed-plugin
            Example: `ws://localhost:9400/ws/_changes`
        """
        super().__init__(*args, **kwargs)
        self.elastic_updates_uri = elastic_updates_uri
        self.updates = []
        self.mutex = asyncio.Lock()

    async def __aenter__(self):
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
        self.mongo_client.close()
        for handler in self.logger.handlers:
            handler.close()

    async def run(self):
        self.logger.info("Listening elasticsearch updates...")
        asyncio.create_task(self.periodic_push(period=1))
        async with websockets.connect(self.elastic_updates_uri) as websocket:
            async for message in websocket:
                asyncio.create_task(self.handle_change(message))

    async def handle_change(self, change: str):
        change = ujson.loads(change)
        if change['_index'] == self.elastic_index:
            operation = change['_operation']
            _id = ObjectId(change['_id']) if ObjectId.is_valid(change['_id']) else change['_id']
            if operation == 'CREATE':
                mongo_op = InsertOne(dict(**change['_source'], _id=_id))
            elif operation == 'INDEX':
                mongo_op = UpdateOne({'_id': _id}, {'$set': change['_source']}, upsert=True)
            elif operation == 'DELETE':
                mongo_op = DeleteOne({'_id': _id})
            else:
                return
            async with self.mutex:
                self.updates.append(mongo_op)
                if len(self.updates) == self.batch_size:
                    asyncio.create_task(self.write_to_db(deepcopy(self.updates)))
                    self.updates.clear()

    async def periodic_push(self, period=1):
        while True:
            await asyncio.sleep(period)
            async with self.mutex:
                if self.updates:
                    asyncio.create_task(self.write_to_db(deepcopy(self.updates)))
                    self.updates.clear()

    async def write_to_db(self, updates: list):
        res = await self.mongo_col.bulk_write(updates)
        self.logger.info(f"Bulk transaction result: {res.bulk_api_result}")


async def main(args):
    async with UpdateStreamer(
        elastic_updates_uri=args.elastic_updates_uri,
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
    parser.add_argument('--elastic_updates_uri', type=str, required=True,\
        help='Websocket address where changes are pushed to by means of `es-change-feed-plugin`\
            More info at https://github.com/ForgeRock/es-change-feed-plugin')
    args = parser.parse_args()

    asyncio.run(main(args))


if __name__ == "__main__":
    run()