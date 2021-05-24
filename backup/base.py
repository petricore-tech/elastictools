import asyncio
from elasticsearch import AsyncElasticsearch
import aiofiles
from typing import Optional
import ujson
import os
import argparse
import logging
import sys
from abc import ABCMeta, abstractmethod


class BaseBackupWorker(object, metaclass=ABCMeta):

    def __init__(
        self, 
        elastic_address: str, 
        index: str, 
        chunk_size: int,
        limit: Optional[int] = None
    ):
        self.elastic_address = elastic_address
        self.index = index
        self.chunk_size = chunk_size
        self.limit = limit

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    async def __aenter__(self):
        self.es = AsyncElasticsearch(hosts=self.elastic_address)
        self.logger.addHandler(logging.StreamHandler(sys.stdout))
        return self

    async def __aexit__(self, *exc_info):
        await self.es.close()
        [h.close() for h in self.logger.handlers]

    @abstractmethod
    async def start(self):
        pass
    