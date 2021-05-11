from abc import ABCMeta, abstractmethod
import logging


class AbstractStreamer(object, metaclass=ABCMeta):
    
    @abstractmethod
    def __init__(
        self,
        mongo_address: str,
        mongo_db: str,
        mongo_collection: str,
        elastic_address: str,
        elastic_index: str,
        batch_size: int = 500
    ):
        self.mongo_address = mongo_address
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.elastic_address = elastic_address
        self.elastic_index = elastic_index
        self.batch_size = batch_size

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    @abstractmethod
    async def run(self):
        pass

    @abstractmethod
    async def __aenter__(self):
        pass

    @abstractmethod
    async def __aexit__(self, *exc_info):
        pass
