import os
import sys
import gc
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import MongoDBConfig
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, PyMongoError

from logger_config import get_logger
logger = get_logger(__name__)

class MongoDBDataAccess:

    def __init__(self, config: MongoDBConfig):
        self.config = config
        self.client = None
        self.database = None

    def connect(self):
        try:
            connection_string = f"mongodb://{self.config.user}:{self.config.password}@{self.config.host}:{self.config.port}"
            self.client = MongoClient(connection_string, serverSelectionTimeoutMS=30000)
            self.database = self.client[self.config.db]
            return self.client
        except ConnectionFailure as e:
            logger.error(f"Error connecting to MongoDB: {e}", exc_info=True)
            raise e
        except Exception as e:
            logger.error(f"Unexpected error connecting to MongoDB: {e}", exc_info=True)
            raise e

    def close(self):
        if self.client:
            self.client.close()
            self.client = None
            logger.info("Closing MongoDB connection")
            gc.collect()

    
    def add_document(self, collection_name: str, document: dict):
        try:
            self.connect()
            self.database[collection_name].insert_one(document)
        except PyMongoError as e:
            logger.error(f"Error adding document to MongoDB: {e}", exc_info=True)
            raise e
        except Exception as e:
            logger.error(f"Unexpected error adding document to MongoDB: {e}", exc_info=True)
            raise e
        finally:
            self.close()
            gc.collect()

    def add_documents(self, collection_name: str, documents: list):
        try:
            self.connect()
            self.database[collection_name].insert_many(documents)
        except PyMongoError as e:
            logger.error(f"Error adding documents to MongoDB: {e}", exc_info=True)
            raise e
        except Exception as e:
            logger.error(f"Unexpected error adding documents to MongoDB: {e}", exc_info=True)
            raise e
        finally:
            self.close()
            gc.collect()
