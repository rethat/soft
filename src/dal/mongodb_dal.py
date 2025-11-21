import os
import sys
import gc

from pymongo.common import MAX_IDLE_TIME_MS
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
            # connection_string = f"mongodb+srv://{self.config.user}:{self.config.password}@{self.config.host}:{self.config.port}?authMechanism=SCRAM-SHA-256&retryWrites=false"
            if self.config.tls == "true":
                import certifi as cert
                self.client = MongoClient(connection_string, tls=True, tlsCAFile= cert.where(), serverSelectionTimeoutMS=30000, maxIdleTimeMS=120000)
            else:
                self.client = MongoClient(connection_string, serverSelectionTimeoutMS=30000, maxIdleTimeMS=120000)
            self.database = self.client[self.config.database]
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

    def drop_collections(self):
        try:
            self.connect()
            collection_names = self.database.list_collection_names()
            for collection in collection_names:
                # if collection.startswith("rms_"):
                self.database[collection].drop()
            logger.info(f"Successfully dropped {len(collection_names)} collections from MongoDB")
        except PyMongoError as e:
            logger.error(f"Error dropping collection from MongoDB: {e}", exc_info=True)
            raise e
        except Exception as e:
            logger.error(f"Unexpected error dropping collection from MongoDB: {e}", exc_info=True)
            raise e
        finally:
            self.close()
            gc.collect()