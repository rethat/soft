
import os
import sys
import gc

from pymongo.common import MAX_IDLE_TIME_MS
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import MongoDBConfig
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import BulkWriteError, ConnectionFailure, PyMongoError, ServerSelectionTimeoutError

from logger_config import get_logger
logger = get_logger(__name__)

class MongoDBDataAccess:

    def __init__(self, config: MongoDBConfig):
        self.config = config
        self.client = None
        self.database = None

    def connect(self):
        try:
            connection_string = self.config.connection_string
            if self.config.tls == "true":
                import certifi as cert
                self.client = MongoClient(
                    connection_string, 
                    tls=True, 
                    tlsCAFile= cert.where(), 
                    serverSelectionTimeoutMS=30000, 
                    maxIdleTimeMS=120000
                )
            else:
                self.client = MongoClient(
                    connection_string, 
                    serverSelectionTimeoutMS=30000, 
                    maxIdleTimeMS=120000
                )
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
            # Removed gc.collect() to prevent debugger hang
            logger.info("Closed MongoDB connection")

    
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
            # Removed gc.collect() to prevent debugger hang

    def add_documents(self, collection_name: str, documents: list, max_retries: int = 5, retry_delay: int = 3):
        import time
        last_error = None
        for attempt in range(max_retries):
            try:
                self.close() # close previous connection since retry
                self.connect()
                self.database[collection_name].insert_many(documents, ordered=False)
                return
            except BulkWriteError as bwe:
                write_errors = bwe.details.get('writeErrors', [])
                failed_indexes = {err['index'] for err in write_errors}
                for i in failed_indexes:
                    retry_doc = documents[i].copy()
                    retry_doc.pop("_id", None)
                    try:
                        self.database[collection_name].insert_one(retry_doc)
                    except PyMongoError as e:
                        logger.error(f"FAILE_DOCUMENT_INSERT: {collection_name}|{documents[i]}|Error:{e}", exc_info=True)
                        continue
                
                # duplicate_errors = [error for error in write_errors if error.get('code') == 11000]
                # if duplicate_errors:
                #     logger.warning(f"Duplicate key violation iggnored for {collection_name}. {len(duplicate_errors)} duplicates found.")
                #     dup_indexes = {err['index'] for err in duplicate_errors}
                #     dup_docs = [doc for idx, doc in enumerate(documents) if idx not in dup_indexes]
                #     if not dup_docs:
                #         return # all docs duplicated
                #     logger.info(f"Retrying insert without duplicates ({len(dup_docs)} documents remaining)")
                #     time.sleep(retry_delay)
                #     continue
                # else:
                #     logger.error(f"Bulk write error for {collection_name}: {bwe.details}", exc_info=True)
                #     raise bwe
            except (PyMongoError, ConnectionFailure, ServerSelectionTimeoutError) as e:
                last_error = e
                is_ssl_error = self._is_ssl_error(e)
                if is_ssl_error:
                    logger.warning(f"SSL/Connection error. Attempt {attempt + 1}/{max_retries})"
                                   f"{collection_name}. Error: {e}")
                else:
                    logger.warning(f"MongoDB error on attempt {attempt + 1}/{max_retries}"
                    f"Collection: {collection_name}. Error: {e}", exc_info=True)
                if attempt < max_retries - 1:
                    delay = retry_delay * (2**attempt)
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"=" * 90)
                    logger.error(
                        f"Failed to insert documents after {max_retries} attempts for {collection_name}. Error: {last_error}",
                        exc_info=True
                    )
                    logger.error(f"=" * 90)
                    raise e
            except Exception as e:
                last_error = e
                logger.error(f"Unexpected error adding documents to MongoDB: {e}", exc_info=True)
                raise e
            finally:
                if attempt < max_retries - 1 or last_error:
                    self.close()
                # Removed gc.collect() to prevent debugger hang

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
            # Removed gc.collect() to prevent debugger hang

    def _is_ssl_error(self, error):
        error_str = str(error).lower()
        ssl_indicators = [
            "ssl",
            "eof occurred in violation of protocol",
            "connection reset",
            "broken pipe",
            "connection aborted",
            "time out"
        ]
        return any(indicator in error_str for indicator in ssl_indicators)

    def document_exists(self, collection_name: str, document_id: str):
        """
        Check if a document with the given _id exists in the collection.
        
        Args:
            collection_name: Name of the collection
            document_id: Document _id to check
            
        Returns:
            bool: True if document exists, False otherwise
        """
        try:
            self.connect()
            result = self.database[collection_name].find_one({"_id": document_id})
            return result is not None
        except PyMongoError as e:
            logger.error(f"Error checking document existence in MongoDB: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error checking document existence: {e}", exc_info=True)
            return False
        finally:
            self.close()
            # Removed gc.collect() to prevent debugger hang