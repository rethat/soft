from dal.mongodb_dal import MongoDBDataAccess
from logger_config import get_logger

logger = get_logger(__name__)

class MongoDBService:

    def __init__(self,  mongo_dal: MongoDBDataAccess = None, mapping_id: bool = False):
        self.mongo_dal = mongo_dal
        self.mapping_id = mapping_id

    def add_document(self, collection_name: str, document: dict):
        try:
            self.mongo_dal.add_document(collection_name, document)
        except Exception as e:
            logger.error(f"Error adding document to MongoDB: {e}", exc_info=True)
            raise e

    def add_documents(self, collection_name: str, documents: list):
        try:
            _documents = []
            if self.mapping_id:
                for doc in documents:
                    if isinstance(doc, dict):
                        if 'id' in doc and 'value' in doc:
                            document = doc['value'].copy()
                            document['_id'] = doc['id']
                            del document['id']
                        elif 'id' in doc:
                            document = doc.copy()
                            document['_id'] = doc['id']
                            del document['id']
                        else:
                            document = doc.copy()
                        _documents.append(document)
            else:
                _documents = documents
            self.mongo_dal.add_documents(collection_name, _documents)
            logger.info(f"Successfully added {len(documents)} documents to MongoDB collection {collection_name}")
        except Exception as e:
            logger.error(f"Error adding documents to MongoDB: {e}", exc_info=True)
            raise e

    