import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dal.couchbase_dal import CouchbaseDataAccess
from logger_config import get_logger

logger = get_logger(__name__)

class CouchbaseService:

    def __init__(self, cb_dal: CouchbaseDataAccess):
        self.cb_dal = cb_dal

    def get_data(self, bucket_name: str):
        try:
            query = f"SELECT meta().id,* FROM `{bucket_name}`"
            return self.cb_dal.get_data(query)
        except Exception as e:
            logger.error(f"Error getting data from bucket {bucket_name}: {e}", exc_info=True)
            return None

    def check_index_status(self, bucket_name: str):
        try:
            return self.cb_dal.check_index_status(bucket_name)
        except Exception as e:
            logger.error(f"Error checking index status for bucket {bucket_name}: {e}", exc_info=True)
            return None

    def create_primary_index(self, bucket_name: str):
        try:
            exists, state = self.check_index_status(bucket_name)
            if not exists:
                self.cb_dal.create_primary_index(bucket_name)
            if not state:
                self.cb_dal.build_primary_index(bucket_name)
        except Exception as e:
            try:
                self.cb_dal.drop_primary_index(bucket_name)
                self.cb_dal.create_primary_index(bucket_name)
                self.cb_dal.build_primary_index(bucket_name)
                logger.info(f"Primary index created and built for bucket: {bucket_name.upper()}")
            except Exception as e:
                logger.error(f"Error creating primary index for bucket {bucket_name}: {e}", exc_info=True)
                raise e
            