import gc
import os
import sys
from typing import final
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
    
    def get_total_count(self, bucket_name: str):
        try:
            total_count = self.cb_dal.get_total_count(bucket_name)
            return total_count
        except Exception as e:
            logger.error(f"Failed to get total count for bucket {bucket_name}: {e}", exc_info=True)
            logger.error(f"This may indicate that the primary index is not available or the bucket is not accessible.")
            raise e

    def get_data_paginated(self, bucket_name: str, page: int, page_size:int, max_retries:int = 3, retry_delay:int = 2):
        try:
            offset = page * page_size
            page_data = self.cb_dal.get_data_paginated(
                bucket_name=bucket_name,
                page_size=page_size,
                offset=offset,
                max_retries=max_retries,
                retry_delay=retry_delay
            )
            if not page_data or len(page_data) == 0:
                logger.info(f"{bucket_name.upper()} Page {page} (offset {offset}): NO_DATA_FOUND")
                return None
            return page_data
        except Exception as e:
            logger.error(f"Error getting data from bucket {bucket_name}: {e}", exc_info=True)
            return None
        finally:
            self.cb_dal.close()
            gc.collect()