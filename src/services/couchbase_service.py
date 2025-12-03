import gc
import os
import sys
import json
from typing import final
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dal.couchbase_dal import CouchbaseDataAccess
from logger_config import get_logger
from couchbase.exceptions import BucketNotFoundException

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

    def create_primary_index(self, bucket_name: str, force: bool = False):
        try:
            if force:
                self.cb_dal.drop_primary_index(bucket_name)
                self.cb_dal.create_primary_index(bucket_name)
                self.cb_dal.build_primary_index(bucket_name)
            else:
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
    
    def export_data_to_json(self, bucket_name: str, data: list, file_path: str = None, append: bool = True):
        """
        Export data to JSON file. Data from the same bucket will be written to the same file.
        
        Args:
            bucket_name: Name of the bucket
            data: List of documents to export
            file_path: Optional custom file path. If not provided, uses {bucket_name}.json
            append: If True, append to existing file. If False, overwrite existing file.
        
        Returns:
            str: Path to the JSON file
        """
        try:
            if file_path is None:
                file_path = f"{bucket_name}.json"
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else '.', exist_ok=True)
            
            if append and os.path.exists(file_path):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                        if not isinstance(existing_data, list):
                            existing_data = [existing_data]
                        # Combine existing data with new data
                        all_data = existing_data + data
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Could not read existing JSON file {file_path}, starting fresh: {e}")
                    all_data = data
            else:
                # Overwrite mode or file doesn't exist
                all_data = data
            
            # Write to file
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Exported {len(data)} documents to {file_path} (total: {len(all_data)} documents)")
            return file_path
        except Exception as e:
            logger.error(f"Error exporting data to JSON for bucket {bucket_name}: {e}", exc_info=True)
            raise e
    
    def check_bucket_exists(self, bucket_name: str):
        """
        Check if a bucket exists in Couchbase cluster.
        
        Args:
            bucket_name: Name of the bucket to check
        
        Returns:
            bool: True if bucket exists, False otherwise
        """
        try:
            return self.cb_dal.check_bucket_exists(bucket_name)
        except Exception as e:
            logger.error(f"Error checking bucket {bucket_name}: {e}", exc_info=True)
            return False
    
    def create_bucket(self, bucket_name: str, ram_quota_mb: int = 100, 
                     wait_ready: bool = True, wait_timeout: int = 30):
        """
        Create a new bucket in Couchbase cluster if it doesn't exist.
        
        Args:
            bucket_name: Name of the bucket to create
            ram_quota_mb: RAM quota in MB (default: 100)
            wait_ready: Wait for bucket to be ready after creation (default: True)
            wait_timeout: Timeout in seconds for waiting bucket to be ready (default: 30)
        
        Returns:
            bool: True if bucket exists or was created successfully, False otherwise
        """
        try:
            return self.cb_dal.create_bucket(
                bucket_name=bucket_name,
                ram_quota_mb=ram_quota_mb,
                wait_ready=wait_ready,
                wait_timeout=wait_timeout
            )
        except Exception as e:
            logger.error(f"Error creating bucket {bucket_name}: {e}", exc_info=True)
            return False
    
    def load_json_to_bucket(self, bucket_name: str, file_path: str, batch_size: int = 100, 
                           check_bucket: bool = True, create_if_not_exists: bool = True,
                           ram_quota_mb: int = 100, max_retries: int = 3, retry_delay: int = 2):
        """
        Load data from JSON file into Couchbase bucket.
        
        Args:
            bucket_name: Name of the bucket to load data into
            file_path: Path to the JSON file
            batch_size: Number of documents to process in each batch (default: 100)
            check_bucket: If True, check if bucket exists before importing (default: True)
            create_if_not_exists: If True, create bucket if it doesn't exist (default: True)
            ram_quota_mb: RAM quota in MB for new bucket (default: 100)
            max_retries: Maximum number of retries for failed upserts (default: 3)
            retry_delay: Delay in seconds between retries (default: 2)
        
        Returns:
            int: Number of documents successfully loaded
        """
        try:
            if not os.path.exists(file_path):
                logger.error(f"JSON file not found: {file_path}")
                raise FileNotFoundError(f"JSON file not found: {file_path}")
            
            # Check if bucket exists and create if needed
            if check_bucket:
                logger.info(f"Checking if bucket {bucket_name} exists...")
                if not self.check_bucket_exists(bucket_name):
                    if create_if_not_exists:
                        logger.info(f"Bucket '{bucket_name}' does not exist. Creating new bucket...")
                        if not self.create_bucket(bucket_name, ram_quota_mb=ram_quota_mb):
                            error_msg = (
                                f"Failed to create bucket '{bucket_name}'.\n"
                                f"Please create it manually using:\n"
                                f"  1. Couchbase Admin UI (http://localhost:8091)\n"
                                f"  2. REST API or command line tools"
                            )
                            logger.error(error_msg)
                            raise BucketNotFoundException(f"Bucket '{bucket_name}' not found and could not be created.")
                        logger.info(f"Bucket '{bucket_name}' created successfully")
                    else:
                        error_msg = (
                            f"Bucket '{bucket_name}' does not exist in Couchbase cluster.\n"
                            f"Please create the bucket first before importing data.\n"
                            f"You can create it using:\n"
                            f"  1. Couchbase Admin UI (http://localhost:8091)\n"
                            f"  2. REST API or command line tools"
                        )
                        logger.error(error_msg)
                        raise BucketNotFoundException(f"Bucket '{bucket_name}' not found. Please create it first.")
                else:
                    logger.info(f"Bucket {bucket_name} exists, proceeding with import...")
            
            # Read JSON file
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Ensure data is a list
            if not isinstance(data, list):
                data = [data]
            
            if not data:
                logger.warning(f"No data found in JSON file: {file_path}")
                return 0
            
            logger.info(f"Loading {len(data)} documents from {file_path} into bucket {bucket_name}")
            
            # Load data into bucket
            total_loaded = self.cb_dal.upsert_documents(
                bucket_name=bucket_name,
                documents=data,
                batch_size=batch_size,
                max_retries=max_retries,
                retry_delay=retry_delay
            )
            
            logger.info(f"Successfully loaded {total_loaded} documents into bucket {bucket_name}")
            return total_loaded
        except BucketNotFoundException:
            raise
        except Exception as e:
            logger.error(f"Error loading JSON to bucket {bucket_name}: {e}", exc_info=True)
            raise e
        finally:
            self.cb_dal.close()
            gc.collect()