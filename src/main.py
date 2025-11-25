
import logging
import json
# import pandas as pd
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import process
from threading import Lock
import time

from config import CouchbaseConfig, MongoDBConfig
from dal.couchbase_dal import CouchbaseDataAccess
from dal.mongodb_dal import MongoDBDataAccess
from logger_config import setup_logging, get_logger

from services.couchbase_service import CouchbaseService
from services.mongodb_service import MongoDBService

# Setup logging
setup_logging(log_dir='logs', log_level=logging.DEBUG)
logger = get_logger(__name__)

# Thread-safe lock for logging
fetch_lock = Lock()

# RMS_BUCKET_LIST = ["rms_events","rms_journal","rms_rating_model","rms_read_model","rms_view","rms_write_model"]

DB_SETTING = json.load(open('./src/dbsetting.json', 'r'))

def process_page(db_name: str, bucket_name: str, migrate_keep_structure: bool, page: int, page_size: int, max_retries: int = 3, retry_delay: int = 2):
    """
    Fetch a page from Couchbase and insert into MongoDB.
    
    Args:
        db_name: MongoDB database name
        bucket_name: Couchbase bucket name (also the MongoDB collection name)
        migrate_keep_structure: True if migrate data with original structure, False if migrate data with RMS structure
        page: Page number (starts from 0)
        page_size: Number of records per page (default: 1000)
        max_retries: Maximum number of retries (default: 3)
        retry_delay: Delay between retries (default: 2)
        
    Returns:
        tuple: (page, success_count) or (page, 0) if failed (NO_DATA_FOUND)
    """
    cb_service = CouchbaseService(CouchbaseDataAccess(CouchbaseConfig()))
    mongo_service = MongoDBService(MongoDBDataAccess(MongoDBConfig(db_name)), mapping_id=True)
    try:
        # Fetch data from Couchbase
        page_data = cb_service.get_data_paginated(
            bucket_name=bucket_name,
            page=page,
            page_size=page_size,
            max_retries=max_retries,
            retry_delay=retry_delay
        )
        if page_data is None:
            return (page, 0)
        # Insert into MongoDB
        if migrate_keep_structure:
            mongo_service.add_documents(bucket_name, page_data)
        else:
            mongo_service.process_rms_data(bucket_name, page_data)
        
        with fetch_lock:
            logger.info(f"Page: {page}x size:{page*page_size}): Successfully processed {len(page_data)} records")
        
        return (page, len(page_data))
        
    except Exception as e:
        with fetch_lock:
            logger.error(f"Error processing page:{page} x size:{page_size} for {bucket_name.upper()}: {e}", exc_info=True)
        return (page, 0)
    finally:
        del cb_service, mongo_service, page_data
        gc.collect()

def migrate_bucket(db_name: str, bucket_name: str, migrate_keep_structure: bool, 
                   page_size: int = 1000, max_workers: int = 5, 
                   max_retries: int = 3, retry_delay: int = 2):
    """
    Migrate data from Couchbase bucket to MongoDB collection.
    Each page is processed by 1 thread: fetch from Couchbase and insert into MongoDB.
    
    Args:
        db_name: MongoDB database name
        bucket_name: Couchbase bucket name (also the MongoDB collection name)
        migrate_keep_structure: True if migrate data with original structure, False if migrate data with RMS structure
        page_size: Number of records per page (default: 1000)
        max_workers: Maximum number of threads (default: 5)
        max_retries: Maximum number of retries (default: 3)
        retry_delay: Delay between retries (default: 2)
    """
    cb_service = CouchbaseService(CouchbaseDataAccess(CouchbaseConfig()))
    try:
        # Lấy tổng số documents để tính số pages
        total_count = cb_service.get_total_count(bucket_name)
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        logger.info(f"{bucket_name.upper()} Total documents: {total_count}, Total pages: {total_pages}, Page size: {page_size}")
        total_processed = 0
        failed_pages = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tất cả các pages
            future_to_page = {
                executor.submit(
                    process_page,
                    db_name,
                    bucket_name,
                    migrate_keep_structure,
                    page,
                    page_size,
                    max_retries,
                    retry_delay
                ): page
                for page in range(total_pages)
            }
        
            # Thu thập kết quả khi hoàn thành
            completed = 0
            for future in as_completed(future_to_page):
                page = future_to_page[future]
                try:
                    result_page, processed_count = future.result()
                    total_processed += processed_count
                    completed += 1
                    if completed % 10 == 0:
                        logger.info(f"Progress: {completed}/{total_pages} pages processed ({total_processed} records)")
                    if processed_count == 0:
                        failed_pages.append(result_page)
                except Exception as e:
                    logger.error(f"Error getting result for page {page}: {e}", exc_info=True)
                    failed_pages.append(page)
        
        logger.info(f"{bucket_name.upper()} Migration completed: {total_processed}/{total_count} records processed")
        if failed_pages:
            logger.warning(f"{bucket_name.upper()}: Failed to process {len(failed_pages)} pages: {failed_pages[:10]}...")
            
    except Exception as e:
        logger.error(f"Error migrating bucket {bucket_name}: {e}", exc_info=True)
    finally:
        del cb_service
        gc.collect()

def create_index(bucket_name: str):
    """
    Create primary index on the bucket.
    Wait until index is online before migrating data
    """
    try:
        couchbase_service = CouchbaseService(CouchbaseDataAccess(CouchbaseConfig()))
        couchbase_service.create_primary_index(bucket_name)
        _, state = couchbase_service.check_index_status(bucket_name)
        while not state:
            time.sleep(1)
            _, state = couchbase_service.check_index_status(bucket_name)
            logger.info(f"Index is not online yet...)")
        logger.info(f"Bucket {bucket_name.upper()} is ready to fetch data")
    except Exception as e:
        logger.error(f"Error creating primary index for bucket {bucket_name}: {e}", exc_info=True)

def drop_collections(db_name: str):
    try:
        mongo_service = MongoDBService(MongoDBDataAccess(MongoDBConfig(db_name)))
        mongo_service.drop_collections()
    except Exception as e:
        logger.error(f"Error dropping collections from MongoDB: {e}", exc_info=True)
    finally:
        gc.collect()
    
def migrate_all_buckets(migrate_keep_structure: bool, 
                        page_size:int=1000, max_workers:int=8, max_retries:int=3, retry_delay:int=2):
    '''
    Migrate all buckets in setting file with parallel processing
    migrate_keep_structure: True if migrate data with original structure, 
    False if migrate data with RMS structure
    '''
    try:
        for db_name, bucket_names in DB_SETTING.items():
            drop_collections(db_name)
            for bucket_name in bucket_names:
                # check and create index if not exists
                create_index(bucket_name)

                logger.info("=" * 60)
                logger.info(f"Migrating bucket: {bucket_name.upper()}")
                logger.info(f"Configuration: page_size={page_size}, max_workers={max_workers}, "
                            f"max_retries={max_retries}, retry_delay={retry_delay}s")
                logger.info("=" * 60)
                
                migrate_bucket(
                    db_name=db_name,
                    bucket_name=bucket_name,
                    migrate_keep_structure=migrate_keep_structure,
                    page_size=page_size,
                    max_workers=max_workers,
                    max_retries=max_retries,
                    retry_delay=retry_delay
                )
    except Exception as e:
        logger.error(f"Error migrating all buckets: {e}", exc_info=True)
    finally:
        gc.collect()

def process_single(db_name:str, bucket_name: str, migrate_keep_structure: bool, 
                   page:int, page_size:int, max_retries:int, retry_delay:int):
    'migrate data with single thread'
    process_page(
        db_name=db_name, 
        bucket_name=bucket_name, 
        migrate_keep_structure=migrate_keep_structure,
        page=page,
        page_size=page_size,
        max_retries=max_retries,
        retry_delay=retry_delay
    )
        
    
if __name__ == "__main__":
    # Configuration
    # db_name = "rms"
    # bucket_name = "rms_events"
    # page = 0
    page_size = 1000  # Number of records per page
    max_workers = 8  # Maximum number of threads for parallel processing
    max_retries = 3  # Maximum number of retries when timeout
    retry_delay = 2  # Delay between retries (seconds)
    migrate_keep_structure = False # migrate RMS data. update to True when migrate original data
    start_time = time.time()

    migrate_all_buckets(
        migrate_keep_structure=migrate_keep_structure, 
        page_size=page_size, 
        max_workers=max_workers, 
        max_retries=max_retries, 
        retry_delay=retry_delay
    )
    
    # process_single(
    #     db_name=db_name, 
    #     bucket_name=bucket_name, 
    #     migrate_keep_structure=migrate_keep_structure, 
    #     page=0, 
    #     page_size=page_size, 
    #     max_retries=max_retries, 
    #     retry_delay=retry_delay
    # )
         
    total_time = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"Migration completed in {total_time:.2f} seconds")
    logger.info("=" * 60)
        