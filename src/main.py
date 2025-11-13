
import logging
import json
import pandas as pd
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
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

RMS_BUCKET_LIST = ["rms_events","rms_journal","rms_rating_model","rms_read_model","rms_view","rms_write_model"]

def process_page(bucket_name: str, page: int, page_size: int, max_retries: int = 3, retry_delay: int = 2):
    """
    Fetch một page từ Couchbase và insert vào MongoDB.
    Mỗi page được xử lý bởi 1 thread.
    
    Args:
        bucket_name: Tên bucket Couchbase (cũng là tên collection MongoDB)
        page: Số thứ tự page (bắt đầu từ 0)
        page_size: Số records mỗi page
        max_retries: Số lần retry tối đa
        retry_delay: Thời gian delay giữa các retry
        
    Returns:
        tuple: (page, success_count) hoặc (page, 0) nếu thất bại
    """
    cb_dal = None
    mongo_dal = None
    offset = page * page_size
    
    try:
        # Fetch data từ Couchbase
        cb_config = CouchbaseConfig()
        cb_dal = CouchbaseDataAccess(cb_config)
        
        page_data = cb_dal.get_data_paginated(
            bucket_name=bucket_name,
            page_size=page_size,
            offset=offset,
            max_retries=max_retries,
            retry_delay=retry_delay
        )
        
        if not page_data or len(page_data) == 0:
            with fetch_lock:
                logger.info(f"Page {page} (offset {offset}): No data found")
            return (page, 0)
        
        # Insert vào MongoDB
        mongo_config = MongoDBConfig()
        mongo_dal = MongoDBDataAccess(mongo_config)
        mongo_service = MongoDBService(mongo_dal, mapping_id=True)
        
        mongo_service.add_documents(bucket_name, page_data)
        mongo_dal = None
        
        with fetch_lock:
            logger.info(f"Page {page} (offset {offset}): Successfully processed {len(page_data)} records")
        
        return (page, len(page_data))
        
    except Exception as e:
        with fetch_lock:
            logger.error(f"Error processing page {page} (offset {offset}): {e}", exc_info=True)
        return (page, 0)
    finally:
        gc.collect()

def migrate_bucket(bucket_name: str, page_size: int = 1000, max_workers: int = 5, 
                   max_retries: int = 3, retry_delay: int = 2):
    """
    Migrate data từ Couchbase bucket sang MongoDB collection.
    Mỗi page được xử lý bởi 1 thread: fetch từ Couchbase và insert vào MongoDB.
    
    Args:
        bucket_name: Tên bucket Couchbase (cũng là tên collection MongoDB)
        page_size: Số records mỗi page
        max_workers: Số threads tối đa
        max_retries: Số lần retry tối đa cho mỗi page
        retry_delay: Thời gian delay giữa các retry
    """
    cb_dal = None
    try:
        # Lấy tổng số documents để tính số pages
        cb_config = CouchbaseConfig()
        cb_dal = CouchbaseDataAccess(cb_config)
        total_count = cb_dal.get_total_count(bucket_name)
        
        if total_count == 0:
            logger.warning(f"No data found in bucket {bucket_name.upper()}")
            return
        
        # Tính số pages
        total_pages = (total_count + page_size - 1) // page_size
        logger.info(f"Total documents: {total_count}, Total pages: {total_pages}, Page size: {page_size}")
        
        # Xử lý các pages song song bằng thread pool
        total_processed = 0
        failed_pages = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tất cả các pages
            future_to_page = {
                executor.submit(
                    process_page,
                    bucket_name,
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
        
        logger.info(f"Migration completed: {total_processed}/{total_count} records processed")
        if failed_pages:
            logger.warning(f"Failed to process {len(failed_pages)} pages: {failed_pages[:10]}...")
            
    except Exception as e:
        logger.error(f"Error migrating bucket {bucket_name}: {e}", exc_info=True)
    finally:
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
        logger.info("Bucket {bucket_name.upper()} is ready to fetch data")
    except Exception as e:
        logger.error(f"Error creating primary index for bucket {bucket_name}: {e}", exc_info=True)

if __name__ == "__main__":
    # Configuration
    page_size = 1000  # Số records mỗi page
    max_workers = 5  # Số threads xử lý song song
    max_retries = 3  # Số lần retry tối đa khi timeout
    retry_delay = 2  # Thời gian delay giữa các retry (giây)
    
    start_time = time.time()
    for bucket_name in [RMS_BUCKET_LIST]:
        # check and create index if not exists
        create_index(bucket_name)

        logger.info("=" * 60)
        logger.info(f"Migrating bucket: {bucket_name.upper()}")
        logger.info(f"Configuration: page_size={page_size}, max_workers={max_workers}, "
                    f"max_retries={max_retries}, retry_delay={retry_delay}s")
        logger.info("=" * 60)
        
        migrate_bucket(
            bucket_name=bucket_name,
            page_size=page_size,
            max_workers=max_workers,
            max_retries=max_retries,
            retry_delay=retry_delay
        )
        
    total_time = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"Migration completed in {total_time:.2f} seconds")
    logger.info("=" * 60)
        