import os
import sys
import gc
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import CouchbaseConfig, MongoDBConfig
from dal.couchbase_dal import CouchbaseDataAccess
from services.couchbase_service import CouchbaseService
from logger_config import setup_logging, get_logger
from dal.mongodb_dal import MongoDBDataAccess
from services.mongodb_service import MongoDBService

# Setup logging
setup_logging(log_dir='logs', log_level='INFO')
logger = get_logger(__name__)

# Thread-safe lock for logging
fetch_lock = Lock()

MECHOICE_BUCKETS = {
    "keep_structure" : ["mechoice_journal"],
    "restructure" : [ "mechoice_notification", "mechoice_report"],
    "missing_type":  ["mechoice_metadata", "mechoice_view", "mechoice_workflow_journal", "mechoice_star_award_view", "mechoice_star_award_journal",  "mechoice_workflow_view" ]
}


def process_page_mechoice(db_name: str, bucket_name: str, migration_type: str, page: int, page_size: int, 
                          max_retries: int = 3, retry_delay: int = 2, batch_size: int = 500):
    """
    Fetch a page from Couchbase and process according to migration type using MongoDBService.
    
    Args:
        db_name: MongoDB database name
        bucket_name: Couchbase bucket name
        migration_type: Type of migration ("keep_structure", "restructure", "missing_type")
        page: Page number (starts from 0)
        page_size: Number of records per page (default: 1000)
        max_retries: Maximum number of retries (default: 3)
        retry_delay: Delay between retries (default: 2)
        batch_size: Batch size for bulk insert (default: 500)
        
    Returns:
        tuple: (page, success_count) or (page, 0) if failed
    """
    cb_service = None
    mongo_service = None
    try:
        cb_service = CouchbaseService(CouchbaseDataAccess(CouchbaseConfig()))
        mongo_service = MongoDBService(MongoDBDataAccess(MongoDBConfig(db_name)))
        
        # Fetch data from Couchbase
        page_data = cb_service.get_data_paginated(
            bucket_name=bucket_name,
            page=page,
            page_size=page_size,
            max_retries=max_retries,
            retry_delay=retry_delay
        )
        
        if page_data is None or len(page_data) == 0:
            return (page, 0)
        
        # Process documents according to migration type using MongoDBService
        try:
            if migration_type == "keep_structure":
                mongo_service.process_mechoice_keep_structure(bucket_name, page_data, batch_size=batch_size)
                success_count = len(page_data)  # Service handles counting internally
            elif migration_type == "restructure":
                mongo_service.process_mechoice_restructure(bucket_name, page_data, batch_size=batch_size)
                success_count = len(page_data)  # Service handles counting internally
            elif migration_type == "missing_type":
                mongo_service.process_mechoice_missing_type(bucket_name, page_data, batch_size=batch_size)
                success_count = len(page_data)  # Service handles counting internally
            else:
                logger.error(f"Unknown migration type: {migration_type}")
                return (page, 0)
            
            with fetch_lock:
                logger.info(f"Page {page} (offset {page*page_size}): Successfully processed {len(page_data)} records for {bucket_name}")
            
            return (page, success_count)
            
        except Exception as e:
            with fetch_lock:
                logger.error(f"Error processing page {page} data for {bucket_name.upper()}: {e}", exc_info=True)
            return (page, 0)
        
    except Exception as e:
        with fetch_lock:
            logger.error(f"Error processing page {page} for {bucket_name.upper()}: {e}", exc_info=True)
        return (page, 0)
    finally:
        if cb_service:
            cb_service.cb_dal.close()
        if mongo_service:
            mongo_service.mongo_dal.close()
        gc.collect()


def migrate_bucket_mechoice(db_name: str, bucket_name: str, migration_type: str,
                           page_size: int = 1000, max_workers: int = 5,
                           max_retries: int = 3, retry_delay: int = 2, batch_size: int = 500):
    """
    Migrate data from Couchbase bucket to MongoDB using specified migration type.
    
    Args:
        db_name: MongoDB database name
        bucket_name: Couchbase bucket name
        migration_type: Type of migration ("keep_structure", "restructure", "missing_type")
        page_size: Number of records per page (default: 1000)
        max_workers: Maximum number of threads (default: 5)
        max_retries: Maximum number of retries (default: 3)
        retry_delay: Delay between retries (default: 2)
        batch_size: Batch size for bulk insert (default: 500)
    """
    cb_service = None
    try:
        cb_service = CouchbaseService(CouchbaseDataAccess(CouchbaseConfig()))
        
        # Get total count
        total_count = cb_service.get_total_count(bucket_name)
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        
        logger.info(f"{bucket_name.upper()} Total documents: {total_count}, Total pages: {total_pages}, "
                   f"Page size: {page_size}, Migration type: {migration_type}")
        
        total_processed = 0
        failed_pages = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all pages
            future_to_page = {
                executor.submit(
                    process_page_mechoice,
                    db_name,
                    bucket_name,
                    migration_type,
                    page,
                    page_size,
                    max_retries,
                    retry_delay,
                    batch_size
                ): page
                for page in range(total_pages)
            }
            
            # Collect results
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
        if cb_service:
            cb_service.cb_dal.close()
        gc.collect()


def create_index_mechoice(bucket_name: str, force: bool = False):
    """
    Create primary index on the bucket.
    Wait until index is online before migrating data
    """
    try:
        couchbase_service = CouchbaseService(CouchbaseDataAccess(CouchbaseConfig()))
        couchbase_service.create_primary_index(bucket_name, force=force)
        _, state = couchbase_service.check_index_status(bucket_name)
        while not state:
            time.sleep(1)
            _, state = couchbase_service.check_index_status(bucket_name)
            logger.info(f"Index is not online yet for {bucket_name}...")
        logger.info(f"Bucket {bucket_name.upper()} is ready to fetch data")
    except Exception as e:
        logger.error(f"Error creating primary index for bucket {bucket_name}: {e}", exc_info=True)
    finally:
        gc.collect()


def migrate_mechoice_data(db_name: str = "mechoice", page_size: int = 1000, max_workers: int = 8,
                        max_retries: int = 3, retry_delay: int = 2, batch_size: int = 500):
    """
    Migrate all mechoice buckets according to MECHOICE_BUCKETS configuration.
    
    Args:
        db_name: MongoDB database name (default: "mechoice")
        page_size: Number of records per page (default: 1000)
        max_workers: Maximum number of threads (default: 8)
        max_retries: Maximum number of retries (default: 3)
        retry_delay: Delay between retries (default: 2)
        batch_size: Batch size for bulk insert (default: 500)
    """
    try:
        logger.info("=" * 60)
        logger.info("Starting MECHOICE data migration")
        logger.info("=" * 60)
        
        # Process each migration type
        for migration_type, bucket_list in MECHOICE_BUCKETS.items():
            logger.info(f"\nProcessing migration type: {migration_type.upper()}")
            logger.info(f"Buckets: {', '.join(bucket_list)}")
            
            for bucket_name in bucket_list:
                logger.info("=" * 60)
                logger.info(f"Migrating bucket: {bucket_name.upper()}")
                logger.info(f"Migration type: {migration_type}")
                logger.info("=" * 60)
                
                # Check and create index if needed
                create_index_mechoice(bucket_name, force=True)
                
                # Migrate bucket
                migrate_bucket_mechoice(
                    db_name=db_name,
                    bucket_name=bucket_name,
                    migration_type=migration_type,
                    page_size=page_size,
                    max_workers=max_workers,
                    max_retries=max_retries,
                    retry_delay=retry_delay,
                    batch_size=batch_size
                )
        
        logger.info("=" * 60)
        logger.info("MECHOICE data migration completed")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error in migrate_mechoice_data: {e}", exc_info=True)
    finally:
        gc.collect()

def drop_collections(db_name: str):
    try:
        mongo_service = MongoDBService(MongoDBDataAccess(MongoDBConfig(db_name)))
        mongo_service.drop_collections()
    except Exception as e:
        logger.error(f"Error dropping collections from MongoDB: {e}", exc_info=True)
    finally:
        gc.collect()

if __name__ == "__main__":
    # Configuration
    db_name = "mechoice"
    page_size = 1000
    max_workers = 8
    max_retries = 3
    retry_delay = 2
    batch_size = 500  # Batch size for bulk insert
    
    start_time = time.time()
    # drop collections if exists
    drop_collections(db_name)

    migrate_mechoice_data(
        db_name=db_name,
        page_size=page_size,
        max_workers=max_workers,
        max_retries=max_retries,
        retry_delay=retry_delay,
        batch_size=batch_size
    )
    
    total_time = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"Migration completed in {total_time:.2f} seconds")
    logger.info("=" * 60)