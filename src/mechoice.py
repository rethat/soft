import os
import sys
import json
import gc
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import CouchbaseConfig
from dal.couchbase_dal import CouchbaseDataAccess
from services.couchbase_service import CouchbaseService
from logger_config import setup_logging, get_logger
from couchbase.exceptions import BucketNotFoundException

# Setup logging
setup_logging(log_dir='logs', log_level='INFO')
logger = get_logger(__name__)

# Thread-safe lock for logging
fetch_lock = Lock()


def export_bucket_to_json(bucket_name: str, output_dir: str = "exports", 
                          page_size: int = 1000, max_retries: int = 3, 
                          retry_delay: int = 2, append: bool = True):
    """
    Export data from a Couchbase bucket to JSON file.
    
    Args:
        bucket_name: Name of the bucket to export
        output_dir: Directory to save JSON files (default: "exports")
        page_size: Number of records per page (default: 1000)
        max_retries: Maximum number of retries (default: 3)
        retry_delay: Delay between retries in seconds (default: 2)
        append: If True, append to existing file. If False, overwrite (default: True)
    
    Returns:
        tuple: (bucket_name, file_path, total_documents) or (bucket_name, None, 0) if failed
    """
    cb_service = None
    try:
        logger.info(f"=" * 50)
        logger.info(f"Exporting bucket: {bucket_name.upper()}")
        logger.info(f"=" * 50)
        
        # Initialize service
        cb_service = CouchbaseService(CouchbaseDataAccess(CouchbaseConfig()))
        
        # Check and create primary index if needed
        try:
            cb_service.create_primary_index(bucket_name)
        except Exception as e:
            logger.warning(f"Could not create index for {bucket_name}: {e}")
        
        # Get total count
        try:
            total_count = cb_service.get_total_count(bucket_name)
            logger.info(f"Total documents in {bucket_name.upper()}: {total_count}")
        except Exception as e:
            logger.error(f"Failed to get total count for {bucket_name}: {e}")
            return (bucket_name, None, 0)
        
        if total_count == 0:
            logger.warning(f"No documents found in bucket {bucket_name.upper()}")
            return (bucket_name, None, 0)
        
        # Calculate total pages
        total_pages = (total_count + page_size - 1) // page_size
        logger.info(f"Total pages: {total_pages}, Page size: {page_size}")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, f"{bucket_name}.json")
        
        # Export data page by page
        total_exported = 0
        failed_pages = []
        
        for page in range(total_pages):
            try:
                page_data = cb_service.get_data_paginated(
                    bucket_name=bucket_name,
                    page=page,
                    page_size=page_size,
                    max_retries=max_retries,
                    retry_delay=retry_delay
                )
                
                if page_data and len(page_data) > 0:
                    # Export to JSON (append mode for subsequent pages)
                    is_append = append if page == 0 else True
                    cb_service.export_data_to_json(
                        bucket_name=bucket_name,
                        data=page_data,
                        file_path=file_path,
                        append=is_append
                    )
                    total_exported += len(page_data)
                    logger.info(f"Page {page + 1}/{total_pages}: Exported {len(page_data)} documents (Total: {total_exported})")
                else:
                    logger.warning(f"Page {page + 1}/{total_pages}: No data found")
                    failed_pages.append(page)
                    
            except Exception as e:
                logger.error(f"Error exporting page {page + 1} for {bucket_name}: {e}", exc_info=True)
                failed_pages.append(page)
                continue
        
        if failed_pages:
            logger.warning(f"Failed to export {len(failed_pages)} pages for {bucket_name}: {failed_pages}")
        
        logger.info(f"Export completed for {bucket_name.upper()}: {total_exported} documents exported to {file_path}")
        return (bucket_name, file_path, total_exported)
        
    except Exception as e:
        logger.error(f"Error exporting bucket {bucket_name}: {e}", exc_info=True)
        return (bucket_name, None, 0)
    finally:
        if cb_service:
            cb_service.cb_dal.close()
        gc.collect()


def export_buckets_to_json(buckets_config: dict, output_dir: str = "exports",
                           page_size: int = 1000, max_workers: int = 3,
                           max_retries: int = 3, retry_delay: int = 2, append: bool = True):
    """
    Export data from multiple Couchbase buckets to JSON files.
    
    Args:
        buckets_config: Dictionary mapping keys to lists of bucket names.
                       Example: {"db1": ["bucket1", "bucket2"], "db2": ["bucket3"]}
                       Or simple list: ["bucket1", "bucket2"]
        output_dir: Directory to save JSON files (default: "exports")
        page_size: Number of records per page (default: 1000)
        max_workers: Maximum number of parallel workers (default: 3)
        max_retries: Maximum number of retries (default: 3)
        retry_delay: Delay between retries in seconds (default: 2)
        append: If True, append to existing file. If False, overwrite (default: True)
    
    Returns:
        dict: Summary of export results {bucket_name: (file_path, total_documents)}
    """
    # Flatten buckets config to list
    bucket_list = []
    if isinstance(buckets_config, dict):
        for key, buckets in buckets_config.items():
            bucket_list.extend(buckets)
    elif isinstance(buckets_config, list):
        bucket_list = buckets_config
    else:
        logger.error("buckets_config must be a dict or list")
        return {}
    
    logger.info(f"=" * 50)
    logger.info(f"Starting export for {len(bucket_list)} buckets")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"=" * 50)
    
    results = {}
    start_time = time.time()
    
    # Process buckets in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                export_bucket_to_json,
                bucket_name=bucket,
                output_dir=output_dir,
                page_size=page_size,
                max_retries=max_retries,
                retry_delay=retry_delay,
                append=append
            ): bucket for bucket in bucket_list
        }
        
        for future in as_completed(futures):
            bucket_name = futures[future]
            try:
                bucket_name, file_path, total_docs = future.result()
                results[bucket_name] = (file_path, total_docs)
            except Exception as e:
                logger.error(f"Error processing bucket {bucket_name}: {e}", exc_info=True)
                results[bucket_name] = (None, 0)
    
    total_time = time.time() - start_time
    
    # Print summary
    logger.info("=" * 50)
    logger.info("EXPORT SUMMARY")
    logger.info("=" * 50)
    total_docs = 0
    success_count = 0
    for bucket_name, (file_path, docs) in results.items():
        if file_path:
            logger.info(f"{bucket_name.upper()}: {docs} documents -> {file_path}")
            total_docs += docs
            success_count += 1
        else:
            logger.error(f"{bucket_name.upper()}: FAILED")
    logger.info("=" * 90)
    logger.info(f"Total buckets: {len(bucket_list)}, Success: {success_count}, Failed: {len(bucket_list) - success_count}")
    logger.info(f"Total documents exported: {total_docs}")
    logger.info(f"Total time: {total_time:.2f} seconds")
    logger.info("=" * 90)
    
    return results


def import_json_to_bucket(bucket_name: str, file_path: str, batch_size: int = 100, 
                          check_bucket: bool = True, create_if_not_exists: bool = True,
                          ram_quota_mb: int = 100, max_retries: int = 3, retry_delay: int = 2):
    """
    Import data from JSON file to Couchbase bucket.
    
    Args:
        bucket_name: Name of the bucket to import data into
        file_path: Path to the JSON file
        batch_size: Number of documents to process in each batch (default: 100)
        check_bucket: If True, check if bucket exists before importing (default: True)
        create_if_not_exists: If True, create bucket if it doesn't exist (default: True)
        ram_quota_mb: RAM quota in MB for new bucket (default: 100)
        max_retries: Maximum number of retries for failed upserts (default: 3)
        retry_delay: Delay in seconds between retries (default: 2)
    
    Returns:
        int: Number of documents successfully imported, or 0 if failed
    """
    cb_service = None
    try:
        logger.info(f"=" * 50)
        logger.info(f"Importing to bucket: {bucket_name.upper()}")
        logger.info(f"Source file: {file_path}")
        logger.info(f"=" * 50)
        
        # Initialize service
        cb_service = CouchbaseService(CouchbaseDataAccess(CouchbaseConfig()))
        
        # Load JSON to bucket
        total_loaded = cb_service.load_json_to_bucket(
            bucket_name=bucket_name,
            file_path=file_path,
            batch_size=batch_size,
            check_bucket=check_bucket,
            create_if_not_exists=create_if_not_exists,
            ram_quota_mb=ram_quota_mb,
            max_retries=max_retries,
            retry_delay=retry_delay
        )
        
        logger.info(f"Import completed for {bucket_name.upper()}: {total_loaded} documents imported")
        return total_loaded
        
    except BucketNotFoundException as e:
        logger.error(f"Bucket '{bucket_name}' not found and could not be created.")
        logger.error(f"Error: {e}")
        return 0
    except Exception as e:
        logger.error(f"Error importing to bucket {bucket_name}: {e}", exc_info=True)
        return 0
    finally:
        if cb_service:
            cb_service.cb_dal.close()
        gc.collect()


def import_json_to_buckets(buckets_config: dict, input_dir: str = "exports",
                           batch_size: int = 100, max_workers: int = 3,
                           create_if_not_exists: bool = True, ram_quota_mb: int = 100,
                           max_retries: int = 3, retry_delay: int = 2):
    """
    Import data from JSON files to Couchbase buckets.
    
    Args:
        buckets_config: Dictionary mapping keys to lists of bucket names.
                       Example: {"db1": ["bucket1", "bucket2"], "db2": ["bucket3"]}
                       Or simple list: ["bucket1", "bucket2"]
        input_dir: Directory containing JSON files (default: "exports")
        batch_size: Number of documents to process in each batch (default: 100)
        max_workers: Maximum number of parallel workers (default: 3)
        create_if_not_exists: If True, create buckets if they don't exist (default: True)
        ram_quota_mb: RAM quota in MB for new buckets (default: 100)
        max_retries: Maximum number of retries for failed upserts (default: 3)
        retry_delay: Delay in seconds between retries (default: 2)
    
    Returns:
        dict: Summary of import results {bucket_name: total_documents}
    """
    # Flatten buckets config to list
    if isinstance(buckets_config, dict):
        bucket_list = []
        for key, buckets in buckets_config.items():
            bucket_list.extend(buckets)
    elif isinstance(buckets_config, list):
        bucket_list = buckets_config
    else:
        logger.error("buckets_config must be a dict or list")
        return {}
    
    logger.info(f"=" * 50)
    logger.info(f"Starting import for {len(bucket_list)} buckets")
    logger.info(f"Input directory: {input_dir}")
    logger.info(f"=" * 50)
    
    results = {}
    start_time = time.time()
    
    # Process buckets in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for bucket in bucket_list:
            file_path = os.path.join(input_dir, f"{bucket}.json")
            if not os.path.exists(file_path):
                logger.warning(f"JSON file not found for {bucket}: {file_path}")
                results[bucket] = 0
                continue
            
            future = executor.submit(
                import_json_to_bucket,
                bucket_name=bucket,
                file_path=file_path,
                batch_size=batch_size,
                create_if_not_exists=create_if_not_exists,
                ram_quota_mb=ram_quota_mb,
                max_retries=max_retries,
                retry_delay=retry_delay
            )
            futures[future] = bucket
        
        for future in as_completed(futures):
            bucket_name = futures[future]
            try:
                total_docs = future.result()
                results[bucket_name] = total_docs
            except Exception as e:
                logger.error(f"Error processing bucket {bucket_name}: {e}", exc_info=True)
                results[bucket_name] = 0
    
    total_time = time.time() - start_time
    
    # Print summary
    logger.info("=" * 50)
    logger.info("IMPORT SUMMARY")
    logger.info("=" * 50)
    total_docs = 0
    success_count = 0
    for bucket_name, docs in results.items():
        if docs > 0:
            logger.info(f"{bucket_name.upper()}: {docs} documents imported")
            total_docs += docs
            success_count += 1
        else:
            logger.error(f"{bucket_name.upper()}: FAILED")
    logger.info("=" * 50)
    logger.info(f"Total buckets: {len(bucket_list)}, Success: {success_count}, Failed: {len(bucket_list) - success_count}")
    logger.info(f"Total documents imported: {total_docs}")
    logger.info(f"Total time: {total_time:.2f} seconds")
    logger.info("=" * 50)
    
    return results

BUCKET_LIST = ["rms_rating_model", "rms_view"]
MECHOICE_BUCKET_LIST = ["mec_rating_model", "mec_view"]

if __name__ == "__main__":
    # mode = 'export' 
    mode = 'import'
    page_size = 1000
    batch_size = 100
    max_workers = 8
    export_path = "exports"
    max_retries = 3
    retry_delay = 2
    append = True
    
    if mode == 'export':
        logger.info(f"Starting EXPORT data")
        results = export_buckets_to_json(
            buckets_config=BUCKET_LIST,
            output_dir=export_path,
            page_size=page_size,
            max_workers=max_workers,
            max_retries=max_retries,
            retry_delay=retry_delay,
            append=append
        )
    else:  # import
        logger.info(f"Starting IMPORT data")
        results = import_json_to_buckets(
            buckets_config=MECHOICE_BUCKET_LIST,
            input_dir=export_path,
            batch_size=batch_size,
            max_workers=max_workers
        )
    
    logger.info("Operation completed!")

