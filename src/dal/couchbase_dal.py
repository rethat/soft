from datetime import timedelta
import os
import sys
import gc
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import CouchbaseConfig
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import CouchbaseException, BucketNotFoundException, AmbiguousTimeoutException
from couchbase.management.buckets import BucketManager, CreateBucketSettings, BucketType
from couchbase.options import ClusterOptions, QueryOptions, ClusterTimeoutOptions
from logger_config import get_logger
import uuid
import json

logger = get_logger(__name__)

class CouchbaseDataAccess:

    def __init__(self, config: CouchbaseConfig):
        self.config = config
        self.cluster = None
        self.bucket = None
        
    def connect(self, timeout: timedelta = timedelta(seconds=300)):
        """
        Connect to Couchbase cluster with configurable timeout
        
        Args:
            timeout: Timeout for cluster connection (default: 30 seconds)
        """
        try:
            authenticator = PasswordAuthenticator(self.config.user, self.config.password)
            
            # Configure timeout options
            timeout_opts = ClusterTimeoutOptions(
                connect_timeout=timeout,
                kv_timeout=timedelta(seconds=60),  # Increased KV timeout for large documents
                query_timeout=timedelta(seconds=120),
                analytics_timeout=timedelta(seconds=120),
                search_timeout=timedelta(seconds=30),
                views_timeout=timedelta(seconds=30),
                management_timeout=timedelta(seconds=30)
            )
            
            cluster_options = ClusterOptions(authenticator, timeout_options=timeout_opts)
            self.cluster = Cluster(f"couchbase://{self.config.host}", cluster_options)
            
            # Wait for the cluster to be ready
            self.cluster.wait_until_ready(timeout=timeout)
            return self.cluster
        except CouchbaseException as e:
            error_msg = str(e)
            if "timeout" in error_msg.lower():
                logger.error(
                    f"Timeout connecting to Couchbase cluster at {self.config.host}. "
                    f"Current timeout: {timeout.total_seconds()}s\n"
                    f"Error: {e}",
                    exc_info=True
                )
            else:
                logger.error(
                    f"Error connecting to Couchbase cluster at {self.config.host}: {e}",
                    exc_info=True
                )
            raise e
        except Exception as e:
            logger.error(
                f"Unexpected error connecting to Couchbase: {e}",
                exc_info=True
            )
            raise e
    
    def close(self):
        if self.cluster:
            self.cluster.close()
            self.cluster = None
            self.bucket = None
            logger.info("Closing Couchbase connection")
            # Removed gc.collect() to prevent debugger hang
    
    def get_data(self, query: str, debug: bool = False, offset: int = None, limit: int = None):
        """
        Get data using N1QL query. Requires primary index on the bucket.
        Returns: list of dictionaries (JSON-like format)
        
        Args:
            query: N1QL query string
            debug: If True, print debug information about row structure
            offset: Optional offset for pagination
            limit: Optional limit for pagination (overrides LIMIT in query if provided)
        """
        # Add pagination to query if provided
        if offset is not None or limit is not None:
            # Remove existing LIMIT and OFFSET if present
            query_upper = query.upper()
            if 'LIMIT' in query_upper:
                # Remove LIMIT clause
                import re
                query = re.sub(r'\s+LIMIT\s+\d+', '', query, flags=re.IGNORECASE)
            if 'OFFSET' in query_upper:
                # Remove OFFSET clause
                import re
                query = re.sub(r'\s+OFFSET\s+\d+', '', query, flags=re.IGNORECASE)
            
            # Add pagination
            if limit is not None:
                query += f" LIMIT {limit}"
            if offset is not None:
                query += f" OFFSET {offset}"
        
        try:
            cluster = self.connect()
            query_options = QueryOptions(client_context_id=str(uuid.uuid4()))
            result = cluster.query(query, query_options)
            rows = []
            first_row_processed = False
            
            for row in result.rows():
                # Convert row to dictionary/JSON format
                try:
                    # In Couchbase Python SDK 4.x, query rows are typically dict-like
                    # They can be accessed directly as dict or converted
                    row_dict = None
                    
                    # Method 1: Row is already a dict
                    if isinstance(row, dict):
                        row_dict = row
                    # Method 2: Try direct dict conversion (most common case)
                    elif hasattr(row, '__iter__') and not isinstance(row, str):
                        try:
                            row_dict = dict(row) if row else None
                        except (TypeError, ValueError):
                            pass
                    
                    # Method 3: If dict conversion didn't work, try JSON serialization
                    if not row_dict:
                        try:
                            # Use json.dumps with default=str to handle any object
                            row_str = json.dumps(row, default=str, ensure_ascii=False)
                            row_dict = json.loads(row_str)
                        except (TypeError, ValueError, json.JSONDecodeError) as json_err:
                            if debug:
                                print(f"JSON conversion failed: {json_err}")
                    
                    # Method 4: Last resort - manual attribute extraction
                    if not row_dict:
                        row_dict = {}
                        # Try common attributes
                        for attr in ['id', 'value', 'key', 'doc']:
                            if hasattr(row, attr):
                                val = getattr(row, attr)
                                if isinstance(val, dict):
                                    row_dict.update(val)
                                else:
                                    row_dict[attr] = val
                        
                        # If still empty, try __dict__
                        if not row_dict and hasattr(row, '__dict__'):
                            row_dict = {k: v for k, v in row.__dict__.items() 
                                      if not k.startswith('_') and not callable(v)}
                    
                    # Debug output for first row
                    if debug and not first_row_processed:
                        logger.debug(f"First row type: {type(row)}")
                        logger.debug(f"First row dict: {row_dict}")
                        logger.debug(f"First row attributes: {dir(row)[:10] if hasattr(row, '__dict__') else 'N/A'}")
                        first_row_processed = True
                    
                    if row_dict:
                        rows.append(row_dict)
                    else:
                        logger.warning(f"Could not convert row to dict. Type: {type(row)}")
                        if debug:
                            logger.debug(f"Row value: {row}")
                        
                except Exception as e:
                    logger.error(f"Error processing row: {e}", exc_info=True)
                    continue
            return rows
        except CouchbaseException as e:
            logger.error(f"Error querying Couchbase: {e}", exc_info=True)
            raise e
        finally:
            self.close()
            # Removed gc.collect() to prevent debugger hang

    def get_data_by_keys(self, bucket_name: str, keys: list):
        """
        Get data using Key-Value operations. Doesn't require index.
        Args:
            bucket_name: Name of the bucket
            keys: List of document keys to retrieve
        Returns: list of dictionaries with 'id' and 'value' keys
        """
        try:
            cluster = self.connect()
            if not self.bucket or self.bucket.name != bucket_name:
                self.bucket = cluster.bucket(bucket_name)
            
            collection = self.bucket.default_collection()
            results = []
            
            for key in keys:
                try:
                    result = collection.get(key)
                    results.append({
                        'id': key,
                        'value': result.content_as[dict]
                    })
                except Exception as e:
                    logger.error(f"Error getting key {key}: {e}", exc_info=True)
                    continue
            
            return results
        except CouchbaseException as e:
            logger.error(f"Error getting data by keys: {e}", exc_info=True)
            raise e
        finally:
            self.close()
            # Removed gc.collect() to prevent debugger hang

    def get_all_keys(self, bucket_name: str, limit: int = None):
        """
        Get all document keys from a bucket using a scan operation.
        Note: This requires a primary index or you need to know the keys beforehand.
        Alternative: Use N1QL with USE KEYS or get keys from another source.
        """
        try:
            # Try to get keys using N1QL (requires index)
            cluster = self.connect()
            query = f"SELECT meta().id FROM `{bucket_name}`"
            if limit:
                query += f" LIMIT {limit}"
            
            query_options = QueryOptions(client_context_id=str(uuid.uuid4()))
            result = cluster.query(query, query_options)
            keys = [row['id'] for row in result.rows()]
            return keys
        except CouchbaseException as e:
            logger.error(f"Error getting keys (index may be required): {e}", exc_info=True)
            logger.info("Tip: You can create a primary index with: CREATE PRIMARY INDEX ON `bucket_name`")
            raise e
        finally:
            self.close()
            # Removed gc.collect() to prevent debugger hang
    
    def get_data_from_bucket(self, bucket_name: str, limit: int = 100):
        """
        Get data from bucket using Key-Value operations.
        First gets all keys, then retrieves documents.
        Returns: list of dictionaries in JSON format
        """
        try:
            # Get all keys first
            keys = self.get_all_keys(bucket_name, limit)
            
            # Get documents by keys
            return self.get_data_by_keys(bucket_name, keys)
        except CouchbaseException as e:
            logger.error(f"Error getting data from bucket: {e}", exc_info=True)
            raise e
    
    def check_index_status(self, bucket_name: str, index_name: str = None):
        """
        Check if primary index exists and is online.
        Returns: (exists, is_online) tuple
        """
        try:
            if index_name is None:
                index_name = "#primary"
            
            cluster = self.connect()
            # Query to check index status from system:indexes
            query = f"SELECT * FROM system:indexes WHERE keyspace_id = '{bucket_name}' AND name = '{index_name}' and is_primary = true"
            query_options = QueryOptions(client_context_id=str(uuid.uuid4()))
            result = cluster.query(query, query_options)
            
            indexes = []
            for row in result.rows():
                if isinstance(row, dict):
                    indexes.append(row)
                else:
                    try:
                        indexes.append(dict(row))
                    except:
                        if hasattr(row, 'state'):
                            indexes.append({'state': row.state})
                        elif hasattr(row, 'get'):
                            indexes.append(row)
            
            if not indexes:
                return (False, False)
            
            # Check if index is online
            index_info = indexes[0].get('indexes', '')
            if isinstance(index_info, dict):
                state = index_info.get('state', '').lower()
                # index_key = len(index_info.get('index_key', []))
            else:
                state = str(getattr(index_info, 'state', '')).lower()
                # index_key = len(getattr(index_info, 'index_key', []))
            is_ready = state == 'online' # and index_key > 0
            return (True, is_ready)
        except CouchbaseException as e:
            error_msg = str(e).lower()
            if "index" in error_msg and "not found" in error_msg:
                return (False, False)
            logger.warning(f"Could not check index status: {e}")
            return (False, False)
        except Exception as e:
            logger.error(f"Error checking index status: {e}", exc_info=True)
            return (False, False)
        finally:
            self.close()
            # Removed gc.collect() to prevent debugger hang
    
    def drop_primary_index(self, bucket_name: str):
        """
        Drop primary index on the bucket.
        """
        try:
            exists, _ = self.check_index_status(bucket_name)
            if not exists:
                return
            cluster = self.connect()
            query = f'DROP PRIMARY INDEX ON `{bucket_name}`;'
            query_options = QueryOptions(client_context_id=str(uuid.uuid4()))
            cluster.query(query, query_options).execute()
            logger.info(f"Primary index dropped for bucket: {bucket_name.upper()}")
        except CouchbaseException as e:
            logger.error(f"Error dropping primary index: {e}", exc_info=True)
            raise e
        finally:
            self.close()
            # Removed gc.collect() to prevent debugger hang
            
    
    def create_primary_index(self, bucket_name: str):
        """
        Create a primary index on the bucket.
        This is required for N1QL queries without USE KEYS clause.
        
        Args:
            bucket_name: Name of the bucket
        """
        try:
            cluster = self.connect()
            query = f'CREATE PRIMARY INDEX `#primary` ON `{bucket_name}` WITH {{"defer_build": true}};'
            query_options = QueryOptions(client_context_id=str(uuid.uuid4()))
            cluster.query(query, query_options).execute()
            logger.info(f"Primary index creation initiated for bucket: {bucket_name.upper()}")
        except CouchbaseException as e:
            logger.error(f"Error creating primary index: {e}", exc_info=True)
            raise e
        finally:
            self.close()
            # Removed gc.collect() to prevent debugger hang

    def build_primary_index(self, bucket_name: str):
        """
        Build primary index on the bucket.
        """
        try:
            cluster = self.connect()
            query = f'BUILD INDEX ON `{bucket_name}` (`#primary`);'
            query_options = QueryOptions(client_context_id=str(uuid.uuid4()))
            cluster.query(query, query_options).execute()
            logger.info(f"Primary index build initiated for bucket: {bucket_name.upper()}")
        except CouchbaseException as e:
            logger.error(f"Error building primary index: {e}", exc_info=True)
            raise e
        finally:
            self.close()
            # Removed gc.collect() to prevent debugger hang

    def get_data_as_json(self, query: str = None, bucket_name: str = None, limit: int = 100, keys: list = None):
        """
        Get data and return as JSON string.
        If query is provided, uses N1QL. 
        If bucket_name is provided, uses Key-Value operations.
        If keys are provided, retrieves only those specific documents.
        """
        if keys:
            # Get specific documents by keys
            data = self.get_data_by_keys(bucket_name, keys)
        elif query:
            data = self.get_data(query)
        elif bucket_name:
            data = self.get_data_from_bucket(bucket_name, limit)
        else:
            raise ValueError("Either query, bucket_name, or keys must be provided")
        
        return json.dumps(data, indent=2, ensure_ascii=False)
        

    def wait_for_index(self, bucket_name: str, index_name: str = None, max_wait: int = 300, check_interval: int = 5):
        """
        Wait for primary index to be online.
        Returns: True if index is online, False if timeout
        """
        if index_name is None:
            index_name = "#primary"
        
        import time
        elapsed = 0
        
        logger.info(f"Waiting for primary index on `{bucket_name}` to be online...")
        while elapsed < max_wait:
            exists, is_online = self.check_index_status(bucket_name, index_name)
            if exists and is_online:
                logger.info(f"Primary index is now online!")
                return True
            
            if not exists:
                logger.debug(f"Index not found yet... ({elapsed}s/{max_wait}s)")
            else:
                logger.debug(f"Index exists but not online yet... ({elapsed}s/{max_wait}s)")
            
            time.sleep(check_interval)
            elapsed += check_interval
        
        logger.warning(f"Timeout waiting for index to be online after {max_wait} seconds")
        return False
    
    def get_data_paginated(self, bucket_name: str, page_size: int = 1000, offset: int = 0, 
                          select_fields: str = "meta().id, *", where_clause: str = None, 
                          order_by: str = None, debug: bool = False, max_retries: int = 3, 
                          retry_delay: int = 2):
        """
        Get data from bucket with pagination support and retry logic.
        Nếu có lỗi (timeout hoặc connection), sẽ retry bằng cách gọi lại chính nó.
        
        Args:
            bucket_name: Name of the bucket
            page_size: Number of records per page (default: 1000)
            offset: Starting offset for pagination (default: 0)
            select_fields: Fields to select (default: "meta().id,*")
            where_clause: Optional WHERE clause (without WHERE keyword)
            order_by: Optional ORDER BY clause (without ORDER BY keywords)
            debug: If True, print debug information
            max_retries: Maximum number of retry attempts (default: 3)
            retry_delay: Delay in seconds between retries (default: 2)
            
        Returns:
            list: List of dictionaries containing the data, or empty list if all retries fail
        """
        # Build query
        query = f"SELECT {select_fields} FROM `{bucket_name}`"
        if where_clause:
            query += f" WHERE {where_clause}"
        if order_by:
            query += f" ORDER BY {order_by}"
        query += f" LIMIT {page_size} OFFSET {offset}"
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"=" * 90)
                logger.debug(f"Executing to fetch data from bucket {bucket_name.upper()} with: offset={offset}, limit={page_size}")
                logger.debug(f"Query: {query}")
                logger.debug(f"=" * 90)
                return self.get_data(query, debug=debug)
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)  # Exponential backoff
                    # logger.warning(
                    #     f"Error at offset {offset} (attempt {attempt + 1}/{max_retries}). "
                    #     f"Retrying in {wait_time} seconds... Error: {e}"
                    # )
                    time.sleep(wait_time)
                    # Tiếp tục vòng lặp để retry
                    continue
                else:
                    # Đã hết số lần retry, ghi nhận lỗi và trả về empty list
                    logger.error(
                        f"Failed to get data for {bucket_name.upper()} at offset {offset} with page_size {page_size} after {max_retries} attempts. "
                        f"Error: {e}. Skipping this page.",
                        exc_info=True
                    )
                    return []
        
        # Nếu đến đây thì đã hết retry
        logger.error(f"=" * 120)
        logger.error(f"Bucket: {bucket_name.upper()}."
                     f"Failed to get data at offset {offset} with page_size {page_size}."
                     f"Skipping this page for bucket {bucket_name.upper()} after {max_retries} attempts.")
        logger.error(f"=" * 120)
        return []
    
    def get_all_data_paginated(self, bucket_name: str, page_size: int = 1000,
                               select_fields: str = "meta().id,*", where_clause: str = None,
                               order_by: str = None, max_records: int = None, 
                               debug: bool = False, max_retries: int = 3, 
                               retry_delay: int = 2):
        """
        Generator function to fetch all data from bucket using pagination.
        Yields data page by page. get_data_paginated đã xử lý retry và trả về empty list nếu lỗi.
        
        Args:
            bucket_name: Name of the bucket
            page_size: Number of records per page (default: 1000)
            select_fields: Fields to select (default: "meta().id,*")
            where_clause: Optional WHERE clause (without WHERE keyword)
            order_by: Optional ORDER BY clause (without ORDER BY keywords)
            max_records: Maximum total records to fetch (None = all records)
            debug: If True, print debug information
            max_retries: Maximum number of retry attempts (default: 3)
            retry_delay: Delay in seconds between retries (default: 2)
            
        Yields:
            tuple: (offset, list) - Offset and list of dictionaries for each page
        """
        offset = 0
        total_fetched = 0
        consecutive_empty_pages = 0
        max_consecutive_empty = 10  # Stop if too many consecutive empty pages (failed pages)
        
        logger.info(f"Starting paginated fetch from bucket: {bucket_name} (page_size: {page_size})")
        
        while True:
            # Fetch one page (đã có retry logic bên trong)
            page_data = self.get_data_paginated(
                bucket_name=bucket_name,
                page_size=page_size,
                offset=offset,
                select_fields=select_fields,
                where_clause=where_clause,
                order_by=order_by,
                debug=debug,
                max_retries=max_retries,
                retry_delay=retry_delay
            )
            
            if not page_data or len(page_data) == 0:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    logger.warning(
                        f"Too many consecutive empty/failed pages ({consecutive_empty_pages}). "
                        f"Stopping pagination. Total fetched: {total_fetched}"
                    )
                    break
                # Skip empty page và tiếp tục
                offset += page_size
                continue
            
            # Reset counter khi có data
            consecutive_empty_pages = 0
            
            # Yield the page with offset info
            yield (offset, page_data)
            
            # Update counters
            page_count = len(page_data)
            total_fetched += page_count
            offset += page_size
            
            logger.debug(f"Fetched page at offset {offset - page_size}: {page_count} records, Total: {total_fetched}")
            
            # Check if we've reached max_records
            if max_records and total_fetched >= max_records:
                logger.info(f"Reached max_records limit: {max_records}")
                break
            
            # If page has fewer records than page_size, we've reached the end
            if page_count < page_size:
                logger.info(f"Reached end of data. Total fetched: {total_fetched}")
                break
    
    def get_total_count(self, bucket_name: str, where_clause: str = None):
        """
        Get total count of documents in bucket.
        
        Args:
            bucket_name: Name of the bucket
            where_clause: Optional WHERE clause (without WHERE keyword)
            
        Returns:
            int: Total count of documents
        """
        try:
            query = f"SELECT COUNT(*) as count FROM `{bucket_name}` USE INDEX(`#primary`)"
            if where_clause:
                query += f" WHERE {where_clause}"
            
            cluster = self.connect()
            query_options = QueryOptions(client_context_id=str(uuid.uuid4()))
            result = cluster.query(query, query_options)
            
            # Check for errors in result metadata
            if hasattr(result, 'metadata') and hasattr(result.metadata, 'warnings'):
                warnings = result.metadata.warnings
                if warnings:
                    logger.warning(f"Query warnings for bucket {bucket_name}: {warnings}")
            
            row_count = 0
            for row in result.rows():
                row_count += 1
                if isinstance(row, dict):
                    count_value = row.get('count', 0)
                    logger.debug(f"Count result for bucket {bucket_name}: {count_value}")
                    return count_value
                else:
                    try:
                        row_dict = dict(row)
                        count_value = row_dict.get('count', 0)
                        logger.debug(f"Count result for bucket {bucket_name}: {count_value}")
                        return count_value
                    except Exception as dict_err:
                        logger.debug(f"Error converting row to dict: {dict_err}")
                        if hasattr(row, 'count'):
                            count_value = row.count
                            return count_value
                        # Try accessing as dict-like
                        try:
                            count_value = row['count']
                            return count_value
                        except:
                            pass
            
            if row_count == 0:
                logger.warning(f"No rows returned from count query for bucket {bucket_name}")
            
            return 0
        except CouchbaseException as e:
            error_msg = str(e).lower()
            logger.error(f"Error getting total count for bucket {bucket_name}: {e}", exc_info=True)
            # Check if it's an index-related error
            if "index" in error_msg or "no index" in error_msg or "GSI" in error_msg:
                logger.error(f"Index may not be available for bucket {bucket_name}. Please ensure primary index is created and online.")
            raise e
        except Exception as e:
            logger.error(f"Unexpected error getting total count for bucket {bucket_name}: {e}", exc_info=True)
            raise e
        finally:
            self.close()
            # Removed gc.collect() to prevent debugger hang
    
    def check_bucket_exists(self, bucket_name: str):
        """
        Check if a bucket exists in the Couchbase cluster.
        
        Args:
            bucket_name: Name of the bucket to check
        
        Returns:
            bool: True if bucket exists, False otherwise
        """
        try:
            cluster = self.connect()
            # Try to access the bucket - this will raise exception if bucket doesn't exist
            bucket = cluster.bucket(bucket_name)
            # Try to access default collection to verify bucket is accessible
            collection = bucket.default_collection()
            return True
        except BucketNotFoundException:
            return False
        except CouchbaseException as e:
            error_msg = str(e).lower()
            if "bucket" in error_msg and "not found" in error_msg:
                return False
            # Other Couchbase exceptions might mean bucket exists but has other issues
            logger.warning(f"Error checking bucket {bucket_name}: {e}")
            return False
        except Exception as e:
            logger.warning(f"Unexpected error checking bucket {bucket_name}: {e}")
            return False
        finally:
            self.close()
            # Removed gc.collect() to prevent debugger hang
    
    def create_bucket(self, bucket_name: str, ram_quota_mb: int = 100, 
                     bucket_type: BucketType = BucketType.COUCHBASE, 
                     flush_enabled: bool = False, wait_ready: bool = True,
                     wait_timeout: int = 30):
        """
        Create a new bucket in Couchbase cluster.
        
        Args:
            bucket_name: Name of the bucket to create
            ram_quota_mb: RAM quota in MB (default: 100)
            bucket_type: Type of bucket (default: BucketType.COUCHBASE)
            flush_enabled: Enable flush capability (default: False)
            wait_ready: Wait for bucket to be ready after creation (default: True)
            wait_timeout: Timeout in seconds for waiting bucket to be ready (default: 30)
        
        Returns:
            bool: True if bucket was created successfully, False otherwise
        """
        try:
            cluster = self.connect()
            bucket_manager = cluster.buckets()
            
            # Check if bucket already exists
            try:
                existing_buckets = bucket_manager.get_all_buckets()
                bucket_names = [bucket.name for bucket in existing_buckets]
                if bucket_name in bucket_names:
                    logger.info(f"Bucket '{bucket_name}' already exists")
                    return True
            except Exception as e:
                logger.warning(f"Could not check existing buckets: {e}, proceeding with creation")
            
            # Create bucket settings
            bucket_settings = CreateBucketSettings(
                name=bucket_name,
                bucket_type=bucket_type,
                ram_quota_mb=ram_quota_mb,
                flush_enabled=flush_enabled
            )
            
            # Create the bucket
            logger.info(f"Creating bucket '{bucket_name}' with RAM quota: {ram_quota_mb}MB")
            bucket_manager.create_bucket(bucket_settings)
            logger.info(f"Bucket '{bucket_name}' creation initiated")
            
            # Wait for bucket to be ready
            if wait_ready:
                logger.info(f"Waiting for bucket '{bucket_name}' to be ready...")
                elapsed = 0
                while elapsed < wait_timeout:
                    try:
                        if self.check_bucket_exists(bucket_name):
                            logger.info(f"Bucket '{bucket_name}' is now ready")
                            return True
                    except Exception:
                        pass
                    time.sleep(1)
                    elapsed += 1
                    if elapsed % 5 == 0:
                        logger.debug(f"Still waiting for bucket '{bucket_name}' to be ready... ({elapsed}s/{wait_timeout}s)")
                
                logger.warning(f"Timeout waiting for bucket '{bucket_name}' to be ready after {wait_timeout} seconds")
                # Check one more time
                if self.check_bucket_exists(bucket_name):
                    logger.info(f"Bucket '{bucket_name}' is ready")
                    return True
                return False
            
            return True
        except CouchbaseException as e:
            error_msg = str(e).lower()
            if "already exists" in error_msg or "duplicate" in error_msg:
                logger.info(f"Bucket '{bucket_name}' already exists")
                return True
            logger.error(f"Error creating bucket '{bucket_name}': {e}", exc_info=True)
            raise e
        except Exception as e:
            logger.error(f"Unexpected error creating bucket '{bucket_name}': {e}", exc_info=True)
            raise e
        finally:
            self.close()
            # Removed gc.collect() to prevent debugger hang
    
    def upsert_document(self, bucket_name: str, document_id: str, document: dict):
        """
        Upsert a single document into Couchbase bucket.
        
        Args:
            bucket_name: Name of the bucket
            document_id: Document key/id
            document: Document data as dictionary
        """
        try:
            cluster = self.connect()
            if not self.bucket or self.bucket.name != bucket_name:
                self.bucket = cluster.bucket(bucket_name)
            
            collection = self.bucket.default_collection()
            collection.upsert(document_id, document)
            logger.debug(f"Upserted document {document_id} into bucket {bucket_name}")
        except CouchbaseException as e:
            logger.error(f"Error upserting document {document_id} into bucket {bucket_name}: {e}", exc_info=True)
            raise e
        except Exception as e:
            logger.error(f"Unexpected error upserting document: {e}", exc_info=True)
            raise e
        finally:
            self.close()
            # Removed gc.collect() to prevent debugger hang
    
    def upsert_documents(self, bucket_name: str, documents: list, batch_size: int = 100,
                        max_retries: int = 3, retry_delay: int = 2):
        """
        Upsert multiple documents into Couchbase bucket.
        
        Args:
            bucket_name: Name of the bucket
            documents: List of dictionaries. Expected structure:
                      - document["id"] is the document ID in bucket
                      - document[bucket_name] is the document value
                      - If value is not a dict, will try to convert it to dict
            batch_size: Number of documents to process in each batch (default: 100)
            max_retries: Maximum number of retries for failed upserts (default: 3)
            retry_delay: Delay in seconds between retries (default: 2)
        """
        try:
            cluster = self.connect()
            if not self.bucket or self.bucket.name != bucket_name:
                try:
                    self.bucket = cluster.bucket(bucket_name)
                except BucketNotFoundException:
                    error_msg = (
                        f"Bucket '{bucket_name}' not found in Couchbase cluster.\n"
                        f"Please create the bucket first using Couchbase Admin UI or REST API.\n"
                        f"To create bucket via REST API:\n"
                        f"  curl -X POST http://{self.config.host}:8091/pools/default/buckets \\\n"
                        f"    -u {self.config.user}:{self.config.password} \\\n"
                        f"    -d name={bucket_name} \\\n"
                        f"    -d bucketType=couchbase \\\n"
                        f"    -d ramQuotaMB=100"
                    )
                    logger.error(error_msg)
                    raise BucketNotFoundException(f"Bucket '{bucket_name}' not found. Please create it first.")
            
            collection = self.bucket.default_collection()
            total_upserted = 0
            skipped = 0
            
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i+batch_size]
                for doc in batch:
                    try:
                        if not isinstance(doc, dict):
                            skipped += 1
                            logger.warning(f"Skipping non-dict document: {type(doc)}")
                            continue
                        
                        # Extract document ID from document["id"]
                        doc_id = doc.get('id')
                        if not doc_id:
                            # Fallback: try other common ID fields
                            doc_id = doc.get('_id') or doc.get('key')
                            if not doc_id:
                                skipped += 1
                                logger.warning(f"Skipping document without ID: {doc}")
                                continue
                        
                        # Get document value from document[bucket_name]
                        doc_value = doc.get(bucket_name)
                        
                        # If bucket_name field doesn't exist, try 'value' field as fallback
                        if doc_value is None:
                            doc_value = doc.get('value')
                        
                        # If still no value, use the entire doc (excluding id fields)
                        if doc_value is None:
                            doc_value = {k: v for k, v in doc.items() if k not in ['id', '_id', 'key']}
                            if not doc_value:
                                skipped += 1
                                logger.warning(f"Skipping document with no value: {doc_id}")
                                continue
                        
                        # Convert value to dict if needed
                        if isinstance(doc_value, dict):
                            doc_content = doc_value.copy()
                        elif isinstance(doc_value, str):
                            # Try to parse string as JSON
                            try:
                                doc_content = json.loads(doc_value)
                                if not isinstance(doc_content, dict):
                                    # If parsed result is not a dict, wrap it
                                    doc_content = {"value": doc_content}
                            except (json.JSONDecodeError, ValueError):
                                # If not valid JSON, wrap the string as dict
                                doc_content = {"value": doc_value}
                        else:
                            # For other types (list, number, etc.), wrap in dict
                            doc_content = {"value": doc_value}
                        
                        # Ensure doc_content is a dict
                        if not isinstance(doc_content, dict):
                            doc_content = {"value": doc_content}
                        
                        # Upsert document with retry logic
                        upsert_success = False
                        last_error = None
                        for attempt in range(max_retries):
                            try:
                                collection.upsert(doc_id, doc_content)
                                total_upserted += 1
                                upsert_success = True
                                break
                            except (AmbiguousTimeoutException, CouchbaseException) as e:
                                last_error = e
                                if attempt < max_retries - 1:
                                    wait_time = retry_delay * (attempt + 1)  # Exponential backoff
                                    logger.warning(
                                        f"Timeout/Error upserting document '{doc_id}' "
                                        f"(attempt {attempt + 1}/{max_retries}). "
                                        f"Retrying in {wait_time} seconds... Error: {type(e).__name__}"
                                    )
                                    time.sleep(wait_time)
                                    # Reconnect if needed (timeout might indicate connection issue)
                                    try:
                                        if not self.bucket or self.bucket.name != bucket_name:
                                            self.bucket = cluster.bucket(bucket_name)
                                        collection = self.bucket.default_collection()
                                    except Exception as reconnect_err:
                                        logger.warning(f"Error reconnecting: {reconnect_err}")
                                else:
                                    # Last attempt failed
                                    logger.error(
                                        f"Failed to upsert document '{doc_id}' after {max_retries} attempts. "
                                        f"Error: {type(e).__name__}: {e}"
                                    )
                            except Exception as e:
                                # Non-retryable errors
                                last_error = e
                                logger.error(f"Non-retryable error upserting document '{doc_id}': {e}")
                                break
                        
                        if not upsert_success:
                            skipped += 1
                            logger.error(
                                f"Skipping document '{doc_id}' after {max_retries} failed attempts. "
                                f"Last error: {last_error}"
                            )
                        
                    except Exception as e:
                        skipped += 1
                        logger.error(f"Error processing document (ID: {doc.get('id', 'unknown')}): {e}", exc_info=True)
                        continue
                
                logger.debug(f"Upserted batch: {total_upserted} documents, skipped: {skipped}")
            
            logger.info(f"Upserted {total_upserted} documents into bucket {bucket_name}, skipped {skipped}")
            return total_upserted
        except BucketNotFoundException as e:
            logger.error(f"Bucket '{bucket_name}' not found. Please create the bucket first.", exc_info=True)
            raise e
        except CouchbaseException as e:
            logger.error(f"Error upserting documents into bucket {bucket_name}: {e}", exc_info=True)
            raise e
        except Exception as e:
            logger.error(f"Unexpected error upserting documents: {e}", exc_info=True)
            raise e
        finally:
            self.close()
            # Removed gc.collect() to prevent debugger hang