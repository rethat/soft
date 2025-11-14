from datetime import timedelta
import os
import sys
import gc
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import CouchbaseConfig
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import CouchbaseException
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
            logger.info(f"Attempting to connect to Couchbase cluster at {self.config.host} (timeout: {timeout.total_seconds()}s)")
            authenticator = PasswordAuthenticator(self.config.user, self.config.password)
            
            # Configure timeout options
            timeout_opts = ClusterTimeoutOptions(
                connect_timeout=timeout,
                kv_timeout=timedelta(seconds=30),
                query_timeout=timedelta(seconds=120),
                analytics_timeout=timedelta(seconds=120),
                search_timeout=timedelta(seconds=30),
                views_timeout=timedelta(seconds=30),
                management_timeout=timedelta(seconds=30)
            )
            
            cluster_options = ClusterOptions(authenticator, timeout_options=timeout_opts)
            self.cluster = Cluster(f"couchbase://{self.config.host}", cluster_options)
            
            # Wait for the cluster to be ready
            logger.debug(f"Waiting for cluster to be ready (timeout: {timeout.total_seconds()}s)...")
            self.cluster.wait_until_ready(timeout=timeout)
            
            logger.info(f"Successfully connected to Couchbase cluster at {self.config.host}")
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
            gc.collect()
    
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
            gc.collect()

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
            gc.collect()

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
            gc.collect()
    
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
            query = f"SELECT * FROM system:indexes WHERE keyspace_id = '{bucket_name}' AND name = '{index_name}'"
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
            else:
                state = str(getattr(index_info, 'state', '')).lower()
            is_online = state == 'online'
            return (True, is_online)
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
            gc.collect()
    
    def drop_primary_index(self, bucket_name: str):
        """
        Drop primary index on the bucket.
        """
        try:
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
            gc.collect()
            
    
    def create_primary_index(self, bucket_name: str):
        """
        Create a primary index on the bucket.
        This is required for N1QL queries without USE KEYS clause.
        
        Args:
            bucket_name: Name of the bucket
        """
        try:
            cluster = self.connect()
            query = f'CREATE PRIMARY INDEX ON `{bucket_name}` WITH {{"defer_build": true}};'
            query_options = QueryOptions(client_context_id=str(uuid.uuid4()))
            cluster.query(query, query_options).execute()
            logger.info(f"Primary index creation initiated for bucket: {bucket_name.upper()}")
        except CouchbaseException as e:
            logger.error(f"Error creating primary index: {e}", exc_info=True)
            raise e
        finally:
            self.close()
            gc.collect()

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
            gc.collect()

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
                logger.debug(f"Executing paginated query (attempt {attempt + 1}/{max_retries}): offset={offset}, limit={page_size}")
                return self.get_data(query, debug=debug)
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)  # Exponential backoff
                    logger.warning(
                        f"Error at offset {offset} (attempt {attempt + 1}/{max_retries}). "
                        f"Retrying in {wait_time} seconds... Error: {e}"
                    )
                    time.sleep(wait_time)
                    # Tiếp tục vòng lặp để retry
                    continue
                else:
                    # Đã hết số lần retry, ghi nhận lỗi và trả về empty list
                    logger.error(
                        f"Failed to get data for {bucket_name.upper()} at offset {offset} after {max_retries} attempts. "
                        f"Error: {e}. Skipping this page.",
                        exc_info=True
                    )
                    return []
        
        # Nếu đến đây thì đã hết retry
        logger.error(f"Failed to get data at offset {offset} after {max_retries} attempts. Skipping this page.")
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
            query = f"SELECT COUNT(*) as count FROM `{bucket_name}`"
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
            gc.collect()
