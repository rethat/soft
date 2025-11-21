"""
Load Testing Module for Couchbase and MongoDB
Tests concurrent user queries and generates performance metrics
"""
import os
import sys
import time
import json
import statistics
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import List, Dict, Tuple
import gc

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import CouchbaseConfig, MongoDBConfig
from dal.couchbase_dal import CouchbaseDataAccess
from dal.mongodb_dal import MongoDBDataAccess
from logger_config import get_logger

logger = get_logger(__name__)

# Thread-safe lock for statistics
stats_lock = Lock()


class LoadTestResult:
    """Stores results from a single query execution"""
    def __init__(self, db_type: str, query_type: str, success: bool, 
                 response_time: float, error: str = None, records_returned: int = 0):
        self.db_type = db_type  # 'couchbase' or 'mongodb'
        self.query_type = query_type  # 'select', 'count', etc.
        self.success = success
        self.response_time = response_time  # in seconds
        self.error = error
        self.records_returned = records_returned
        self.timestamp = datetime.now()


class LoadTester:
    """Main class for load testing both databases"""
    
    def __init__(self, bucket_name: str, collection_name: str, db_name: str = None):
        self.bucket_name = bucket_name
        self.collection_name = collection_name
        self.db_name = db_name
        self.cb_config = CouchbaseConfig()
        self.mongo_config = MongoDBConfig(db_name) if db_name else MongoDBConfig()
        self.results: List[LoadTestResult] = []
        
    def _execute_couchbase_query(self, query_type: str, query: str = None) -> LoadTestResult:
        """Execute a single query against Couchbase"""
        cb_dal = None
        start_time = time.time()
        try:
            cb_dal = CouchbaseDataAccess(self.cb_config)
            
            if query_type == "count":
                # Count query
                count = cb_dal.get_total_count(self.bucket_name)
                response_time = time.time() - start_time
                return LoadTestResult(
                    db_type="couchbase",
                    query_type=query_type,
                    success=True,
                    response_time=response_time,
                    records_returned=count
                )
            elif query_type == "select_all":
                # Select all with limit
                query = f"SELECT meta().id, * FROM `{self.bucket_name}` LIMIT 100"
                data = cb_dal.get_data(query)
                response_time = time.time() - start_time
                return LoadTestResult(
                    db_type="couchbase",
                    query_type=query_type,
                    success=True,
                    response_time=response_time,
                    records_returned=len(data) if data else 0
                )
            elif query_type == "select_paginated":
                # Paginated query
                data = cb_dal.get_data_paginated(
                    bucket_name=self.bucket_name,
                    page_size=100,
                    offset=0
                )
                response_time = time.time() - start_time
                return LoadTestResult(
                    db_type="couchbase",
                    query_type=query_type,
                    success=True,
                    response_time=response_time,
                    records_returned=len(data) if data else 0
                )
            elif query and query_type == "custom":
                # Custom query
                data = cb_dal.get_data(query)
                response_time = time.time() - start_time
                return LoadTestResult(
                    db_type="couchbase",
                    query_type=query_type,
                    success=True,
                    response_time=response_time,
                    records_returned=len(data) if data else 0
                )
            else:
                raise ValueError(f"Unknown query type: {query_type}")
                
        except Exception as e:
            response_time = time.time() - start_time
            logger.error(f"Couchbase query error: {e}", exc_info=True)
            return LoadTestResult(
                db_type="couchbase",
                query_type=query_type,
                success=False,
                response_time=response_time,
                error=str(e)
            )
        finally:
            if cb_dal:
                cb_dal.close()
            gc.collect()
    
    def _execute_mongodb_query(self, query_type: str, filter_query: dict = None) -> LoadTestResult:
        """Execute a single query against MongoDB"""
        mongo_dal = None
        start_time = time.time()
        try:
            mongo_dal = MongoDBDataAccess(self.mongo_config)
            mongo_dal.connect()
            collection = mongo_dal.database[self.collection_name]
            
            if query_type == "count":
                # Count query
                count = collection.count_documents({} if filter_query is None else filter_query)
                response_time = time.time() - start_time
                return LoadTestResult(
                    db_type="mongodb",
                    query_type=query_type,
                    success=True,
                    response_time=response_time,
                    records_returned=count
                )
            elif query_type == "select_all":
                # Find all with limit
                cursor = collection.find({} if filter_query is None else filter_query).limit(100)
                data = list(cursor)
                response_time = time.time() - start_time
                return LoadTestResult(
                    db_type="mongodb",
                    query_type=query_type,
                    success=True,
                    response_time=response_time,
                    records_returned=len(data)
                )
            elif query_type == "select_paginated":
                # Paginated query
                cursor = collection.find({} if filter_query is None else filter_query).skip(0).limit(100)
                data = list(cursor)
                response_time = time.time() - start_time
                return LoadTestResult(
                    db_type="mongodb",
                    query_type=query_type,
                    success=True,
                    response_time=response_time,
                    records_returned=len(data)
                )
            else:
                raise ValueError(f"Unknown query type: {query_type}")
                
        except Exception as e:
            response_time = time.time() - start_time
            logger.error(f"MongoDB query error: {e}", exc_info=True)
            return LoadTestResult(
                db_type="mongodb",
                query_type=query_type,
                success=False,
                response_time=response_time,
                error=str(e)
            )
        finally:
            if mongo_dal:
                mongo_dal.close()
            gc.collect()
    
    def _run_single_user_test(self, db_type: str, query_type: str, 
                              query: str = None, filter_query: dict = None) -> LoadTestResult:
        """Run a single query for one user"""
        if db_type == "couchbase":
            return self._execute_couchbase_query(query_type, query)
        elif db_type == "mongodb":
            return self._execute_mongodb_query(query_type, filter_query)
        else:
            raise ValueError(f"Unknown database type: {db_type}")
    
    def run_concurrent_test(self, num_users: int, query_type: str = "select_all",
                           db_types: List[str] = None, duration_seconds: int = None,
                           query: str = None, filter_query: dict = None) -> Dict:
        """
        Run concurrent load test
        
        Args:
            num_users: Number of concurrent users
            query_type: Type of query to run ('count', 'select_all', 'select_paginated', 'custom')
            db_types: List of database types to test ['couchbase', 'mongodb'] or None for both
            duration_seconds: Duration to run test in seconds (None = run once per user)
            query: Custom query string for Couchbase (if query_type='custom')
            filter_query: Custom filter dict for MongoDB (if query_type='custom')
        
        Returns:
            Dictionary with test results and statistics
        """
        if db_types is None:
            db_types = ["couchbase", "mongodb"]
        
        logger.info(f"Starting load test: {num_users} concurrent users, query_type={query_type}")
        logger.info(f"Testing databases: {db_types}")
        if duration_seconds:
            logger.info(f"Test duration: {duration_seconds} seconds")
        
        self.results = []
        start_time = time.time()
        
        def run_user_queries(db_type: str, user_id: int):
            """Run queries for a single user"""
            user_results = []
            end_time = start_time + duration_seconds if duration_seconds else start_time + 1
            
            while time.time() < end_time or not duration_seconds:
                result = self._run_single_user_test(
                    db_type=db_type,
                    query_type=query_type,
                    query=query,
                    filter_query=filter_query
                )
                user_results.append(result)
                
                if not duration_seconds:
                    # Run once if no duration specified
                    break
                    
                # Small delay between queries for same user
                time.sleep(0.1)
            
            return user_results
        
        # Run tests concurrently
        with ThreadPoolExecutor(max_workers=num_users * len(db_types)) as executor:
            futures = []
            
            # Submit tasks for each user and database
            for user_id in range(num_users):
                for db_type in db_types:
                    future = executor.submit(run_user_queries, db_type, user_id)
                    futures.append((future, db_type, user_id))
            
            # Collect results
            for future, db_type, user_id in futures:
                try:
                    user_results = future.result()
                    with stats_lock:
                        self.results.extend(user_results)
                except Exception as e:
                    logger.error(f"Error in user {user_id} for {db_type}: {e}", exc_info=True)
        
        total_time = time.time() - start_time
        
        # Calculate statistics
        stats = self._calculate_statistics()
        stats['total_test_time'] = total_time
        stats['num_users'] = num_users
        stats['query_type'] = query_type
        
        logger.info(f"Load test completed in {total_time:.2f} seconds")
        logger.info(f"Total queries executed: {stats['total_queries']}")
        
        return stats
    
    def _calculate_throughput(self, db_results: List[LoadTestResult], successful: List[LoadTestResult]) -> float:
        """Calculate throughput in queries per second"""
        if len(db_results) < 2 or len(successful) == 0:
            return 0.0
        
        timestamps = [r.timestamp.timestamp() for r in db_results]
        time_span = max(timestamps) - min(timestamps)
        
        if time_span <= 0:
            return float(len(successful))
        
        return len(successful) / time_span
    
    def _calculate_statistics(self) -> Dict:
        """Calculate statistics from test results"""
        stats = {
            'couchbase': {},
            'mongodb': {},
            'total_queries': len(self.results)
        }
        
        for db_type in ['couchbase', 'mongodb']:
            db_results = [r for r in self.results if r.db_type == db_type]
            
            if not db_results:
                stats[db_type] = {
                    'total_queries': 0,
                    'successful_queries': 0,
                    'failed_queries': 0,
                    'success_rate': 0.0,
                    'avg_response_time': 0.0,
                    'min_response_time': 0.0,
                    'max_response_time': 0.0,
                    'median_response_time': 0.0,
                    'p95_response_time': 0.0,
                    'p99_response_time': 0.0,
                    'throughput_qps': 0.0,
                    'total_records_returned': 0
                }
                continue
            
            successful = [r for r in db_results if r.success]
            failed = [r for r in db_results if not r.success]
            
            response_times = [r.response_time for r in successful]
            
            if response_times:
                response_times_sorted = sorted(response_times)
                n = len(response_times_sorted)
                
                stats[db_type] = {
                    'total_queries': len(db_results),
                    'successful_queries': len(successful),
                    'failed_queries': len(failed),
                    'success_rate': len(successful) / len(db_results) * 100 if db_results else 0.0,
                    'avg_response_time': statistics.mean(response_times),
                    'min_response_time': min(response_times),
                    'max_response_time': max(response_times),
                    'median_response_time': statistics.median(response_times),
                    'p95_response_time': response_times_sorted[int(n * 0.95)] if n > 0 else 0.0,
                    'p99_response_time': response_times_sorted[int(n * 0.99)] if n > 0 else 0.0,
                    'throughput_qps': self._calculate_throughput(db_results, successful),
                    'total_records_returned': sum(r.records_returned for r in successful),
                    'errors': [r.error for r in failed[:10]]  # First 10 errors
                }
            else:
                stats[db_type] = {
                    'total_queries': len(db_results),
                    'successful_queries': 0,
                    'failed_queries': len(failed),
                    'success_rate': 0.0,
                    'avg_response_time': 0.0,
                    'min_response_time': 0.0,
                    'max_response_time': 0.0,
                    'median_response_time': 0.0,
                    'p95_response_time': 0.0,
                    'p99_response_time': 0.0,
                    'throughput_qps': 0.0,
                    'total_records_returned': 0,
                    'errors': [r.error for r in failed[:10]]
                }
        
        return stats
    
    def get_all_results(self) -> List[LoadTestResult]:
        """Get all test results"""
        return self.results
    
    def save_results(self, filename: str):
        """Save results to JSON file"""
        results_data = {
            'test_info': {
                'bucket_name': self.bucket_name,
                'collection_name': self.collection_name,
                'timestamp': datetime.now().isoformat()
            },
            'results': [
                {
                    'db_type': r.db_type,
                    'query_type': r.query_type,
                    'success': r.success,
                    'response_time': r.response_time,
                    'error': r.error,
                    'records_returned': r.records_returned,
                    'timestamp': r.timestamp.isoformat()
                }
                for r in self.results
            ]
        }
        
        with open(filename, 'w') as f:
            json.dump(results_data, f, indent=2)
        
        logger.info(f"Results saved to {filename}")

