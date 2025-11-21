"""
Main script to run load tests and generate reports
Tests Couchbase and MongoDB with concurrent users and generates comparison reports
"""
import os
import sys
import json
import time
import logging
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from load_test import LoadTester
from report_generator import ReportGenerator
from logger_config import setup_logging, get_logger

# Setup logging
setup_logging(log_dir='logs', log_level=logging.INFO)
logger = get_logger(__name__)

# Load database settings
DB_SETTING = json.load(open('./src/dbsetting.json', 'r'))


def run_load_test_suite(bucket_name: str, collection_name: str, db_name: str = None,
                       user_counts: list = None, query_type: str = "select_all",
                       duration_seconds: int = None):
    """
    Run a complete load test suite with multiple concurrent user scenarios
    
    Args:
        bucket_name: Name of Couchbase bucket
        collection_name: Name of MongoDB collection
        db_name: Name of MongoDB database
        user_counts: List of concurrent user counts to test (e.g., [10, 50, 100, 200, 500])
        query_type: Type of query to run ('count', 'select_all', 'select_paginated')
        duration_seconds: Duration to run each test in seconds (None = run once per user)
    
    Returns:
        List of test result statistics
    """
    if user_counts is None:
        # Default: test with increasing concurrent users
        user_counts = [10, 50, 100, 200, 500, 1000]
    
    logger.info("=" * 80)
    logger.info(f"Bắt đầu Load Test Suite cho {bucket_name}/{collection_name}")
    logger.info(f"Số lượng concurrent users sẽ test: {user_counts}")
    logger.info(f"Loại query: {query_type}")
    if duration_seconds:
        logger.info(f"Thời gian mỗi test: {duration_seconds} giây")
    logger.info("=" * 80)
    
    tester = LoadTester(bucket_name, collection_name, db_name)
    all_results = []
    
    for num_users in user_counts:
        logger.info(f"\n{'='*80}")
        logger.info(f"Đang test với {num_users} concurrent users...")
        logger.info(f"{'='*80}\n")
        
        try:
            # Run test
            stats = tester.run_concurrent_test(
                num_users=num_users,
                query_type=query_type,
                duration_seconds=duration_seconds
            )
            
            all_results.append(stats)
            
            # Log summary
            logger.info(f"\nKết quả cho {num_users} users:")
            logger.info(f"  Couchbase - Avg Response Time: {stats['couchbase']['avg_response_time']*1000:.2f}ms")
            logger.info(f"  MongoDB   - Avg Response Time: {stats['mongodb']['avg_response_time']*1000:.2f}ms")
            logger.info(f"  Couchbase - Throughput: {stats['couchbase']['throughput_qps']:.2f} QPS")
            logger.info(f"  MongoDB   - Throughput: {stats['mongodb']['throughput_qps']:.2f} QPS")
            logger.info(f"  Couchbase - Success Rate: {stats['couchbase']['success_rate']:.2f}%")
            logger.info(f"  MongoDB   - Success Rate: {stats['mongodb']['success_rate']:.2f}%")
            
            # Save intermediate results
            results_file = f"reports/{bucket_name}_test_results_{num_users}users.json"
            os.makedirs("reports", exist_ok=True)
            tester.save_results(results_file)
            
            # Small delay between tests
            logger.info(f"\nChờ 5 giây trước khi chạy test tiếp theo...\n")
            time.sleep(5)
            
        except Exception as e:
            logger.error(f"Lỗi khi test với {num_users} users: {e}", exc_info=True)
            continue
    
    return all_results


def main():
    """Main function to run load tests for all buckets/collections"""
    # Configuration
    user_counts = [10, 50, 100, 200, 500, 1000, 2000, 5000]  # Test với số lượng users rất lớn
    query_type = "select_all"  # Có thể thay đổi: 'count', 'select_all', 'select_paginated'
    duration_seconds = 30  # Chạy mỗi test trong 30 giây (None = chạy 1 lần mỗi user)
    
    logger.info("=" * 80)
    logger.info("CHƯƠNG TRÌNH LOAD TEST VÀ SO SÁNH COUCHBASE vs MONGODB")
    logger.info("=" * 80)
    logger.info(f"Cấu hình:")
    logger.info(f"  - Số lượng concurrent users: {user_counts}")
    logger.info(f"  - Loại query: {query_type}")
    logger.info(f"  - Thời gian mỗi test: {duration_seconds} giây")
    logger.info("=" * 80)
    
    all_test_results = {}
    
    # Run tests for each bucket/collection
    for db_name, bucket_names in DB_SETTING.items():
        for bucket_name in bucket_names:
            # MongoDB collection name is same as bucket name
            collection_name = bucket_name
            
            logger.info(f"\n\n{'#'*80}")
            logger.info(f"# Testing: {bucket_name} (DB: {db_name})")
            logger.info(f"{'#'*80}\n")
            
            try:
                # Run load test suite
                results = run_load_test_suite(
                    bucket_name=bucket_name,
                    collection_name=collection_name,
                    db_name=db_name,
                    user_counts=user_counts,
                    query_type=query_type,
                    duration_seconds=duration_seconds
                )
                
                all_test_results[bucket_name] = results
                
                # Generate report
                logger.info(f"\nĐang tạo báo cáo cho {bucket_name}...")
                report_gen = ReportGenerator(output_dir="reports")
                report_path = report_gen.generate_report(
                    test_results=results,
                    bucket_name=bucket_name,
                    collection_name=collection_name
                )
                logger.info(f"Báo cáo đã được tạo: {report_path}")
                
            except Exception as e:
                logger.error(f"Lỗi khi test {bucket_name}: {e}", exc_info=True)
                continue
    
    # Generate summary report
    logger.info("\n\n" + "=" * 80)
    logger.info("TẤT CẢ CÁC TEST ĐÃ HOÀN THÀNH")
    logger.info("=" * 80)
    logger.info(f"Tổng số buckets đã test: {len(all_test_results)}")
    for bucket_name, results in all_test_results.items():
        logger.info(f"  - {bucket_name}: {len(results)} test scenarios")
    logger.info("\nCác báo cáo HTML đã được tạo trong thư mục 'reports/'")
    logger.info("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\nTest bị dừng bởi người dùng")
    except Exception as e:
        logger.error(f"Lỗi không mong muốn: {e}", exc_info=True)
        sys.exit(1)

