
import logging
import json
import pandas as pd
import gc   

from config import CouchbaseConfig, MongoDBConfig
from dal.couchbase_dal import CouchbaseDataAccess
from dal.mongodb_dal import MongoDBDataAccess
from logger_config import setup_logging, get_logger

from services.couchbase_service import CouchbaseService

# Setup logging
setup_logging(log_dir='logs', log_level=logging.DEBUG)
logger = get_logger(__name__)

RMS_BUCKET_LIST = ["rms_events","rms_journal","rms_rating_model","rms_read_model","rms_view","rms_write_model"]

def get_couchbase_data(bucket_name: str):
    try:
        cb_config = CouchbaseConfig()
        cb_dal = CouchbaseDataAccess(cb_config)
        all_data = []
        for page in cb_dal.get_all_data_paginated(
            bucket_name=bucket_name,
            page_size=1000
        ):
            all_data.extend(page)
        logger.info(f"Total records fetched: {len(all_data)}")
        return all_data
    except Exception as e:
        logger.error(f"Error getting data from bucket {bucket_name}: {e}", exc_info=True)
        return None
    finally:
        cb_dal.close()
        gc.collect()

def sample_get_couchbase_data():
    try:
        cb_config = CouchbaseConfig()
        cb_dal = CouchbaseDataAccess(cb_config)
        cb_service = CouchbaseService(cb_dal)
        # for bucket in RMS_BUCKET_LIST:
        #     cb_service.create_primary_index(bucket)
        
        # Get data from first bucket
        bucket_name = RMS_BUCKET_LIST[2]
        
        # Example 1: Get data with pagination using get_data_paginated
        logger.info("=" * 60)
        logger.info("Example 1: Fetching data with pagination (page 1, size 100)")
        logger.info("=" * 60)
        page_data = cb_dal.get_data_paginated(
            bucket_name=bucket_name,
            page_size=100,
            offset=0
        )
        logger.info(f"Fetched {len(page_data)} records from page 1")
        
        # Example 2: Get total count
        logger.info("=" * 60)
        logger.info("Example 2: Getting total document count")
        logger.info("=" * 60)
        total_count = cb_dal.get_total_count(bucket_name)
        logger.info(f"Total documents in {bucket_name}: {total_count}")
        
        # Example 3: Fetch all data using pagination generator
        logger.info("=" * 60)
        logger.info("Example 3: Fetching all data using pagination generator")
        logger.info("=" * 60)
        all_data = []
        page_size = 1000
        page_num = 0
        
        for page in cb_dal.get_all_data_paginated(
            bucket_name=bucket_name,
            page_size=page_size,
            max_records=500  # Limit to 500 records for demo
        ):
            page_num += 1
            all_data.extend(page)
            logger.info(f"Page {page_num}: {len(page)} records, Total so far: {len(all_data)}")
        
        logger.info(f"Total records fetched: {len(all_data)}")
        
        # Use the paginated data
        # data = all_data if all_data else page_data
        data = page_data
                
        # Convert to DataFrame and JSON
        if data and len(data) > 0:
            # If data is list of dicts with 'id' and 'value', flatten it
            if isinstance(data[0], dict) and 'value' in data[0]:
                rows = []
                for item in data:
                    row = item['value'].copy()
                    row['_id'] = item['id']  # Add document ID
                    rows.append(row)
                df = pd.DataFrame(rows)
            else:
                df = pd.DataFrame(data)
            
            logger.info(f"DataFrame shape: {df.shape}")
            logger.debug(f"First few rows:\n{df.head()}")
            
            # Convert to JSON
            json_data = json.dumps(data, indent=2, ensure_ascii=False, default=str)
            logger.debug(f"JSON output (first 500 chars):\n{json_data[:500] if len(json_data) > 500 else json_data}")
            
            # Save to JSON file
            with open(f'{bucket_name}.json', 'w', encoding='utf-8') as f:
                f.write(json_data)
            logger.info(f"Data saved to {bucket_name}.json ({len(data)} documents)")
        else:
            logger.warning("No data retrieved or data is empty.")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        cb_dal.close()
        gc.collect()

def add_data_to_mongodb(collection_name: str, data: list):
    try:
        mongo_config = MongoDBConfig()
        mongo_dal = MongoDBDataAccess(mongo_config)
        mongo_dal.add_documents(collection_name, data)
    except Exception as e:
        logger.error(f"Error adding data to MongoDB: {e}", exc_info=True)
    finally:
        mongo_dal.close()
        gc.collect()

if __name__ == "__main__":
    bucket_name = "rms_rating_model"
    data = get_couchbase_data(bucket_name)
    print(data)
    add_data_to_mongodb(bucket_name, data)
    