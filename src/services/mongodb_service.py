import gc
import json
import re
from dal.mongodb_dal import MongoDBDataAccess
from logger_config import get_logger

logger = get_logger(__name__)

class MongoDBService:

    def __init__(self,  mongo_dal: MongoDBDataAccess = None, mapping_id: bool = False):
        self.mongo_dal = mongo_dal
        self.mapping_id = mapping_id

    def _normalize_json_string(self, json_str: str) -> str:
        """
        Normalize JSON string by converting numbers with leading zeros to strings.
        Example: ': 00000' -> ': "00000"', ': 00123' -> ': "00123"'
        Note: Single '0' is valid JSON and won't be converted.
        """
        # First, clean up the string - remove leading/trailing whitespace and newlines
        normalized = json_str.strip()
        
        # Pattern to match numbers with leading zeros that are invalid in JSON
        # We need to match: colon, whitespace, then number starting with at least 2 zeros
        # The tricky part is ensuring we don't match valid numbers like 0, 0.5, etc.
        
        def replace_leading_zero(match):
            # Extract the full match (colon + whitespace + number)
            prefix = match.group(1)  # colon and whitespace
            number_str = match.group(2)  # the number with leading zeros
            # Convert to string with quotes
            return f'{prefix}"{number_str}"'
        
        # Match pattern: 
        # - (:\s+) - colon followed by one or more whitespace characters
        # - (0{2,}\d*) - at least 2 zeros optionally followed by more digits
        # - This ensures we match "key": 00000 but not "key": 0 or "key": 0.5
        # The pattern will work in nested objects too since it's context-independent
        pattern = r'(:\s+)(0{2,}\d*)'
        normalized = re.sub(pattern, replace_leading_zero, normalized)
        
        # Wrap the entire string in braces if it's not already wrapped
        normalized = normalized.strip()
        if not normalized.startswith('{'):
            normalized = '{' + normalized + '}'
        
        return normalized

    def add_document(self, collection_name: str, document: dict):
        try:
            # Remove collection_name from document if it exists
            if isinstance(document, dict) and collection_name in document:
                collection_value = document[collection_name]
                processed_doc = None
                
                # If collection_value is a dict, use it directly
                if isinstance(collection_value, dict):
                    processed_doc = collection_value.copy()
                # If collection_value is a string, try to parse it as JSON
                elif isinstance(collection_value, str):
                    try:
                        # First try to parse directly
                        processed_doc = json.loads(collection_value)
                        if not isinstance(processed_doc, dict):
                            # If parsed result is not a dict, keep original structure
                            processed_doc = None
                    except (json.JSONDecodeError, ValueError) as e:
                        # If direct parse fails, try to normalize the string first
                        try:
                            normalized_str = self._normalize_json_string(collection_value)
                            processed_doc = json.loads(normalized_str)
                            if not isinstance(processed_doc, dict):
                                processed_doc = None
                        except (json.JSONDecodeError, ValueError) as e2:
                            logger.warning(f"Failed to parse JSON string for collection {collection_name}: {e2}")
                            logger.debug(f"Normalized string (first 500 chars): {normalized_str[:500]}")
                            processed_doc = None
                
                if processed_doc is not None:
                    # Preserve _id from outer level if it exists
                    if '_id' in document:
                        processed_doc['_id'] = document['_id']
                    # Also preserve any other top-level keys except collection_name
                    for key, value in document.items():
                        if key != collection_name and key not in processed_doc:
                            processed_doc[key] = value
                    document = processed_doc
                else:
                    # If collection_value is not a dict or cannot be parsed, keep the original document structure
                    # but remove the collection_name key
                    processed_doc = document.copy()
                    processed_doc.pop(collection_name, None)
                    document = processed_doc
            
            self.mongo_dal.add_document(collection_name, document)
        except Exception as e:
            logger.error(f"Error adding document to MongoDB: {e}", exc_info=True)
            raise e

    def add_documents(self, collection_name: str, documents: list):
        try:
            if not documents:
                logger.warning(f"No documents to add to collection {collection_name}")
                return
                
            _documents = []
            skipped_count = 0
            
            for doc in documents:
                # Skip None or non-dict values
                if doc is None:
                    skipped_count += 1
                    logger.warning(f"Skipping None document in collection {collection_name}")
                    continue
                    
                if not isinstance(doc, dict):
                    skipped_count += 1
                    logger.warning(f"Skipping non-dict document in collection {collection_name}: {type(doc)}")
                    continue
                
                # Remove collection_name from document if it exists
                if collection_name in doc:
                    collection_value = doc[collection_name]
                    processed_doc = None
                    
                    # If collection_value is a dict, use it directly
                    if isinstance(collection_value, dict):
                        processed_doc = collection_value.copy()
                    # If collection_value is a string, try to parse it as JSON
                    elif isinstance(collection_value, str):
                        try:
                            # First try to parse directly
                            processed_doc = json.loads(collection_value)
                            if not isinstance(processed_doc, dict):
                                # If parsed result is not a dict, keep original structure
                                processed_doc = None
                        except (json.JSONDecodeError, ValueError) as e:
                            # If direct parse fails, try to normalize the string first
                            try:
                                normalized_str = self._normalize_json_string(collection_value)
                                processed_doc = json.loads(normalized_str)
                                if not isinstance(processed_doc, dict):
                                    processed_doc = None
                            except (json.JSONDecodeError, ValueError) as e2:
                                logger.warning(f"Failed to parse JSON string for collection {collection_name}: {e2}")
                                logger.debug(f"Normalized string (first 500 chars): {normalized_str[:500]}")
                                processed_doc = None
                    
                    if processed_doc is not None:
                        document = processed_doc
                        #preserve any other top-level keys except collection_name
                        for key, value in doc.items():
                            if key != collection_name and key not in document:
                                document[key] = value
                    else:
                        # If collection_value is not a dict or cannot be parsed, keep the original document structure
                        # but remove the collection_name key
                        document = doc.copy()
                        document.pop(collection_name, None)
                else:
                    document = doc.copy()
                
                # Ensure document is still a dict after processing
                if not isinstance(document, dict):
                    skipped_count += 1
                    logger.warning(f"Skipping document after processing in collection {collection_name}: result is not a dict")
                    continue
                
                # Handle id mapping if enabled
                if self.mapping_id and isinstance(document, dict):
                    document['_id'] = doc['id']
                    if 'id' in document and 'value' in document:
                        if isinstance(document['value'], dict):
                            inner_doc = document['value'].copy()
                            inner_doc['_id'] = document['id']
                            if 'id' in inner_doc:
                                del inner_doc['id']
                            document = inner_doc
                    elif 'id' in document:
                        # document['_id'] = document['id']
                        del document['id']
                
                # Final check before appending
                if isinstance(document, dict):
                    _documents.append(document)
                else:
                    skipped_count += 1
                    logger.warning(f"Skipping document after id mapping in collection {collection_name}: result is not a dict")
            
            if not _documents:
                logger.warning(f"No valid documents to insert into collection {collection_name} (skipped {skipped_count} invalid documents)")
                return
            
            if skipped_count > 0:
                logger.warning(f"Skipped {skipped_count} invalid documents when adding to collection {collection_name}")
            
            self.mongo_dal.add_documents(collection_name, _documents)
            logger.info(f"Successfully added {len(_documents)} documents to MongoDB collection {collection_name}")
        except Exception as e:
            logger.error(f"Error adding documents to MongoDB: {e}", exc_info=True)
            raise e
        finally:
            self.mongo_dal.close()
            gc.collect()

    def drop_collections(self):
        try:
            self.mongo_dal.drop_collections()
        except Exception as e:
            logger.error(f"Error dropping collections from MongoDB: {e}", exc_info=True)
            raise e
        finally:
            self.mongo_dal.close()
            gc.collect()


    def process_rms_data(self, bucket_name: str, page: int, data: list):
        'process RMS data then insert into MongoDB'
        try:
            default_group_key = "Others"
            group = {}
            skipped_count = 0
            
            def convert_to_dict(value):
                if isinstance(value, str):
                    try:
                        # First try to parse directly
                        processed_doc = json.loads(value)
                        if not isinstance(processed_doc, dict):
                            # If parsed result is not a dict, keep original structure
                            return None
                    except (json.JSONDecodeError, ValueError) as e:
                        # If direct parse fails, try to normalize the string first
                        try:
                            normalized_str = self._normalize_json_string(value)
                            processed_doc = json.loads(normalized_str)
                            if not isinstance(processed_doc, dict):
                                return None
                        except (json.JSONDecodeError, ValueError) as e2:
                            logger.warning(f"Failed to parse JSON string for collection {bucket_name}: {e2}")
                            return None
                    return processed_doc
                elif isinstance(value, dict):
                    return value
                else:
                    return None

            for doc in data:
                try:
                    if not isinstance(doc, dict):
                        skipped_count += 1
                        logger.warning(f"Skipping non-dict document in {bucket_name}: {type(doc)}")
                        continue
                        
                    _document = doc.copy()
                    rms_dict = convert_to_dict(_document.get(bucket_name))
                    
                    # Skip if rms_dict is None or not a dict
                    if not isinstance(rms_dict, dict):
                        skipped_count += 1
                        doc_id = _document.get('id', 'unknown')
                        logger.warning(f"Skipping document with id '{doc_id}' in {bucket_name}: could not convert to dict")
                        continue
                    
                    typekey = rms_dict.get('typekey') if isinstance(rms_dict, dict) else None
                    
                    group_key = typekey if typekey else default_group_key
                    if group_key not in group:
                        group[group_key] = []
                    
                    # Ensure rms_dict is a dict before processing
                    if isinstance(rms_dict, dict):
                        rms_dict = rms_dict.copy()  # Make a copy to avoid modifying original
                        rms_dict['bucket_name'] = bucket_name

                    if self.mapping_id and isinstance(rms_dict, dict):
                        rms_dict['_id'] = doc['id'] # add bucket name there to avoid duplicate id
                        if 'id' in rms_dict and 'value' in rms_dict:
                            if isinstance(rms_dict['value'], dict):
                                inner_doc = rms_dict['value'].copy()
                                inner_doc['_id'] = rms_dict['id']
                                if 'id' in inner_doc:
                                    del inner_doc['id']
                                rms_dict = inner_doc
                        # elif 'id' in rms_dict:
                        #     rms_dict['_id'] = rms_dict['id']
                        #     del rms_dict['id']
                    
                    # Only append if rms_dict is still a dict after processing
                    if isinstance(rms_dict, dict):
                        group[group_key].append(rms_dict)
                    else:
                        skipped_count += 1
                        logger.warning(f"Skipping document after processing in {bucket_name}: result is not a dict")
                except Exception as e:
                    skipped_count += 1
                    doc_id = doc.get('id', 'unknown') if isinstance(doc, dict) else 'unknown'
                    logger.error(f"Error processing document with id '{doc_id}' in {bucket_name}: {e}", exc_info=True)
                    continue
            
            if skipped_count > 0:
                logger.warning(f"Skipped {skipped_count} invalid documents in {bucket_name}")
            
            # with(open(f'{bucket_name}_group.json', 'w')) as f:
            #     json.dump(group, f, indent=2, ensure_ascii=False)

            # Insert data into MongoDB
            for group_key, group_value in group.items():
                if group_value:  # Only process non-empty groups
                    # Filter out any None or non-dict values before inserting
                    valid_docs = [doc for doc in group_value if isinstance(doc, dict)]
                    if valid_docs:
                        try:
                            BATCH_SIZE = 500
                            for i in range(0, len(valid_docs), BATCH_SIZE):
                                batch = valid_docs[i:i+BATCH_SIZE]
                                self.mongo_dal.add_documents(group_key, batch, max_retries=5, retry_delay=3)
                        except Exception as e:
                            logger.error(f"ERROR_INSERT_MONGODB: {bucket_name.upper()}[{page}] - '{group_key}' | Error:{e}", exc_info=True)
                            with open(f'{bucket_name}_{page}_error_{group_key}.json', 'w', encoding='utf-8') as f:
                                json.dump(valid_docs, f, indent=2, ensure_ascii=False)
                            continue
                    else:
                        logger.warning(f"No valid documents to insert for group '{group_key}' in {bucket_name}")

        except Exception as e:
            logger.error(f"Error processing RMS data in {bucket_name}: {e}", exc_info=True)
            raise e
        finally:
            self.mongo_dal.close()
            gc.collect()

    def _convert_doc_value(self, doc_value):
        """
        Convert doc_value from string to dict if needed.
        
        Args:
            doc_value: Value to convert (can be str, dict, or other)
            
        Returns:
            dict or None: Converted dict or None if conversion failed
        """
        if isinstance(doc_value, dict):
            return doc_value
        elif isinstance(doc_value, str):
            try:
                # First try to parse directly
                processed_doc = json.loads(doc_value)
                if isinstance(processed_doc, dict):
                    return processed_doc
            except (json.JSONDecodeError, ValueError):
                # If direct parse fails, try to normalize the string first
                try:
                    normalized_str = self._normalize_json_string(doc_value)
                    processed_doc = json.loads(normalized_str)
                    if isinstance(processed_doc, dict):
                        return processed_doc
                except (json.JSONDecodeError, ValueError):
                    return None
        return None

    def _check_duplicates_batch(self, collection_name: str, doc_ids: list):
        """
        Check for duplicate documents in batch using $in operator.
        Much more efficient than checking one by one.
        
        Args:
            collection_name: Name of the collection
            doc_ids: List of document _id values to check
            
        Returns:
            set: Set of _id values that already exist in the collection
        """
        if not doc_ids:
            return set()
        
        try:
            self.mongo_dal.connect()
            # Query all _id values at once using $in operator
            existing_docs = self.mongo_dal.database[collection_name].find(
                {"_id": {"$in": doc_ids}},
                {"_id": 1}  # Only return _id field
            )
            existing_ids = {doc["_id"] for doc in existing_docs}
            return existing_ids
        except Exception as e:
            logger.error(f"Error checking duplicates in collection {collection_name}: {e}", exc_info=True)
            return set()
        finally:
            self.mongo_dal.close()
            gc.collect()

    def process_mechoice_keep_structure(self, bucket_name: str, data: list, batch_size: int = 500):
        """
        Process documents with keep_structure strategy:
        - Giữ nguyên cấu trúc bucket
        - _id mapping với bucket["id"]
        - value bằng bucket[bucket_name]
        - Thêm field bucket_name
        - Collection = bucket_name
        
        Args:
            bucket_name: Name of the bucket
            data: List of documents from Couchbase
            batch_size: Batch size for bulk insert (default: 500)
        """
        try:
            collection_name = bucket_name
            processed_docs = []
            skipped_count = 0
            
            for doc in data:
                try:
                    if not isinstance(doc, dict):
                        skipped_count += 1
                        continue
                    
                    doc_id = doc.get('id')
                    if not doc_id:
                        skipped_count += 1
                        logger.warning(f"Document missing 'id' field in {bucket_name}")
                        continue
                    
                    # Lấy value từ bucket[bucket_name]
                    doc_value = doc.get(bucket_name)
                    if doc_value is None:
                        skipped_count += 1
                        logger.warning(f"Document missing '{bucket_name}' field, doc_id: {doc_id}")
                        continue
                    
                    # Convert value to dict
                    doc_value = self._convert_doc_value(doc_value)
                    if not isinstance(doc_value, dict):
                        skipped_count += 1
                        logger.warning(f"Document value is not a dict for doc_id: {doc_id}")
                        continue
                    
                    # Tạo document mới với _id và thêm bucket_name
                    final_doc = doc_value.copy()
                    final_doc['_id'] = doc_id
                    final_doc['bucket_name'] = bucket_name
                    processed_docs.append(final_doc)
                    
                except Exception as e:
                    skipped_count += 1
                    doc_id = doc.get('id', 'unknown') if isinstance(doc, dict) else 'unknown'
                    logger.error(f"Error processing document with id '{doc_id}' in {bucket_name}: {e}", exc_info=True)
                    continue
            
            if not processed_docs:
                logger.warning(f"No valid documents to insert for {bucket_name}")
                return
            
            # Batch check duplicates
            doc_ids = [doc['_id'] for doc in processed_docs]
            existing_ids = self._check_duplicates_batch(collection_name, doc_ids)
            
            # Filter out duplicates
            unique_docs = [doc for doc in processed_docs if doc['_id'] not in existing_ids]
            duplicate_count = len(processed_docs) - len(unique_docs)
            
            if duplicate_count > 0:
                logger.info(f"[{bucket_name.upper()}]: Found {duplicate_count} duplicate documents in {collection_name}, skipping")
            
            # Insert in batches
            if unique_docs:
                for i in range(0, len(unique_docs), batch_size):
                    batch = unique_docs[i:i+batch_size]
                    try:
                        self.mongo_dal.add_documents(collection_name, batch, max_retries=5, retry_delay=3)
                        logger.debug(f"Inserted batch {i//batch_size + 1} ({len(batch)} documents) into {collection_name}")
                    except Exception as e:
                        logger.error(f"Error inserting batch into {collection_name}: {e}", exc_info=True)
                        raise e
                
                logger.info(f"Successfully processed {len(unique_docs)} documents for {bucket_name} (skipped {skipped_count + duplicate_count} documents)")
            else:
                logger.warning(f"All documents are duplicates for {bucket_name}")
                
        except Exception as e:
            logger.error(f"Error processing keep_structure data for {bucket_name}: {e}", exc_info=True)
            raise e
        finally:
            self.mongo_dal.close()
            gc.collect()

    def process_mechoice_restructure(self, bucket_name: str, data: list, batch_size: int = 500):
        """
        Process documents with restructure strategy:
        - Collection name = doc_value['_type'] (giá trị của _type field)
        - id = doc["id"]
        - Thêm field bucket_name
        
        Args:
            bucket_name: Name of the bucket
            data: List of documents from Couchbase
            batch_size: Batch size for bulk insert (default: 500)
        """
        try:
            # Group documents by collection name (bucket_name + _type)
            collection_groups = {}
            skipped_count = 0
            
            for doc in data:
                try:
                    if not isinstance(doc, dict):
                        skipped_count += 1
                        continue
                    
                    doc_id = doc.get('id')
                    if not doc_id:
                        skipped_count += 1
                        logger.warning(f"[{bucket_name.upper()}]: Document missing 'id'")
                        continue
                    
                    # Lấy value từ bucket[bucket_name]
                    doc_value = doc.get(bucket_name)
                    if doc_value is None:
                        skipped_count += 1
                        logger.warning(f"[{bucket_name.upper()}]: Document missing '{bucket_name}' field, doc_id: {doc_id}")
                        continue
                    
                    # Convert value to dict
                    doc_value = self._convert_doc_value(doc_value)
                    if not isinstance(doc_value, dict):
                        skipped_count += 1
                        logger.warning(f"[{bucket_name.upper()}]: Document value is not a dict for doc_id: {doc_id}")
                        continue
                    
                    # Lấy _type từ doc_value
                    doc_type = doc_value.get('_type')
                    if not doc_type:
                        skipped_count += 1
                        logger.warning(f"[{bucket_name.upper()}]: Document missing '_type' , doc_id: {doc_id}")
                        continue
                    
                    # Tạo collection name: giá trị của _type (không thêm bucket_name prefix)
                    collection_name = doc_type
                    
                    # Tạo document mới với _id và thêm bucket_name
                    final_doc = doc_value.copy()
                    final_doc['_id'] = doc_id
                    final_doc['bucket_name'] = bucket_name
                    
                    if collection_name not in collection_groups:
                        collection_groups[collection_name] = []
                    collection_groups[collection_name].append(final_doc)
                    
                except Exception as e:
                    skipped_count += 1
                    doc_id = doc.get('id', 'unknown') if isinstance(doc, dict) else 'unknown'
                    logger.error(f"Error processing document with id '{doc_id}' in {bucket_name}: {e}", exc_info=True)
                    continue
            
            if not collection_groups:
                logger.warning(f"No valid documents to insert for {bucket_name}")
                return
            
            # Process each collection group
            total_inserted = 0
            total_duplicates = 0
            
            for collection_name, docs in collection_groups.items():
                # Batch check duplicates
                doc_ids = [doc['_id'] for doc in docs]
                existing_ids = self._check_duplicates_batch(collection_name, doc_ids)
                
                # Filter out duplicates
                unique_docs = [doc for doc in docs if doc['_id'] not in existing_ids]
                duplicate_count = len(docs) - len(unique_docs)
                total_duplicates += duplicate_count
                
                if duplicate_count > 0:
                    logger.info(f"Found {duplicate_count} duplicate documents in {collection_name}, skipping")
                
                # Insert in batches
                if unique_docs:
                    for i in range(0, len(unique_docs), batch_size):
                        batch = unique_docs[i:i+batch_size]
                        try:
                            self.mongo_dal.add_documents(collection_name, batch, max_retries=5, retry_delay=3)
                            logger.debug(f"Inserted batch {i//batch_size + 1} ({len(batch)} documents) into {collection_name}")
                        except Exception as e:
                            logger.error(f"Error inserting batch into {collection_name}: {e}", exc_info=True)
                            raise e
                    
                    total_inserted += len(unique_docs)
                    logger.info(f"Successfully inserted {len(unique_docs)} documents into {collection_name}")
                else:
                    logger.warning(f"All documents are duplicates for {collection_name}")
            
            logger.info(f"Successfully processed {total_inserted} documents for {bucket_name} "
                       f"(skipped {skipped_count + total_duplicates} documents)")
                
        except Exception as e:
            logger.error(f"Error processing restructure data for {bucket_name}: {e}", exc_info=True)
            raise e
        finally:
            self.mongo_dal.close()
            gc.collect()

    def process_mechoice_missing_type(self, bucket_name: str, data: list, batch_size: int = 500):
        """
        Process documents with missing_type strategy:
        - Nếu doc[bucket_name] có _type thì collection_name = doc_value['_type']
        - Nếu không có _type:
          - Kiểm tra doc_value.get('count') -> collection_name = 'Count'
          - Kiểm tra doc_value.get('counter') -> collection_name = 'Counter'
          - Nếu không có cả hai -> collection_name = 'mechoice_metadata'
        - Thêm field bucket_name
        
        Args:
            bucket_name: Name of the bucket
            data: List of documents from Couchbase
            batch_size: Batch size for bulk insert (default: 500)
        """
        try:
            # Group documents by collection name
            collection_groups = {}
            skipped_count = 0
            
            for doc in data:
                try:
                    if not isinstance(doc, dict):
                        skipped_count += 1
                        continue
                    
                    doc_id = doc.get('id')
                    if not doc_id:
                        skipped_count += 1
                        logger.warning(f"Document missing 'id' field in {bucket_name}")
                        continue
                    
                    # Lấy value từ bucket[bucket_name]
                    doc_value = doc.get(bucket_name)
                    if doc_value is None:
                        skipped_count += 1
                        logger.warning(f"Document missing '{bucket_name}' field, doc_id: {doc_id}")
                        continue
                    
                    # Convert value to dict
                    doc_value = self._convert_doc_value(doc_value)
                    if not isinstance(doc_value, dict):
                        skipped_count += 1
                        logger.warning(f"Document value is not a dict for doc_id: {doc_id}")
                        continue
                    
                    # Kiểm tra _type
                    doc_type = doc_value.get('_type')
                    if doc_type:
                        # Có _type -> collection name = giá trị của _type
                        collection_name = doc_type
                    else:
                        # Missing _type -> kiểm tra các field khác
                        if doc_value.get('count') is not None:
                            collection_name = 'Count'
                        elif doc_value.get('counter') is not None:
                            collection_name = 'Counter'
                        else:
                            # Không có cả count và counter -> dùng mechoice_metadata
                            collection_name = 'mechoice_metadata'
                    
                    # Tạo document mới với _id và thêm bucket_name
                    final_doc = doc_value.copy()
                    final_doc['_id'] = doc_id
                    final_doc['bucket_name'] = bucket_name
                    
                    if collection_name not in collection_groups:
                        collection_groups[collection_name] = []
                    collection_groups[collection_name].append(final_doc)
                    
                except Exception as e:
                    skipped_count += 1
                    doc_id = doc.get('id', 'unknown') if isinstance(doc, dict) else 'unknown'
                    logger.error(f"Error processing document with id '{doc_id}' in {bucket_name}: {e}", exc_info=True)
                    continue
            
            if not collection_groups:
                logger.warning(f"No valid documents to insert for {bucket_name}")
                return
            
            # Process each collection group
            total_inserted = 0
            total_duplicates = 0
            
            for collection_name, docs in collection_groups.items():
                # Batch check duplicates
                doc_ids = [doc['_id'] for doc in docs]
                existing_ids = self._check_duplicates_batch(collection_name, doc_ids)
                
                # Filter out duplicates
                unique_docs = [doc for doc in docs if doc['_id'] not in existing_ids]
                duplicate_count = len(docs) - len(unique_docs)
                total_duplicates += duplicate_count
                
                if duplicate_count > 0:
                    logger.info(f"Found {duplicate_count} duplicate documents in {collection_name}, skipping")
                
                # Insert in batches
                if unique_docs:
                    for i in range(0, len(unique_docs), batch_size):
                        batch = unique_docs[i:i+batch_size]
                        try:
                            self.mongo_dal.add_documents(collection_name, batch, max_retries=5, retry_delay=3)
                            logger.debug(f"Inserted batch {i//batch_size + 1} ({len(batch)} documents) into {collection_name}")
                        except Exception as e:
                            logger.error(f"Error inserting batch into {collection_name}: {e}", exc_info=True)
                            raise e
                    
                    total_inserted += len(unique_docs)
                    logger.info(f"Successfully inserted {len(unique_docs)} documents into {collection_name}")
                else:
                    logger.warning(f"All documents are duplicates for {collection_name}")
            
            logger.info(f"Successfully processed {total_inserted} documents for {bucket_name} "
                       f"(skipped {skipped_count + total_duplicates} documents)")
                
        except Exception as e:
            logger.error(f"Error processing missing_type data for {bucket_name}: {e}", exc_info=True)
            raise e
        finally:
            self.mongo_dal.close()
            gc.collect()

    # def add_document(self, db_name: str, bucket_name: str, document: dict):
    #     try:
    #         self.mongo_dal.add_document(db_name, bucket_name, document)
    #     except Exception as e:
    #         logger.error(f"{bucket_name.upper()}: Error adding document to MongoDB: {e}", exc_info=True)
    #         raise e