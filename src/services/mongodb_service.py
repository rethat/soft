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

    # def add_document(self, db_name: str, bucket_name: str, document: dict):
    #     try:
    #         self.mongo_dal.add_document(db_name, bucket_name, document)
    #     except Exception as e:
    #         logger.error(f"{bucket_name.upper()}: Error adding document to MongoDB: {e}", exc_info=True)
    #         raise e