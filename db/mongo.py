import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import numpy as np
from typing import Dict, List, Any, Optional, Union
import logging
import hashlib
import json

class GenericMongo:
    def __init__(self, connection_string: str, database_name: str, collection_name: str):
        """
        Generic MongoDB handler for DataFrame operations
        
        Args:
            connection_string: MongoDB connection string
            database_name: Database name
            collection_name: Collection name
        """
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]
    
    def _convert_nan_to_none(self, value):
        """Convert NaN values to None for MongoDB compatibility"""
        if pd.isna(value):
            return None
        return value
    
    def _convert_dataframe_row_to_document(self, row: pd.Series) -> Dict:
        """Convert DataFrame row to MongoDB document"""
        document = {}
        
        for column, value in row.items():
            # Convert NaN to None
            converted_value = self._convert_nan_to_none(value)
            
            # Convert numpy types to native Python types
            if isinstance(converted_value, np.integer):
                converted_value = int(converted_value)
            elif isinstance(converted_value, np.floating):
                converted_value = float(converted_value)
            elif isinstance(converted_value, np.bool_):
                converted_value = bool(converted_value)
            elif isinstance(converted_value, (np.ndarray, list)):
                converted_value = [self._convert_nan_to_none(item) for item in converted_value]
            
            document[column] = converted_value
        
        return document
    
    def insert(self, df: pd.DataFrame, unique_field: Optional[str] = None) -> Dict[str, Any]:
        """
        Insert DataFrame into MongoDB collection
        
        Args:
            df: DataFrame to insert
            unique_field: Field name to use for uniqueness check (optional)
            
        Returns:
            Dictionary with insertion results
        """
        try:
            documents = []
            errors = []
            
            # Convert DataFrame rows to documents
            for index, row in df.iterrows():
                try:
                    document = self._convert_dataframe_row_to_document(row)
                    
                    # Add timestamp
                    document['_inserted_at'] = datetime.utcnow()
                    
                    documents.append(document)
                    
                except Exception as e:
                    errors.append(f"Row {index}: {str(e)}")
            
            if not documents:
                return {
                    "success": False,
                    "inserted_count": 0,
                    "total_processed": len(df),
                    "errors": errors,
                    "message": "No valid documents to insert"
                }
            
            # Create unique index if specified
            if unique_field:
                try:
                    self.collection.create_index(unique_field, unique=True)
                except Exception as e:
                    self.logger.warning(f"Could not create unique index on {unique_field}: {e}")
            
            # Insert documents
            inserted_count = 0
            try:
                if unique_field:
                    # Insert one by one to handle duplicates
                    for doc in documents:
                        try:
                            self.collection.insert_one(doc)
                            inserted_count += 1
                        except Exception as insert_error:
                            if unique_field in doc:
                                errors.append(f"{unique_field} {doc[unique_field]}: {str(insert_error)}")
                            else:
                                errors.append(f"Document: {str(insert_error)}")
                else:
                    # Bulk insert
                    result = self.collection.insert_many(documents, ordered=False)
                    inserted_count = len(result.inserted_ids)
                    
            except Exception as e:
                errors.append(f"Bulk insert error: {str(e)}")
            
            result = {
                "success": True,
                "inserted_count": inserted_count,
                "total_processed": len(df),
                "errors": errors
            }
            
            self.logger.info(f"Inserted {inserted_count} documents out of {len(df)} processed")
            return result
            
        except Exception as e:
            self.logger.error(f"Insert operation failed: {str(e)}")
            return {
                "success": False,
                "inserted_count": 0,
                "total_processed": len(df),
                "errors": [str(e)]
            }
    
    def update(self, query: Dict, update_data: Union[Dict, pd.DataFrame], 
               upsert: bool = False, multi: bool = False) -> Dict[str, Any]:
        """
        Update documents in MongoDB collection
        
        Args:
            query: MongoDB query to find documents
            update_data: Update data (dict or DataFrame)
            upsert: Create document if not found
            multi: Update multiple documents
            
        Returns:
            Dictionary with update results
        """
        try:
            if isinstance(update_data, pd.DataFrame):
                # Convert DataFrame to update document
                if len(update_data) == 1:
                    update_doc = self._convert_dataframe_row_to_document(update_data.iloc[0])
                else:
                    raise ValueError("DataFrame must contain exactly one row for update")
            else:
                update_doc = update_data
            
            # Add timestamp
            update_doc['_updated_at'] = datetime.utcnow()
            
            # Prepare update operation
            update_operation = {"$set": update_doc}
            
            if multi:
                result = self.collection.update_many(query, update_operation, upsert=upsert)
            else:
                result = self.collection.update_one(query, update_operation, upsert=upsert)
            
            return {
                "success": True,
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "upserted_id": result.upserted_id if hasattr(result, 'upserted_id') else None
            }
            
        except Exception as e:
            self.logger.error(f"Update operation failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def exists(self, identifier_value: str, column_name: str) -> Dict[str, Any]:
        """
        Check if document exists and return hash of specified columns
        
        Args:
            identifier_value: Value to search for
            column_name: Column name to search in (or comma-separated list)
            
        Returns:
            Dictionary with existence check and hash sum
        """
        try:
            # Parse column names
            columns = [col.strip() for col in column_name.split(',')]
            
            # Find document
            document = self.collection.find_one({columns[0]: identifier_value})
            
            if not document:
                return {
                    "exists": False,
                    "hash": None,
                    "identifier": identifier_value
                }
            
            # Calculate hash for specified columns
            hash_data = {}
            for col in columns:
                if col in document:
                    value = document[col]
                    # Convert to string for consistent hashing
                    if isinstance(value, (dict, list)):
                        hash_data[col] = json.dumps(value, sort_keys=True, default=str)
                    else:
                        hash_data[col] = str(value) if value is not None else ""
                else:
                    hash_data[col] = ""
            
            # Create hash string
            hash_string = json.dumps(hash_data, sort_keys=True)
            hash_sum = hashlib.md5(hash_string.encode()).hexdigest()
            
            return {
                "exists": True,
                "hash": hash_sum,
                "identifier": identifier_value,
                "document_id": str(document.get('_id')),
                "columns_used": columns
            }
            
        except Exception as e:
            self.logger.error(f"Exists check failed: {str(e)}")
            return {
                "exists": False,
                "hash": None,
                "identifier": identifier_value,
                "error": str(e)
            }
    
    def find(self, query: Dict, limit: Optional[int] = None) -> List[Dict]:
        """
        Find documents in collection
        
        Args:
            query: MongoDB query
            limit: Maximum number of documents to return
            
        Returns:
            List of documents
        """
        try:
            cursor = self.collection.find(query)
            if limit:
                cursor = cursor.limit(limit)
            
            return list(cursor)
            
        except Exception as e:
            self.logger.error(f"Find operation failed: {str(e)}")
            return []
    
    def find_npi(self, npi: str) -> Optional[Dict]:
        """
        Find document by NPI number
        
        Args:
            npi: NPI number to search for
            
        Returns:
            Document if found, None otherwise
        """
        try:
            # Convert to string and remove any non-numeric characters
            npi_clean = str(npi).strip()
            
            # Try to find by different possible field names
            possible_fields = ['npi', 'number', 'NPI', 'Number']
            
            for field in possible_fields:
                document = self.collection.find_one({field: npi_clean})
                if document:
                    self.logger.info(f"Found NPI {npi_clean} using field '{field}'")
                    return document
            
            # If not found with string, try with integer conversion
            try:
                npi_int = int(float(npi_clean))
                for field in possible_fields:
                    document = self.collection.find_one({field: npi_int})
                    if document:
                        self.logger.info(f"Found NPI {npi_clean} as integer using field '{field}'")
                        return document
            except ValueError:
                pass
            
            self.logger.info(f"NPI {npi_clean} not found")
            return None
            
        except Exception as e:
            self.logger.error(f"Find NPI operation failed: {str(e)}")
            return None
    
    def count(self, query: Dict = None) -> int:
        """
        Count documents in collection
        
        Args:
            query: MongoDB query (optional)
            
        Returns:
            Number of documents
        """
        try:
            if query is None:
                query = {}
            return self.collection.count_documents(query)
            
        except Exception as e:
            self.logger.error(f"Count operation failed: {str(e)}")
            return 0
    
    def create_index(self, field: str, unique: bool = False) -> bool:
        """
        Create index on field
        
        Args:
            field: Field name to index
            unique: Whether index should be unique
            
        Returns:
            True if successful
        """
        try:
            self.collection.create_index(field, unique=unique)
            self.logger.info(f"Created {'unique ' if unique else ''}index on field: {field}")
            return True
            
        except Exception as e:
            self.logger.error(f"Index creation failed: {str(e)}")
            return False
    
    def close(self):
        """Close MongoDB connection"""
        self.client.close()
        self.logger.info("MongoDB connection closed")