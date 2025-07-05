# GenericMongo - Usage Documentation

## Overview

`GenericMongo` is a comprehensive MongoDB handler class designed for seamless DataFrame operations. It provides a unified interface for inserting, updating, querying, and managing MongoDB documents with automatic data type conversion and error handling.

## Key Features

- **DataFrame Integration**: Direct pandas DataFrame to MongoDB document conversion
- **Data Type Handling**: Automatic conversion of numpy types and NaN values
- **Duplicate Prevention**: Unique field constraints and duplicate handling
- **Hash-based Change Detection**: Document integrity checking with MD5 hashing
- **NPI-specific Methods**: Specialized methods for NPI (National Provider Identifier) operations
- **Error Handling**: Comprehensive error tracking and logging
- **Flexible Querying**: Support for complex MongoDB queries

## Installation Requirements

```bash
pip install pandas pymongo numpy
```

## Initialization

```python
from generic_mongo import GenericMongo

# Initialize MongoDB connection
mongo_handler = GenericMongo(
    connection_string="mongodb://localhost:27017/",
    database_name="your_database",
    collection_name="your_collection"
)
```

**Parameters:**
- `connection_string` (str): MongoDB connection string
- `database_name` (str): Target database name
- `collection_name` (str): Target collection name

## Core Methods

### 1. Insert Operations

#### `insert(df, unique_field=None)`
Inserts a pandas DataFrame into MongoDB collection.

```python
# Example DataFrame
df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'city': ['New York', 'London', 'Paris']
})

# Insert with unique constraint
result = mongo_handler.insert(df, unique_field='id')
print("Insert result:", result)
```

**Parameters:**
- `df` (pd.DataFrame): DataFrame to insert
- `unique_field` (str, optional): Field name for uniqueness constraint

**Returns:**
```python
{
    "success": True,
    "inserted_count": 3,
    "total_processed": 3,
    "errors": []
}
```

### 2. Update Operations

#### `update(query, update_data, upsert=False, multi=False)`
Updates documents in MongoDB collection.

```python
# Update single document
update_result = mongo_handler.update(
    query={'id': 1},
    update_data={'age': 26, 'city': 'Boston'},
    upsert=True
)

# Update with DataFrame
df_update = pd.DataFrame({'age': [27], 'city': ['Chicago']})
update_result = mongo_handler.update(
    query={'id': 1},
    update_data=df_update
)
```

**Parameters:**
- `query` (Dict): MongoDB query to find documents
- `update_data` (Dict or pd.DataFrame): Data to update
- `upsert` (bool): Create document if not found (default: False)
- `multi` (bool): Update multiple documents (default: False)

### 3. Existence and Hash Checking

#### `exists(identifier_value, column_name)`
Checks if document exists and returns hash of specified columns.

```python
# Check existence and get hash
exists_result = mongo_handler.exists('1', 'id,name,age')
print("Exists result:", exists_result)

# Result structure:
{
    "exists": True,
    "hash": "a1b2c3d4e5f6...",
    "identifier": "1",
    "document_id": "507f1f77bcf86cd799439011",
    "columns_used": ["id", "name", "age"]
}
```

**Use Cases:**
- Change detection between data updates
- Data integrity verification
- Incremental data processing

### 4. Query Operations

#### `find(query, limit=None)`
Finds documents matching the query.

```python
# Find all documents
all_docs = mongo_handler.find({})

# Find with conditions
adults = mongo_handler.find({'age': {'$gte': 30}}, limit=10)

# Complex queries
query = {
    'city': {'$in': ['New York', 'London']},
    'age': {'$gte': 25}
}
results = mongo_handler.find(query)
```

#### `find_npi(npi)`
Specialized method for finding documents by NPI number.

```python
# Find by NPI
npi_doc = mongo_handler.find_npi('1234567890')
if npi_doc:
    print("Found NPI document:", npi_doc)
else:
    print("NPI not found")
```

**Features:**
- Automatic field name detection (`npi`, `number`, `NPI`, `Number`)
- String and integer format handling
- Flexible NPI format support

### 5. Utility Methods

#### `count(query=None)`
Counts documents in collection.

```python
# Count all documents
total = mongo_handler.count()

# Count with conditions
adults_count = mongo_handler.count({'age': {'$gte': 30}})
```

#### `create_index(field, unique=False)`
Creates index on specified field.

```python
# Create regular index
mongo_handler.create_index('name')

# Create unique index
mongo_handler.create_index('id', unique=True)
```

#### `close()`
Closes MongoDB connection.

```python
mongo_handler.close()
```

## Complete Usage Example

```python
import pandas as pd
from generic_mongo import GenericMongo

def main():
    # Initialize connection
    mongo_handler = GenericMongo(
        connection_string="mongodb://localhost:27017/",
        database_name="healthcare_db",
        collection_name="providers"
    )
    
    try:
        # Sample healthcare provider data
        df = pd.DataFrame({
            'npi': ['1234567890', '2345678901', '3456789012'],
            'name': ['Dr. Smith', 'Dr. Johnson', 'Dr. Brown'],
            'specialty': ['Cardiology', 'Pediatrics', 'Orthopedics'],
            'city': ['New York', 'Chicago', 'Los Angeles'],
            'years_experience': [15, 8, 20]
        })
        
        # Insert data with NPI as unique field
        print("Inserting provider data...")
        insert_result = mongo_handler.insert(df, unique_field='npi')
        print(f"Inserted {insert_result['inserted_count']} providers")
        
        # Check if specific NPI exists
        npi_to_check = '1234567890'
        exists_result = mongo_handler.exists(npi_to_check, 'npi,name,specialty')
        print(f"NPI {npi_to_check} exists: {exists_result['exists']}")
        print(f"Hash: {exists_result['hash']}")
        
        # Update provider information
        update_data = {
            'years_experience': 16,
            'city': 'Boston',
            'last_updated': '2024-01-15'
        }
        
        update_result = mongo_handler.update(
            query={'npi': '1234567890'},
            update_data=update_data
        )
        print(f"Updated {update_result['modified_count']} documents")
        
        # Check hash after update
        exists_after_update = mongo_handler.exists(npi_to_check, 'npi,name,specialty')
        print(f"Hash after update: {exists_after_update['hash']}")
        print(f"Hash changed: {exists_result['hash'] != exists_after_update['hash']}")
        
        # Find providers by specialty
        cardiologists = mongo_handler.find({'specialty': 'Cardiology'})
        print(f"Found {len(cardiologists)} cardiologists")
        
        # Find specific NPI
        provider = mongo_handler.find_npi('1234567890')
        if provider:
            print(f"Provider found: {provider['name']}")
        
        # Count total providers
        total_providers = mongo_handler.count()
        print(f"Total providers in database: {total_providers}")
        
        # Create indexes for better performance
        mongo_handler.create_index('npi', unique=True)
        mongo_handler.create_index('specialty')
        
    finally:
        # Always close the connection
        mongo_handler.close()

if __name__ == "__main__":
    main()
```

## Data Type Handling

The class automatically handles various data types:

- **NaN Values**: Converted to `None` for MongoDB compatibility
- **Numpy Types**: Converted to native Python types
- **Arrays/Lists**: Recursively processed for NaN values
- **Nested Objects**: Preserved as-is for MongoDB storage

## Error Handling

The class provides comprehensive error handling:

- **Individual Row Errors**: Tracked and reported without stopping the entire operation
- **Bulk Operation Errors**: Graceful handling of partial failures
- **Connection Errors**: Proper error reporting and resource cleanup
- **Data Type Errors**: Automatic type conversion with fallback handling

## Best Practices

1. **Use Unique Fields**: Always specify unique fields for data integrity
2. **Batch Processing**: Process large datasets in chunks
3. **Index Creation**: Create indexes on frequently queried fields
4. **Hash Checking**: Use hash-based change detection for incremental updates
5. **Connection Management**: Always close connections when done
6. **Error Monitoring**: Monitor and log insertion/update errors

## Performance Considerations

- **Bulk Operations**: Use bulk inserts for large datasets
- **Index Strategy**: Create appropriate indexes before large operations
- **Memory Management**: Process large DataFrames in chunks
- **Connection Pooling**: Reuse connections for multiple operations

## Common Use Cases

- **Data Migration**: Moving data from CSV/Excel to MongoDB
- **Change Detection**: Identifying modified records using hash comparison
- **Healthcare Data**: Managing NPI-based provider information
- **Batch Updates**: Processing large datasets with duplicate handling
- **Data Validation**: Ensuring data integrity with unique constraints