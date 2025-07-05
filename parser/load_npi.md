# NPI_Load - Usage Documentation

## Overview

`NPI_Load` is a universal class for working with data in CSV and ZIP formats. It provides a unified interface for reading, processing, and analyzing data regardless of the file format.

## Key Features

- Work with ZIP archives and regular CSV files
- Read data in chunks for processing large files
- Get information about data structure
- Column normalization
- Automatic schema detection

## Initialization

### Working with ZIP files

```python
npi_loader_zip = NPI_Load(
    file_path="path/to/npi_data.zip",
    prefix="npi"  # prefix for searching files in ZIP archive
)
```

### Working with CSV files

```python
npi_loader_csv = NPI_Load(
    file_path="path/to/data.csv"
    # prefix is ignored for CSV files
)
```

## Usage Examples

### Complete data processing example

```python
if __name__ == "__main__":
    # Working with ZIP file
    print("=== Working with ZIP file ===")
    npi_loader_zip = NPI_Load(
        file_path="path/to/npi_data.zip",
        prefix="npi"  # prefix for searching in ZIP
    )
    
    # Working with regular CSV file
    print("\n=== Working with CSV file ===")
    npi_loader_csv = NPI_Load(
        file_path="path/to/data.csv"
        # prefix is ignored for CSV files
    )
    
    # Same methods for both cases
    for loader, name in [(npi_loader_zip, "ZIP"), (npi_loader_csv, "CSV")]:
        print(f"\n--- {name} file ---")
        
        # Get file information
        file_info = loader.get_file_info()
        print("File information:", file_info)
        
        # Read first 10 rows
        head_df = loader.read_csv_head(n=10)
        print(f"First 10 rows: {head_df.shape}")
        
        # Get data schema
        schema = loader.get_schema_from_sample(sample_size=100)
        print("Number of columns:", len(schema))
        
        # Read data in chunks
        for chunk in loader.read_csv_in_chunks(chunk_size=50000):
            # Normalize columns
            normalized_chunk = loader.normalize_columns(chunk)
            print(f"Processed chunk: {normalized_chunk.shape}")
            break  # For example, process only first chunk
```

## Core Methods

### `get_file_info()`
Returns file information (size, path, type).

### `read_csv_head(n=10)`
Reads first n rows of the file for preliminary analysis.

**Parameters:**
- `n` (int): number of rows to read (default 10)

### `get_schema_from_sample(sample_size=100)`
Analyzes data structure based on a sample.

**Parameters:**
- `sample_size` (int): sample size for analysis (default 100)

### `read_csv_in_chunks(chunk_size=50000)`
Reads data in chunks for efficient processing of large files.

**Parameters:**
- `chunk_size` (int): size of data chunk (default 50000)

**Returns:** generator of DataFrame objects

### `normalize_columns(chunk)`
Normalizes columns in the provided DataFrame.

**Parameters:**
- `chunk` (DataFrame): data for normalization

**Returns:** normalized DataFrame

## Usage Specifics

### ZIP files
- For ZIP files, it's mandatory to specify `prefix` to search for required files inside the archive
- The class will automatically find and process the corresponding CSV file in the archive

### CSV files
- For regular CSV files, the `prefix` parameter is ignored
- Direct file processing is supported without additional extraction

### Large file processing
- Use `read_csv_in_chunks()` for large files
- Recommended chunk size: 50000-100000 rows
- Apply `normalize_columns()` to each chunk for data consistency

## Benefits

- **Universality**: unified interface for different formats
- **Efficiency**: support for streaming reading of large files
- **Flexibility**: customizable parameters for various use cases
- **Simplicity**: minimal code for complex operations