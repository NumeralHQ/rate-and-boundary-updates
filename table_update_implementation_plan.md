# Table Update Implementation Plan

## Overview
This document outlines the implementation plan for a new table update functionality that processes CSV files to update or append data to existing DuckDB tables based on specified filtering criteria.

## Feature Requirements

### Core Functionality
- Process CSV files containing table updates/appends from timestamped job folders
- **Schema validation**: Validate CSV field names match target table schemas before processing
- Update existing DuckDB database tables based on filtering criteria
- Support both append and update operations with **optimized batch processing**
- Generate comprehensive error logging
- Maintain data integrity through proper transaction handling
- **Performance optimized**: Uses DuckDB native CSV import and chunked processing for large files

### Input Sources
1. **Base DuckDB File**: `DATABASE_PATH` from `config.py`
2. **Filtering Criteria**: `table_updates/filtering_criteria.json`
3. **CSV Update Files**: Located in newest timestamped folder within `table_updates/`

## Implementation Plan

### 1. New Script: `table_updates/table_updater.py`

#### 1.1 Core Classes and Structure

```python
class TableUpdater:
    """Main class for processing table updates from CSV files"""
    
    def __init__(self):
        # Initialize logging, database connections, and configuration
        
    def find_latest_update_folder(self) -> str:
        # Find newest {YYMMDD}_update folder in table_updates/
        
    def duplicate_database(self, source_path: str, job_folder: str, timestamp: str) -> str:
        # Copy base DuckDB file to job folder as tax_db_{YYMMDD}.duckdb
        
    def load_filtering_criteria(self) -> dict:
        # Load and parse filtering_criteria.json
        
    def process_csv_files(self, job_folder: str, db_path: str):
        # Main processing loop for all CSV files in job folder
        
    def parse_csv_filename(self, filename: str) -> tuple:
        # Extract table_name, job_type, sequential_number from filename
        
    def validate_csv_schema(self, csv_path: str, table_name: str, db_path: str) -> bool:
        # Validate CSV field names match target table schema
        
    def process_append_job(self, csv_path: str, table_name: str, db_path: str):
        # Handle append operations (simple insertion)
        
    def process_update_job(self, csv_path: str, table_name: str, db_path: str, filter_fields: list):
        # Handle update operations with filtering logic
        
    def log_error(self, error_data: dict, job_folder: str):
        # Append errors to errors.json file
```

#### 1.2 Key Implementation Details

##### File Discovery and Processing
```python
def find_latest_update_folder(self) -> str:
    """
    Find the newest timestamped folder in table_updates/
    Format: {YYMMDD}_update
    Returns: full path to the latest folder
    """
    update_folders = []
    for folder in os.listdir('table_updates/'):
        if folder.endswith('_update') and len(folder) == 13:  # YYMMDD_update = 13 chars
            try:
                timestamp = folder[:6]  # Extract YYMMDD
                datetime.strptime(timestamp, '%y%m%d')  # Validate format
                update_folders.append((timestamp, folder))
            except ValueError:
                continue
    
    if not update_folders:
        raise ValueError("No valid update folders found")
    
    # Sort by timestamp and return latest
    latest = max(update_folders, key=lambda x: x[0])
    return os.path.join('table_updates', latest[1])
```

##### Database Operations
```python
def duplicate_database(self, source_path: str, job_folder: str, timestamp: str) -> str:
    """
    Copy base DuckDB file to job folder with timestamp naming
    Returns: path to new database file
    """
    target_filename = f"tax_db_{timestamp}.duckdb"
    target_path = os.path.join(job_folder, target_filename)
    
    # Overwrite if exists
    if os.path.exists(target_path):
        os.remove(target_path)
    
    shutil.copy2(source_path, target_path)
    return target_path
```

##### Schema Validation
```python
def validate_csv_schema(self, csv_path: str, table_name: str, db_path: str) -> bool:
    """
    Validate that CSV field names match the target table schema
    Returns: True if schema matches, False otherwise
    """
    import duckdb
    
    try:
        # Get table schema from database
        conn = duckdb.connect(db_path)
        table_columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
        db_field_names = {col[0].lower() for col in table_columns}  # Convert to lowercase set
        conn.close()
        
        # Get CSV field names
        df = pd.read_csv(csv_path, nrows=0)  # Read only headers
        csv_field_names = {col.lower() for col in df.columns}  # Convert to lowercase set
        
        # Check if CSV fields are a subset of table fields
        if not csv_field_names.issubset(db_field_names):
            missing_fields = csv_field_names - db_field_names
            error_data = {
                "file": os.path.basename(csv_path),
                "error": "CSV schema validation failed",
                "table": table_name,
                "missing_fields": list(missing_fields),
                "csv_fields": list(csv_field_names),
                "table_fields": list(db_field_names)
            }
            self.log_error(error_data, os.path.dirname(csv_path))
            return False
            
        return True
        
    except Exception as e:
        error_data = {
            "file": os.path.basename(csv_path),
            "error": f"Schema validation error: {str(e)}",
            "table": table_name
        }
        self.log_error(error_data, os.path.dirname(csv_path))
        return False
```

##### CSV Processing Logic
```python
def process_csv_files(self, job_folder: str, db_path: str):
    """
    Main processing loop for all CSV files in job folder
    """
    csv_files = [f for f in os.listdir(job_folder) if f.endswith('.csv')]
    
    for csv_file in csv_files:
        csv_path = os.path.join(job_folder, csv_file)
        
        # Parse filename
        try:
            table_name, job_type, seq_num = self.parse_csv_filename(csv_file)
        except ValueError as e:
            error_data = {
                "file": csv_file,
                "error": f"Invalid filename format: {str(e)}"
            }
            self.log_error(error_data, job_folder)
            continue
        
        # Validate schema before processing
        if not self.validate_csv_schema(csv_path, table_name, db_path):
            continue  # Error already logged, skip to next file
        
        # Process based on job type
        if job_type == "append":
            self.process_append_job(csv_path, table_name, db_path)
        elif job_type == "update":
            filter_fields = self.filtering_criteria.get(table_name, {}).get("filter_fields", [])
            if not filter_fields:
                error_data = {
                    "file": csv_file,
                    "error": f"No filtering criteria found for table: {table_name}",
                    "table": table_name
                }
                self.log_error(error_data, job_folder)
                continue
            self.process_update_job(csv_path, table_name, db_path, filter_fields)

def process_update_job(self, csv_path: str, table_name: str, db_path: str, filter_fields: list):
    """
    Process update CSV files with filtering logic - optimized for batch processing
    """
    import duckdb
    
    conn = duckdb.connect(db_path)
    
    try:
        # Read CSV data in chunks for large files (optimized processing)
        chunk_size = 1000  # Process 1000 rows at a time for optimal performance
        csv_reader = pd.read_csv(csv_path, chunksize=chunk_size)
        
        for chunk_idx, df_chunk in enumerate(csv_reader):
            for index, row in df_chunk.iterrows():
                # Build WHERE clause from filter fields
                where_conditions = []
                params = {}
                
                for field in filter_fields:
                    if field in row and pd.notna(row[field]):
                        where_conditions.append(f"{field} = ?")
                        params[field] = row[field]
                
                if not where_conditions:
                    # No filter conditions - log error and skip
                    error_data = {
                        "file": os.path.basename(csv_path),
                        "row": chunk_idx * chunk_size + index + 1,
                        "error": "No valid filter conditions found in row",
                        "filter_fields": filter_fields
                    }
                    self.log_error(error_data, os.path.dirname(csv_path))
                    continue
                
                where_clause = " AND ".join(where_conditions)
                param_values = [params[field] for field in filter_fields if field in params]
                
                # Check for existing records
                query = f"SELECT COUNT(*) as count FROM {table_name} WHERE {where_clause}"
                result = conn.execute(query, param_values).fetchone()
                
                if result[0] == 0:
                    # No match found - append
                    self._insert_row(conn, table_name, row)
                elif result[0] == 1:
                    # Single match - update
                    self._update_row(conn, table_name, row, where_clause, param_values)
                else:
                    # Multiple matches - log error
                    error_data = {
                        "file": os.path.basename(csv_path),
                        "row": chunk_idx * chunk_size + index + 1,  # 1-based indexing
                        "error": "Multiple matching records found",
                        "filter_fields": filter_fields,
                        "filter_values": {field: row[field] for field in filter_fields if field in row and pd.notna(row[field])},
                        "match_count": result[0]
                    }
                    self.log_error(error_data, os.path.dirname(csv_path))
                
    except Exception as e:
        error_data = {
            "file": os.path.basename(csv_path),
            "error": f"Processing failed: {str(e)}",
            "table": table_name
        }
        self.log_error(error_data, os.path.dirname(csv_path))
    finally:
        conn.close()

def process_append_job(self, csv_path: str, table_name: str, db_path: str):
    """
    Process append CSV files - optimized for batch insertion
    """
    import duckdb
    
    conn = duckdb.connect(db_path)
    
    try:
        # For append operations, use DuckDB's native CSV import for maximum speed
        temp_table = f"temp_{table_name}_{int(time.time())}"
        
        # Create temporary table from CSV
        conn.execute(f"CREATE TEMP TABLE {temp_table} AS SELECT * FROM read_csv_auto('{csv_path}')")
        
        # Insert all rows from temp table to target table
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table}")
        
        # Drop temporary table
        conn.execute(f"DROP TABLE {temp_table}")
        
    except Exception as e:
        error_data = {
            "file": os.path.basename(csv_path),
            "error": f"Append operation failed: {str(e)}",
            "table": table_name
        }
        self.log_error(error_data, os.path.dirname(csv_path))
    finally:
        conn.close()
```

### 2. Error Handling and Logging

#### 2.1 Error Types to Track
- Invalid CSV file naming format
- **CSV schema validation failures** (field name mismatches)
- Missing or invalid filtering criteria for table
- Multiple record matches during update operations
- Database connection/transaction failures
- CSV parsing errors
- Missing filter field values in update rows

#### 2.2 Error Logging Format (`errors.json`)
```json
{
    "timestamp": "2025-01-15T10:30:00Z",
    "total_errors": 4,
    "errors": [
        {
            "file": "product_item_update_1.csv",
            "error": "CSV schema validation failed",
            "table": "product_item",
            "missing_fields": ["invalid_column"],
            "csv_fields": ["group", "item", "description", "invalid_column"],
            "table_fields": ["group", "item", "description"]
        },
        {
            "file": "product_item_update_1.csv",
            "row": 5,
            "error": "Multiple matching records found",
            "filter_fields": ["group", "item"],
            "filter_values": {"group": "7777", "item": "001"},
            "match_count": 2
        },
        {
            "file": "matrix_update_1.csv",
            "error": "No filtering criteria found for table: matrix_invalid",
            "table": "matrix_invalid"
        },
        {
            "file": "detail_append_1.csv",
            "error": "Append operation failed: table does not exist",
            "table": "detail"
        }
    ]
}
```

### 3. Configuration Integration

#### 3.1 Add to `config.py`
```python
# Table Update Configuration
TABLE_UPDATE_FOLDER = os.path.join(BASE_DIR, "table_updates")
ERROR_LOG_FILENAME = "errors.json"

# Supported table operations
SUPPORTED_JOB_TYPES = ["append", "update"]

# CSV file naming pattern: {table_name}_{job_type}_{sequential_number}.csv
CSV_FILENAME_PATTERN = r"^(.+)_(append|update)_(\d+)\.csv$"
```

### 4. Main Execution Script

#### 4.1 Command Line Interface
```python
def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Update DuckDB tables from CSV files')
    parser.add_argument('--dry-run', action='store_true', 
                        help='Validate files and show operations without executing')
    parser.add_argument('--job-folder', type=str, 
                        help='Specific job folder to process (default: latest)')
    
    args = parser.parse_args()
    
    updater = TableUpdater()
    
    try:
        # Find job folder
        if args.job_folder:
            job_folder = args.job_folder
        else:
            job_folder = updater.find_latest_update_folder()
        
        print(f"Processing job folder: {job_folder}")
        
        # Extract timestamp from folder name
        folder_name = os.path.basename(job_folder)
        timestamp = folder_name[:6]  # YYMMDD
        
        # Duplicate database
        if not args.dry_run:
            db_path = updater.duplicate_database(DATABASE_PATH, job_folder, timestamp)
            print(f"Created database copy: {db_path}")
        
        # Process CSV files
        updater.process_csv_files(job_folder, db_path if not args.dry_run else None)
        
        print("Processing completed successfully")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

### 5. Integration Points

#### 5.1 Existing Code Modifications
- **Minimal changes required** to existing codebase
- Script located in `table_updates/` folder for logical organization
- Optionally integrate with existing logging system in `src/logger.py`

#### 5.2 Dependencies
- Add to `requirements.txt`:
  ```
  duckdb>=0.9.0
  pandas>=1.5.0
  ```

### 6. Testing Strategy

#### 6.1 Unit Tests
- Test CSV filename parsing
- Test filtering criteria loading
- Test database duplication
- Test error logging functionality

#### 6.2 Integration Tests  
- Test complete workflow with sample data
- Test error handling scenarios
- Test dry-run functionality

### 7. Usage Examples

#### 7.1 Basic Usage
```bash
# Process latest update folder
python table_updates/table_updater.py

# Dry run to validate
python table_updates/table_updater.py --dry-run

# Process specific folder
python table_updates/table_updater.py --job-folder table_updates/250801_update
```

#### 7.2 Expected File Structure
```
table_updates/
├── table_updater.py  (new script)
├── filtering_criteria.json
├── tests/
|   ├── _init_.py  (generated)
|   ├── test_error_handling.py  (generated)
|   ├── test_dry_run.py  (generated)
|   └── test_complete_table_update.py  (generated)
└── 250801_update/
    ├── detail_append_1.csv
    ├── detail_append_2.csv
    ├── matrix_update_1.csv
    ├── product_item_update_1.csv
    ├── errors.json  (generated)
    └── tax_db_250801.duckdb  (generated)
```

## Success Criteria

1. **Functionality**: Successfully process all supported CSV file types
2. **Schema Validation**: Validate CSV field names against table schemas before processing
3. **Data Integrity**: Maintain referential integrity and prevent data corruption  
4. **Error Handling**: Comprehensive error logging without stopping execution
5. **Performance**: Optimized batch processing for large CSV files (7K+ rows)
6. **Maintainability**: Clean, documented code that integrates well with existing architecture

## Future Enhancements

- Support for additional database formats
- Web interface for monitoring updates
- Automated rollback capabilities
- Progress reporting for long-running operations
- Parallel processing of multiple CSV files
- Data validation rules beyond schema matching