#!/usr/bin/env python3
"""
Table Updater Script

This script processes CSV files to update or append data to existing DuckDB tables
based on specified filtering criteria. It supports schema validation, batch processing,
and comprehensive error logging.

Usage:
    python table_updates/table_updater.py [--dry-run] [--job-folder FOLDER]
"""

import os
import sys
import json
import argparse
import shutil
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import re

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

try:
    import pandas as pd
    import duckdb
except ImportError as e:
    print(f"Error: Required package not installed: {e}")
    print("Please install requirements: pip install pandas duckdb")
    sys.exit(1)

from config import DATABASE_PATH


class TableUpdater:
    """Main class for processing table updates from CSV files"""
    
    def __init__(self):
        """Initialize the TableUpdater with configuration and logging setup"""
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.table_updates_folder = os.path.join(self.base_dir, "table_updates")
        self.filtering_criteria = {}
        
        # Configuration constants
        self.error_log_filename = "errors.json"
        self.supported_job_types = ["append", "update"]
        self.csv_filename_pattern = r"^(.+)_(append|update)_(\d+)\.csv$"
        
        # Load filtering criteria
        self.load_filtering_criteria()
    
    def _get_table_schema(self, table_name: str, db_path: str) -> dict:
        """
        Get table schema from DuckDB database
        Returns: dict mapping column names to DuckDB data types
        """
        conn = duckdb.connect(db_path)
        try:
            result = conn.execute(f'DESCRIBE "{table_name}"').fetchall()
            # Result format: [(column_name, column_type, null, key, default, extra), ...]
            schema = {}
            for row in result:
                col_name = row[0]
                col_type = row[1].upper()  # Convert to uppercase for consistency
                schema[col_name] = col_type
            return schema
        except Exception as e:
            raise Exception(f"Failed to get schema for table {table_name}: {str(e)}")
        finally:
            conn.close()
    
    def _duckdb_to_pandas_dtype(self, duckdb_type: str) -> str:
        """
        Map DuckDB data types to appropriate pandas dtypes
        Returns: pandas dtype or 'str' for string preservation
        """
        duckdb_type = duckdb_type.upper()
        
        # VARCHAR and CHAR types - preserve as strings to maintain formatting
        if 'VARCHAR' in duckdb_type or 'CHAR' in duckdb_type or duckdb_type == 'TEXT':
            return str
        
        # Integer types
        elif any(int_type in duckdb_type for int_type in ['INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT']):
            return 'Int64'  # Nullable integer
        
        # Floating point types
        elif any(float_type in duckdb_type for float_type in ['DOUBLE', 'REAL', 'FLOAT', 'DECIMAL', 'NUMERIC']):
            return 'float64'
        
        # Boolean types
        elif 'BOOLEAN' in duckdb_type or 'BOOL' in duckdb_type:
            return 'boolean'  # Nullable boolean
        
        # Date/time types - keep as string for proper parsing
        elif any(date_type in duckdb_type for date_type in ['DATE', 'TIME', 'TIMESTAMP']):
            return str
        
        # Default to string for unknown types
        else:
            return str
    
    def _get_csv_dtypes_from_schema(self, csv_path: str, table_name: str, db_path: str) -> dict:
        """
        Get appropriate pandas dtypes based on the target table schema
        """
        try:
            # Get table schema from database
            table_schema = self._get_table_schema(table_name, db_path)
            
            # Get CSV column names
            csv_columns = pd.read_csv(csv_path, nrows=0).columns
            
            # Build dtype mapping based on table schema
            dtypes = {}
            for col in csv_columns:
                if col in table_schema:
                    duckdb_type = table_schema[col]
                    pandas_dtype = self._duckdb_to_pandas_dtype(duckdb_type)
                    dtypes[col] = pandas_dtype
                else:
                    # Column not in table - will be caught by schema validation
                    dtypes[col] = str
            
            return dtypes
        except Exception as e:
            # If schema lookup fails, log error and use string types for safety
            print(f"Warning: Could not get table schema, using string types: {e}")
            return str
    
    def _convert_date_value(self, value: str, column_name: str) -> str:
        """
        Convert common date formats to YYYY-MM-DD format
        Handles Excel's automatic date reformatting (M/D/YYYY -> YYYY-MM-DD)
        """
        if not isinstance(value, str) or not value.strip():
            return value
        
        value = value.strip()
        
        # Common date formats that Excel creates
        date_patterns = [
            # M/D/YYYY or MM/DD/YYYY formats (Excel's favorite)
            (r'^(\d{1,2})/(\d{1,2})/(\d{4})$', lambda m: f"{m.group(3)}-{m.group(1):0>2}-{m.group(2):0>2}"),
            # M-D-YYYY or MM-DD-YYYY formats  
            (r'^(\d{1,2})-(\d{1,2})-(\d{4})$', lambda m: f"{m.group(3)}-{m.group(1):0>2}-{m.group(2):0>2}"),
            # YYYY/MM/DD format
            (r'^(\d{4})/(\d{1,2})/(\d{1,2})$', lambda m: f"{m.group(1)}-{m.group(2):0>2}-{m.group(3):0>2}"),
            # YYYY-MM-DD format (already correct)
            (r'^(\d{4})-(\d{1,2})-(\d{1,2})$', lambda m: f"{m.group(1)}-{m.group(2):0>2}-{m.group(3):0>2}"),
        ]
        
        for pattern, converter in date_patterns:
            match = re.match(pattern, value)
            if match:
                try:
                    converted = converter(match)
                    # Validate the converted date
                    datetime.strptime(converted, '%Y-%m-%d')
                    return converted
                except (ValueError, AttributeError):
                    continue
        
        # If no pattern matches, return original value
        # This will let DuckDB handle the error if it's truly invalid
        return value
    
    def _preprocess_row_data(self, row: pd.Series, table_schema: dict) -> pd.Series:
        """
        Preprocess row data to handle date conversions and other formatting
        """
        processed_row = row.copy()
        
        for col_name, value in row.items():
            if pd.notna(value) and col_name in table_schema:
                col_type = table_schema[col_name].upper()
                
                # Handle DATE columns
                if 'DATE' in col_type:
                    processed_row[col_name] = self._convert_date_value(value, col_name)
                # Handle TIMESTAMP columns  
                elif 'TIMESTAMP' in col_type:
                    processed_row[col_name] = self._convert_date_value(value, col_name)
        
        return processed_row
    
    def _read_csv_with_error_handling(self, csv_path: str, table_name: str, db_path: str, **kwargs) -> pd.DataFrame:
        """
        Read CSV with schema-based dtypes and handle conversion errors
        """
        try:
            # Get schema-based dtypes
            dtypes = self._get_csv_dtypes_from_schema(csv_path, table_name, db_path)
            
            # Try to read with specified dtypes
            df = pd.read_csv(csv_path, dtype=dtypes, keep_default_na=False, **kwargs)
            return df
            
        except Exception as conversion_error:
            # If type conversion fails, try reading as all strings and log the error
            try:
                df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, **kwargs)
                
                # Log the conversion error
                error_data = {
                    "file": os.path.basename(csv_path),
                    "error": f"CSV type conversion failed, reading as strings: {str(conversion_error)}",
                    "table": table_name
                }
                self.log_error(error_data, os.path.dirname(csv_path))
                
                return df
            except Exception as read_error:
                # If even string reading fails, re-raise the original error
                raise conversion_error
    
    def load_filtering_criteria(self) -> None:
        """Load and parse filtering_criteria.json"""
        criteria_path = os.path.join(self.table_updates_folder, "filtering_criteria.json")
        
        try:
            with open(criteria_path, 'r', encoding='utf-8') as f:
                self.filtering_criteria = json.load(f)
            print(f"Loaded filtering criteria for {len(self.filtering_criteria)} tables")
        except FileNotFoundError:
            print(f"Error: filtering_criteria.json not found at {criteria_path}")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in filtering_criteria.json: {e}")
            sys.exit(1)
    
    def find_latest_update_folder(self) -> str:
        """
        Find the newest timestamped folder in table_updates/
        Format: {YYMMDD}_update
        Returns: full path to the latest folder
        """
        if not os.path.exists(self.table_updates_folder):
            raise ValueError(f"table_updates folder not found: {self.table_updates_folder}")
        
        update_folders = []
        for folder in os.listdir(self.table_updates_folder):
            folder_path = os.path.join(self.table_updates_folder, folder)
            if (os.path.isdir(folder_path) and 
                folder.endswith('_update') and 
                len(folder) == 13):  # YYMMDD_update = 13 chars
                try:
                    timestamp = folder[:6]  # Extract YYMMDD
                    datetime.strptime(timestamp, '%y%m%d')  # Validate format
                    update_folders.append((timestamp, folder))
                except ValueError:
                    continue
        
        if not update_folders:
            raise ValueError("No valid update folders found in table_updates/")
        
        # Sort by timestamp and return latest
        latest = max(update_folders, key=lambda x: x[0])
        return os.path.join(self.table_updates_folder, latest[1])
    
    def duplicate_database(self, source_path: str, job_folder: str, timestamp: str) -> str:
        """
        Copy base DuckDB file to job folder with timestamp naming
        Returns: path to new database file
        """
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"Source database not found: {source_path}")
        
        target_filename = f"tax_db_{timestamp}.duckdb"
        target_path = os.path.join(job_folder, target_filename)
        
        # Overwrite if exists
        if os.path.exists(target_path):
            os.remove(target_path)
            print(f"Removed existing database copy: {target_filename}")
        
        print(f"Copying database from {source_path} to {target_path}")
        shutil.copy2(source_path, target_path)
        return target_path
    
    def parse_csv_filename(self, filename: str) -> Tuple[str, str, str]:
        """
        Extract table_name, job_type, sequential_number from filename
        Returns: (table_name, job_type, sequential_number)
        """
        match = re.match(self.csv_filename_pattern, filename)
        if not match:
            raise ValueError(f"Invalid filename format. Expected: table_name_(append|update)_number.csv")
        
        table_name, job_type, seq_num = match.groups()
        
        if job_type not in self.supported_job_types:
            raise ValueError(f"Unsupported job type: {job_type}")
        
        return table_name, job_type, seq_num
    
    def validate_csv_schema(self, csv_path: str, table_name: str, db_path: str) -> bool:
        """
        Validate that CSV field names match the target table schema
        Returns: True if schema matches, False otherwise
        """
        try:
            # Get table schema from database
            conn = duckdb.connect(db_path)
            try:
                table_columns = conn.execute(f'DESCRIBE "{table_name}"').fetchall()
                db_field_names = {col[0].lower() for col in table_columns}
            except Exception as e:
                error_data = {
                    "file": os.path.basename(csv_path),
                    "error": f"Failed to get table schema: {str(e)}",
                    "table": table_name
                }
                self.log_error(error_data, os.path.dirname(csv_path))
                return False
            finally:
                conn.close()
            
            # Get CSV field names
            df = pd.read_csv(csv_path, nrows=0)  # Read only headers
            csv_field_names = {col.lower().strip() for col in df.columns}
            
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
    
    def process_csv_files(self, job_folder: str, db_path: str, dry_run: bool = False):
        """
        Main processing loop for all CSV files in job folder
        """
        if not os.path.exists(job_folder):
            raise ValueError(f"Job folder not found: {job_folder}")
        
        csv_files = [f for f in os.listdir(job_folder) if f.endswith('.csv')]
        
        if not csv_files:
            print("No CSV files found in job folder")
            return
        
        print(f"Found {len(csv_files)} CSV files to process")
        
        for csv_file in csv_files:
            print(f"\nProcessing: {csv_file}")
            csv_path = os.path.join(job_folder, csv_file)
            
            # Parse filename
            try:
                table_name, job_type, seq_num = self.parse_csv_filename(csv_file)
                print(f"  Table: {table_name}, Job Type: {job_type}, Sequence: {seq_num}")
            except ValueError as e:
                error_data = {
                    "file": csv_file,
                    "error": f"Invalid filename format: {str(e)}"
                }
                self.log_error(error_data, job_folder)
                print(f"  SKIPPED: {e}")
                continue
            
            # For update jobs, check filtering criteria first
            if job_type == "update":
                filter_fields = self.filtering_criteria.get(table_name, {}).get("filter_fields", [])
                if not filter_fields:
                    error_data = {
                        "file": csv_file,
                        "error": f"No filtering criteria found for table: {table_name}",
                        "table": table_name
                    }
                    self.log_error(error_data, job_folder)
                    print(f"  SKIPPED: No filtering criteria for table {table_name}")
                    continue
            
            # Validate schema before processing
            if not dry_run:
                if not self.validate_csv_schema(csv_path, table_name, db_path):
                    print(f"  SKIPPED: Schema validation failed")
                    continue
            else:
                print(f"  DRY RUN: Would validate schema for table {table_name}")
            
            # Process based on job type
            if job_type == "append":
                if dry_run:
                    print(f"  DRY RUN: Would append {self._count_csv_rows(csv_path)} rows to {table_name}")
                else:
                    self.process_append_job(csv_path, table_name, db_path)
            elif job_type == "update":
                # We already validated filter_fields exist above
                filter_fields = self.filtering_criteria.get(table_name, {}).get("filter_fields", [])
                
                if dry_run:
                    print(f"  DRY RUN: Would update {table_name} using filter fields: {filter_fields}")
                    print(f"  DRY RUN: Would process {self._count_csv_rows(csv_path)} rows")
                else:
                    self.process_update_job(csv_path, table_name, db_path, filter_fields)
    
    def _count_csv_rows(self, csv_path: str, table_name: str = None, db_path: str = None) -> int:
        """Count rows in CSV file (excluding header)"""
        try:
            if table_name and db_path:
                # Use schema-based reading if we have database access
                df = self._read_csv_with_error_handling(csv_path, table_name, db_path)
            else:
                # Fallback to simple string reading for counting (e.g., during dry-run)
                df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
            return len(df)
        except Exception:
            return 0
    
    def process_append_job(self, csv_path: str, table_name: str, db_path: str):
        """
        Process append CSV files - consistent data type handling with date conversion
        """
        conn = duckdb.connect(db_path)
        
        try:
            print(f"  Reading CSV data with schema-based types...")
            # Read CSV with database schema-based data types
            df = self._read_csv_with_error_handling(csv_path, table_name, db_path)
            
            # Get table schema for date preprocessing
            table_schema = self._get_table_schema(table_name, db_path)
            
            print(f"  Inserting {len(df)} rows into {table_name}...")
            
            # Insert rows using the same method as updates for consistency
            for index, row in df.iterrows():
                self._insert_row(conn, table_name, row, table_schema)
            
            print(f"  SUCCESS: Appended {len(df)} rows to {table_name}")
            
        except Exception as e:
            error_data = {
                "file": os.path.basename(csv_path),
                "error": f"Append operation failed: {str(e)}",
                "table": table_name
            }
            self.log_error(error_data, os.path.dirname(csv_path))
            print(f"  ERROR: Append failed - {str(e)}")
        finally:
            conn.close()
    
    def process_update_job(self, csv_path: str, table_name: str, db_path: str, filter_fields: List[str]):
        """
        Process update CSV files with filtering logic - optimized for batch processing
        """
        conn = duckdb.connect(db_path)
        
        try:
            print(f"  Reading CSV data...")
            # Read CSV data in chunks for large files (optimized processing)
            # Use database schema-based data types
            chunk_size = 1000  # Process 1000 rows at a time for optimal performance
            
            # Get table schema for date preprocessing
            table_schema = self._get_table_schema(table_name, db_path)
            
            try:
                # Get schema-based dtypes for chunked reading
                dtypes = self._get_csv_dtypes_from_schema(csv_path, table_name, db_path)
                csv_reader = pd.read_csv(csv_path, chunksize=chunk_size, dtype=dtypes, keep_default_na=False)
            except Exception as conversion_error:
                # If type conversion fails, log error and use string types
                error_data = {
                    "file": os.path.basename(csv_path),
                    "error": f"CSV type conversion failed, using string types: {str(conversion_error)}",
                    "table": table_name
                }
                self.log_error(error_data, os.path.dirname(csv_path))
                csv_reader = pd.read_csv(csv_path, chunksize=chunk_size, dtype=str, keep_default_na=False)
            
            total_processed = 0
            total_updated = 0
            total_appended = 0
            total_errors = 0
            
            for chunk_idx, df_chunk in enumerate(csv_reader):
                print(f"  Processing chunk {chunk_idx + 1} ({len(df_chunk)} rows)...")
                
                for index, row in df_chunk.iterrows():
                    total_processed += 1
                    
                    # Build WHERE clause from filter fields
                    where_conditions = []
                    param_values = []
                    
                    for field in filter_fields:
                        if field in row:
                            value = row[field]
                            if pd.notna(value):
                                # Only exclude empty strings for string values
                                if isinstance(value, str) and value.strip() == '':
                                    continue  # Skip empty strings in filter conditions
                                else:
                                    where_conditions.append(f'"{field}" = ?')
                                    param_values.append(value)
                    
                    if not where_conditions:
                        # No filter conditions - log error and skip
                        error_data = {
                            "file": os.path.basename(csv_path),
                            "row": chunk_idx * chunk_size + index + 1,
                            "error": "No valid filter conditions found in row",
                            "filter_fields": filter_fields
                        }
                        self.log_error(error_data, os.path.dirname(csv_path))
                        total_errors += 1
                        continue
                    
                    where_clause = " AND ".join(where_conditions)
                    
                    # Check for existing records
                    query = f"SELECT COUNT(*) as count FROM {table_name} WHERE {where_clause}"
                    result = conn.execute(query, param_values).fetchone()
                    
                    if result[0] == 0:
                        # No match found - append
                        self._insert_row(conn, table_name, row, table_schema)
                        total_appended += 1
                    elif result[0] == 1:
                        # Single match - update
                        self._update_row(conn, table_name, row, where_clause, param_values, table_schema)
                        total_updated += 1
                    else:
                        # Multiple matches - log error
                        filter_values = {}
                        for field in filter_fields:
                            if field in row:
                                value = row[field]
                                if pd.notna(value) and not (isinstance(value, str) and value.strip() == ''):
                                    # Ensure JSON serializable values
                                    if hasattr(value, 'item'):  # numpy types
                                        value = value.item()
                                    elif hasattr(value, 'isoformat'):  # datetime types
                                        value = value.isoformat()
                                    filter_values[field] = value
                        
                        error_data = {
                            "file": os.path.basename(csv_path),
                            "row": chunk_idx * chunk_size + index + 1,  # 1-based indexing
                            "error": "Multiple matching records found",
                            "filter_fields": filter_fields,
                            "filter_values": filter_values,
                            "match_count": int(result[0])
                        }
                        self.log_error(error_data, os.path.dirname(csv_path))
                        total_errors += 1
            
            print(f"  SUCCESS: Processed {total_processed} rows")
            print(f"    Updated: {total_updated}, Appended: {total_appended}, Errors: {total_errors}")
                
        except Exception as e:
            error_data = {
                "file": os.path.basename(csv_path),
                "error": f"Processing failed: {str(e)}",
                "table": table_name
            }
            self.log_error(error_data, os.path.dirname(csv_path))
            print(f"  ERROR: Update processing failed - {str(e)}")
        finally:
            conn.close()
    
    def _insert_row(self, conn, table_name: str, row: pd.Series, table_schema: dict = None):
        """Insert a single row into the table with date preprocessing"""
        # Preprocess row data (including date conversions) if schema is provided
        if table_schema:
            row = self._preprocess_row_data(row, table_schema)
        
        # Build column list and values
        # Include all columns, but convert empty strings to None for proper NULL handling
        columns = []
        values = []
        
        for col in row.index:
            value = row[col]
            if pd.notna(value):
                # Check if it's an empty string (only for string columns)
                if isinstance(value, str) and value.strip() == '':
                    # Handle empty strings as None for proper NULL insertion
                    columns.append(col)
                    values.append(None)
                else:
                    # Valid non-empty value
                    columns.append(col)
                    values.append(value)
            # Skip NaN/None values entirely
        
        if not columns:
            return  # No data to insert
            
        placeholders = ','.join(['?'] * len(values))
        columns_str = ','.join([f'"{col}"' for col in columns])  # Escape column names
        
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        conn.execute(query, values)
    
    def _update_row(self, conn, table_name: str, row: pd.Series, where_clause: str, where_params: List, table_schema: dict = None):
        """Update a single row in the table with date preprocessing"""
        # Preprocess row data (including date conversions) if schema is provided
        if table_schema:
            row = self._preprocess_row_data(row, table_schema)
            
        # Build SET clause for non-filter fields
        set_clauses = []
        set_values = []
        
        for col in row.index:
            value = row[col]
            if pd.notna(value):
                set_clauses.append(f'"{col}" = ?')  # Escape column names
                # Handle empty strings as None for proper NULL handling (only for strings)
                if isinstance(value, str) and value.strip() == '':
                    set_values.append(None)
                else:
                    set_values.append(value)
        
        if not set_clauses:
            return  # Nothing to update
        
        set_clause = ','.join(set_clauses)
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        
        # Combine SET values with WHERE values
        all_params = set_values + where_params
        conn.execute(query, all_params)
    
    def log_error(self, error_data: Dict, job_folder: str):
        """Append errors to errors.json file"""
        error_file_path = os.path.join(job_folder, self.error_log_filename)
        
        # Load existing errors or create new structure
        if os.path.exists(error_file_path):
            try:
                with open(error_file_path, 'r', encoding='utf-8') as f:
                    error_log = json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                error_log = {"timestamp": datetime.now().isoformat(), "total_errors": 0, "errors": []}
        else:
            error_log = {"timestamp": datetime.now().isoformat(), "total_errors": 0, "errors": []}
        
        # Add new error
        error_log["errors"].append(error_data)
        error_log["total_errors"] = len(error_log["errors"])
        error_log["timestamp"] = datetime.now().isoformat()  # Update timestamp
        
        # Write back to file
        try:
            with open(error_file_path, 'w', encoding='utf-8') as f:
                json.dump(error_log, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Warning: Failed to write error log: {e}")


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
            if os.path.isabs(args.job_folder):
                job_folder = args.job_folder
            else:
                job_folder = os.path.join(updater.base_dir, args.job_folder)
        else:
            job_folder = updater.find_latest_update_folder()
        
        if not os.path.exists(job_folder):
            print(f"Error: Job folder not found: {job_folder}")
            sys.exit(1)
        
        print(f"Processing job folder: {job_folder}")
        
        # Extract timestamp from folder name
        folder_name = os.path.basename(job_folder)
        if not folder_name.endswith('_update') or len(folder_name) != 13:
            print(f"Error: Invalid folder name format. Expected: YYMMDD_update")
            sys.exit(1)
        
        timestamp = folder_name[:6]  # YYMMDD
        
        # Duplicate database
        db_path = None
        if not args.dry_run:
            db_path = updater.duplicate_database(DATABASE_PATH, job_folder, timestamp)
            print(f"Created database copy: {os.path.basename(db_path)}")
        else:
            print(f"DRY RUN: Would create database copy: tax_db_{timestamp}.duckdb")
        
        # Process CSV files
        print(f"\n{'='*50}")
        print(f"{'DRY RUN - ' if args.dry_run else ''}Processing CSV files...")
        print(f"{'='*50}")
        
        updater.process_csv_files(job_folder, db_path, dry_run=args.dry_run)
        
        print(f"\n{'='*50}")
        print("Processing completed successfully")
        print(f"{'='*50}")
        
        # Check for errors
        error_file = os.path.join(job_folder, updater.error_log_filename)
        if os.path.exists(error_file):
            with open(error_file, 'r') as f:
                error_data = json.load(f)
            print(f"Warning: {error_data['total_errors']} errors logged in {updater.error_log_filename}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()