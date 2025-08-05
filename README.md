# Tax Data Update Utility

## Overview

The Tax Data Update Utility is a command-line tool designed to automate the generation of data for updating tax tables in a local DuckDB database. It prioritizes safety and accuracy by producing `.csv` files for manual review and import, rather than writing directly to the database.

The tool is designed to be extensible, with the initial version supporting a "Rate Update" job type.

## Project Structure

```
rate-and-boundary-updates/
├── .gitignore
├── README.md
├── requirements.txt
├── job/
│   └── rate_update_250627.csv      # Input job files go here
├── output/
│   └── 250627-114530_job/          # Timestamped output folders are created here
│       ├── rate_update_output.csv
│       └── errors.json
├── src/
│   ├── __init__.py
│   ├── main.py                     # Main script entry point
│   ├── config.py                   # Configuration constants
│   ├── db_handler.py               # Database connection and queries
│   ├── file_handler.py             # File I/O operations
│   └── logger.py                   # Error and warning logging
└── table_updates/                  # Table update functionality
    ├── table_updater.py            # Table update script
    ├── filtering_criteria.json     # Table filtering configuration
    ├── tests/                      # Test suite
    │   ├── __init__.py
    │   ├── test_complete_table_update.py
    │   ├── test_dry_run.py
    │   └── test_error_handling.py
    └── 250801_update/              # Example timestamped job folder
        ├── detail_append_1.csv
        ├── product_item_update_1.csv
        ├── errors.json             # Generated error log
        └── tax_db_250801.duckdb    # Generated database copy
```

## Prerequisites

- Python 3.13+
- DuckDB Python package (`pip install duckdb`)
- Pandas (`pip install pandas`) for data manipulation
- The DuckDB database file (`tax_database.duckdb`) must be accessible at the path configured in the script

## Installation

1. Navigate to the project directory:
   ```bash
   cd rate-and-boundary-updates
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure the database path in `src/config.py` if needed (default is `C:\Users\Gregg\Documents\tax_db_tables\tax_database.duckdb`)

## How to Run

### Step 1: Place Job File
Ensure the relevant job file (e.g., `rate_update_250627.csv`) is placed in the `/job` directory.

### Step 2: Execute the Script
Run the script from the root directory of the project:

```bash
python src/main.py
```

### Step 3: Follow Prompts
1. You will be asked to select a job type:
   - Enter `1` for a Rate Update
   - Enter `2` for a New Tax
   - Enter `3` for a New Authority
2. The script will find the latest applicable job file and ask for your confirmation. Enter `Y` to proceed.
3. You will be prompted to enter an effective date (Rate Update and New Tax only):
   - Enter a specific date in MM/DD/YYYY format (e.g., `12/31/2025`)
   - Or enter `0` to use today's date
   - Note: New Authority jobs do not require an effective date

### Step 4: Review Output
Upon completion, the script will print a summary and the path to a new folder in the `/output` directory.

Navigate to this folder to find:
- `{job_type}_output.csv`: The generated data rows ready for review and import
  - Each row includes a `status` column (first column) indicating processing results
  - Status values: `"Success"` (no issues) or warning/error descriptions
  - Rate Update and New Tax jobs output to detail table format
  - New Authority jobs output to tax_authority table format
- `errors.json`: (If generated) A file containing detailed warnings and errors for debugging

## Job File Format (rate_update_*.csv)

The job file for rate updates requires the following columns. The fields `tax_type`, `tax_cat`, `new_rate`, `old_fee`, and `new_fee` are mandatory for the script to run, while other fields will produce more specific results.

| Column | Description | Type | Required | Default | Example |
|--------|-------------|------|----------|---------|---------|
| geocode | Specific geocode to target | VARCHAR | No | - | US0210559650 |
| state | State abbreviation | VARCHAR | No | - | AK |
| county | County name | VARCHAR | No | - | HOONAH-ANGOON |
| city | City name | VARCHAR | No | - | PELICAN |
| description | Tax description to match | VARCHAR | No | - | CITY SALES TAX |
| tax_type | Tax type to match | VARCHAR | Yes | - | 04 |
| tax_cat | Tax category to match | VARCHAR | Yes | - | 01 |
| old_rate | The expected current rate in DB | DECIMAL | No | - | 4 |
| new_rate | The new rate to apply | DECIMAL | Yes | - | 6 |
| old_fee | The expected current fee in DB | DECIMAL | Yes | - | 0 |
| new_fee | The new fee to apply | DECIMAL | Yes | - | 0.25 |

**Note on Rates**: Rates are represented as percentages (e.g., 1.5 for 1.5%). The script will automatically handle the conversion to the decimal format required by the database (e.g., 0.015).

**Note on Fees**: Fees are represented as dollar amounts (e.g., 1.25 for $1.25). Fees must be non-negative values (>= 0).

**Note on Tax Codes**: Both `tax_type` and `tax_cat` are automatically formatted as 2-character codes. Numeric values are padded with leading zeros (e.g., 4 becomes "04"), while alphanumeric values are used as-is and converted to uppercase (e.g., "ff" becomes "FF"). Ensure your CSV contains the values that correspond to your database schema.

## Job File Format (new_tax_*.csv)

The job file for new tax creation requires the following columns. The fields `tax_type`, `tax_rate`, `tax_auth_id`, and `description` are mandatory for the script to run.

| Column | Description | Type | Required | Default | Example |
|--------|-------------|------|----------|---------|---------|
| geocode | Comma-separated geocodes | VARCHAR | No | From lookup | US0602909780 |
| state | State abbreviation | VARCHAR | No | - | CA |
| county | County name | VARCHAR | No | - | KERN |
| city | City name | VARCHAR | No | - | CALIFORNIA CITY |
| tax_district | Tax district name | VARCHAR | No | - | DOWNTOWN |
| tax_type | Tax type | VARCHAR | Yes | - | 04 |
| tax_cat | Tax category | VARCHAR | No | 01 | 01 |
| tax_auth_id | Tax authority ID | VARCHAR | Yes | - | 27631 |
| effective | Effective date (MM/DD/YYYY) | DATE | No | User input/today | 01/01/2025 |
| description | Tax description | VARCHAR | Yes | - | CITY SALES TAX |
| pass_flag | Pass flag | VARCHAR | No | 01 | 01 |
| pass_type | Pass type | VARCHAR | No | (blank) | |
| base_type | Base type | VARCHAR | No | 00 | 00 |
| date_flag | Date flag | VARCHAR | No | 02 | 02 |
| rounding | Rounding | VARCHAR | No | 00 | 00 |
| location | Location | VARCHAR | No | (blank) | |
| report_to | Report to | INTEGER | No | (blank) | |
| max_tax | Maximum tax | DECIMAL | No | 0 | 0 |
| unit_type | Unit type | VARCHAR | No | 99 | 99 |
| max_type | Maximum type | VARCHAR | No | 99 | 99 |
| thresh_type | Threshold type | VARCHAR | No | 09 | 09 |
| unit_and_or_tax | Unit and/or tax | VARCHAR | No | (blank) | |
| formula | Formula | VARCHAR | No | 01 | 01 |
| tier | Tier | INTEGER | No | 0 | 0 |
| tax_rate | Tax rate | DECIMAL | Yes | - | 1.0 |
| min_tax_base | Minimum tax base | DECIMAL | No | 0 | 0 |
| max_tax_base | Maximum tax base | DECIMAL | No | 0 | 0 |
| fee | Fee | DECIMAL | No | 0 | 0 |
| min_unit_base | Minimum unit base | DECIMAL | No | 0 | 0 |
| max_unit_base | Maximum unit base | DECIMAL | No | 0 | 0 |

**Note on New Tax Processing**: The new tax job creates detail table records for each geocode found based on the location criteria. Multiple output rows may be generated from a single input row if multiple geocodes match the criteria.

## Job File Format (new_authority_*.csv)

The job file for new authority creation requires the following columns. None of the fields are strictly mandatory, but providing more specific information results in more accurate authority records.

| Column | Description | Type | Required | Default | Example |
|--------|-------------|------|----------|---------|---------|
| country | Country code | VARCHAR | No | US (with warning if defaulted) | US |
| state | State abbreviation | VARCHAR | No | - | CA |
| county | County name | VARCHAR | No | - | KERN |
| city | City name | VARCHAR | No | - | CALIFORNIA CITY |
| district | District name | VARCHAR | No | - | DOWNTOWN |

**Authority Level Detection**: The system determines the authority level based on the lowest (most specific) jurisdiction provided:
- If `district` is provided → District level authority
- Else if `city` is provided → City level authority  
- Else if `county` is provided → County level authority
- Else if `state` is provided → State level authority
- Else → Country level authority (defaults to US)

**Authority Name Formatting**: Names are automatically formatted based on the authority level:
- **Country**: `{COUNTRY}` (e.g., "US")
- **State**: `{STATE}, STATE OF` (e.g., "CA, STATE OF") 
- **County**: `{COUNTY}, COUNTY OF` (e.g., "LOS ANGELES, COUNTY OF")
- **City**: `{CITY}, CITY OF` (e.g., "SAN FRANCISCO, CITY OF")
- **District**: 
  - If city provided: `CITY OF {CITY}, {DISTRICT}` (e.g., "CITY OF SEATTLE, DOWNTOWN")
  - Else if county provided: `{COUNTY} COUNTY, {DISTRICT}` (e.g., "KING COUNTY, METRO TRANSIT")
  - Else: `{DISTRICT}` (with warning)

**Note on Authority Processing**: All text values are automatically converted to uppercase and trimmed of whitespace. Sequential tax_auth_id values are assigned starting from the next available ID in the database.

## Output File Format

The generated output CSV file contains a `status` column as the first column, followed by all columns from the detail table with updated values.

### Status Column Values

#### Rate Update Job
| Status | Description |
|--------|-------------|
| `Success` | Row processed without any issues |
| `Warning: rate mismatch` | The old_rate in job file doesn't match database rate |
| `Warning: failed to compare rates` | Error occurred while comparing rates |
| `Warning: fee mismatch` | The old_fee in job file doesn't match database fee |
| `Warning: failed to compare fees` | Error occurred while comparing fees |
| `Error: invalid new_rate` | The new_rate value is invalid or malformed |
| `Error: invalid new_fee` | The new_fee value is invalid or malformed |
| `Error: negative fee not allowed` | The new_fee value is negative (fees must be >= 0) |

#### New Tax Job  
| Status | Description |
|--------|-------------|
| `Success` | Row processed without any issues |
| `Warning: invalid effective date format` | Effective date in CSV couldn't be parsed |
| `Warning: invalid tax_rate` | Tax rate value is invalid or malformed |

#### New Authority Job
| Status | Description |
|--------|-------------|
| `Success` | Row processed without any issues |
| `Warning: defaulted country to US` | Country field was empty, defaulted to US |
| `Warning: missing state for non-country authority` | State required but not provided for state/county/city/district level |
| `Warning: missing city or county for district authority` | District level authority without parent city or county |

**Note**: Rows missing required fields are skipped entirely and do not appear in the output file. These errors are logged in the errors.json file.

**Note**: Multiple issues are separated by line breaks within the same status cell.

## Features

- **Multiple Job Types**: Supports Rate Update, New Tax, and New Authority creation workflows
- **Interactive CLI**: Guides users through job selection and confirmation
- **Custom Effective Dates**: Users can specify exact effective dates or use today's date (Rate Update and New Tax)
- **Automatic File Discovery**: Finds the latest job file based on date in filename
- **Dynamic Database Queries**: Handles incomplete location data gracefully
- **Advanced Geocode Lookup**: Supports comma-separated geocodes and tax_district filtering (New Tax)
- **Rate Validation**: Warns when old rates don't match database values (Rate Update)
- **Field Defaulting**: Applies intelligent defaults for missing fields (New Tax)
- **Authority Level Detection**: Automatically determines jurisdiction level and formats names (New Authority)
- **Sequential ID Assignment**: Assigns unique tax_auth_id values automatically (New Authority)
- **Text Normalization**: Converts all text to uppercase and trims whitespace (New Authority)
- **Comprehensive Logging**: Tracks warnings and errors for audit trails
- **Timestamped Output**: Each run creates a unique output folder
- **Safe Operation**: Never writes directly to the database

## Database Schema

The application expects the following database tables:

### geocode table
```sql
CREATE TABLE geocode (
    country VARCHAR,
    state VARCHAR,
    county VARCHAR,
    city VARCHAR,
    tax_district VARCHAR,
    geocode VARCHAR,
    gnis VARCHAR
);
```

### detail table
```sql
CREATE TABLE detail (
    geocode VARCHAR,
    tax_type VARCHAR,
    tax_cat VARCHAR,
    tax_auth_id VARCHAR,
    effective TIMESTAMP,
    description VARCHAR,
    pass_flag VARCHAR,
    pass_type VARCHAR,
    base_type VARCHAR,
    date_flag VARCHAR,
    rounding VARCHAR,
    location VARCHAR,
    report_to INTEGER,
    max_tax DECIMAL(7,2),
    unit_type VARCHAR,
    max_type VARCHAR,
    thresh_type VARCHAR,
    unit_and_or_tax VARCHAR,
    formula VARCHAR,
    tier INTEGER,
    tax_rate DECIMAL(13,12),
    min_tax_base DECIMAL(10,2),
    max_tax_base DECIMAL(10,2),
    fee DECIMAL(11,8),
    min_unit_base DECIMAL(14,5),
    max_unit_base DECIMAL(14,5)
);
```

### tax_authority table
```sql
CREATE TABLE tax_authority (
    tax_auth_id VARCHAR,
    country VARCHAR,
    state VARCHAR,
    authority_name VARCHAR,
    tax_auth_type VARCHAR
);
```

## Example Usage

### Rate Update Job
1. Place a job file like `rate_update_250627.csv` in the `/job` directory
2. Run `python src/main.py`
3. Select option `1` for Rate Update
4. Confirm processing when prompted
5. Enter effective date or use default
6. Review the generated output files in the timestamped folder

### New Tax Job
1. Place a job file like `new_tax_250627.csv` in the `/job` directory
2. Run `python src/main.py`
3. Select option `2` for New Tax
4. Confirm processing when prompted
5. Enter effective date (used only if not specified in job file)
6. Review the generated output files in the timestamped folder

### New Authority Job
1. Place a job file like `new_authority_250627.csv` in the `/job` directory
2. Run `python src/main.py`
3. Select option `3` for New Authority
4. Confirm processing when prompted
5. No effective date needed - processing begins immediately
6. Review the generated output files in the timestamped folder

### Output Review
- Check the `status` column in the output CSV to identify any issues with specific rows
- Use `errors.json` for detailed debugging information if needed

## Table Update Functionality

### Overview

The Tax Data Update Utility now includes a powerful table update feature that processes CSV files to directly update or append data to existing DuckDB tables. This functionality is designed for bulk operations on tax database tables with comprehensive error handling and validation.

**Location**: `table_updates/table_updater.py`

### Key Features

- **Schema Validation**: Automatically validates CSV field names against target table schemas
- **Dual Operation Modes**: Supports both append and update operations
- **Batch Processing**: Optimized for large CSV files with chunked processing
- **Error Recovery**: Continues processing even when individual files fail
- **Comprehensive Logging**: Detailed error tracking in JSON format
- **Dry Run Mode**: Validate operations without making actual changes
- **Performance Optimized**: Uses DuckDB native CSV import for maximum speed
- **Schema-Based Type Mapping**: Consults database schema for authoritative data type handling
- **Excel Date Auto-Correction**: Automatically converts Excel date formats (M/D/YYYY) to database format (YYYY-MM-DD)
- **Intelligent Type Conversion**: Preserves data formatting (e.g., zero-padded codes) based on database column types

### Advanced Data Handling

#### Schema-Based Type Mapping
The table updater now uses the **database schema as the single source of truth** for data type handling:
- **VARCHAR/CHAR columns**: Automatically preserved as strings to maintain formatting (e.g., `"004"` stays `"004"`)
- **INTEGER columns**: Converted to nullable Int64 for proper handling
- **FLOAT/DECIMAL columns**: Converted to float64 with appropriate precision
- **DATE columns**: Handled as strings with automatic format conversion

#### Excel Date Format Auto-Correction
Automatically handles Excel's date format corruption:
- **Input**: `7/1/2025` (Excel's M/D/YYYY format)
- **Output**: `2025-07-01` (Database-compatible YYYY-MM-DD format)
- **Supported formats**: M/D/YYYY, MM/DD/YYYY, YYYY/M/D, YYYY-MM-DD (unchanged)
- **Error handling**: Invalid dates are logged but processing continues

#### Consistent Data Integrity
- **Unified processing**: Same type handling logic for both append and update operations
- **Zero-padding preservation**: Item codes like `"004"` maintain their leading zeros
- **Empty string handling**: Properly converted to NULL values for database insertion
- **Type conversion errors**: Gracefully handled with detailed error logging

### File Structure Requirements

The table updater expects a specific directory structure:

```
table_updates/
├── table_updater.py          # Main script
├── filtering_criteria.json   # Table filtering configuration
└── {YYMMDD}_update/          # Timestamped job folders
    ├── {table}_{operation}_{seq}.csv  # CSV files to process
    ├── errors.json           # Generated error log
    └── tax_db_{YYMMDD}.duckdb # Generated database copy
```

### CSV File Naming Convention

CSV files must follow the naming pattern: `{table_name}_{job_type}_{sequential_number}.csv`

- **table_name**: Exact name of the target table in the database (e.g., `detail`, `product_item`, `matrix`)
- **job_type**: Either `append` or `update`
- **sequential_number**: Numeric identifier to prevent filename conflicts

**Examples**:
- `detail_append_1.csv` - Appends new records to the detail table
- `product_item_update_1.csv` - Updates existing records in the product_item table
- `matrix_append_2.csv` - Second append file for the matrix table

### Operation Types

#### Append Operations
- **Purpose**: Add new records to existing tables
- **Behavior**: All CSV rows are inserted as new records
- **Performance**: Uses DuckDB's native CSV import for maximum speed
- **Validation**: Schema validation ensures CSV columns match table structure

#### Update Operations
- **Purpose**: Modify existing records or add new ones if no match found
- **Behavior**: Uses filtering criteria to identify matching records
  - **Single Match**: Updates the existing record
  - **No Match**: Inserts as new record
  - **Multiple Matches**: Logs error and skips record
- **Filtering**: Based on `filtering_criteria.json` configuration

### Filtering Criteria Configuration

The `filtering_criteria.json` file defines which fields are used to identify matching records for update operations:

```json
{
    "detail": {
        "filter_fields": ["geocode", "tax_type", "tax_cat", "tax_auth_id", "effective"]
    },
    "product_item": {
        "filter_fields": ["group", "item"]
    },
    "matrix": {
        "filter_fields": ["geocode", "group", "item", "tax_type", "tax_cat", "customer", "provider"]
    }
}
```

### Usage

#### Basic Commands

```bash
# Process the latest timestamped folder
python table_updates/table_updater.py

# Dry run to validate without making changes
python table_updates/table_updater.py --dry-run

# Process a specific folder
python table_updates/table_updater.py --job-folder table_updates/250801_update
```

#### Workflow Steps

1. **Prepare CSV Files**: Create properly named CSV files with correct schemas
2. **Create Job Folder**: Place files in a timestamped folder (`YYMMDD_update`)
3. **Run Script**: Execute the table updater (optionally with --dry-run first)
4. **Review Results**: Check console output and `errors.json` for any issues
5. **Verify Database**: The updated database is saved as `tax_db_{YYMMDD}.duckdb`

### Error Handling

The system provides comprehensive error tracking:

#### Error Types
- Invalid CSV file naming format
- CSV schema validation failures (field name mismatches)
- Missing filtering criteria for update operations
- Multiple record matches during updates
- Database connection/transaction failures
- CSV parsing errors
- Data type conversion failures (with fallback to string types)
- Date format conversion issues (invalid date formats)
- Database insertion errors due to type mismatches

#### Error Log Format

Errors are logged to `errors.json` in the job folder:

```json
{
    "timestamp": "2025-01-15T10:30:00Z",
    "total_errors": 2,
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
            "file": "detail_update_1.csv",
            "row": 5,
            "error": "Multiple matching records found",
            "filter_fields": ["geocode", "tax_type"],
            "filter_values": {"geocode": "US123", "tax_type": "04"},
            "match_count": 3
        }
    ]
}
```

### Performance Considerations

- **Large Files**: The system uses chunked processing (1000 rows per chunk) for optimal memory usage
- **Append Operations**: Leverages DuckDB's native `read_csv_auto()` for maximum speed
- **Update Operations**: Batch processes records while maintaining transaction integrity
- **Memory Efficient**: Processes files incrementally rather than loading everything into memory

### Best Practices

1. **Always Use Dry Run First**: Validate your files with `--dry-run` before actual processing
2. **Schema Validation**: Ensure CSV columns exactly match target table columns
3. **Excel Date Handling**: The system automatically corrects Excel date formats, but be aware of potential corruption when opening CSV files in Excel
4. **Backup Strategy**: The script creates database copies, but maintain separate backups
5. **Error Review**: Always check `errors.json` after processing for any issues
6. **File Organization**: Use clear, descriptive sequential numbers for multiple files
7. **Data Type Consistency**: Let the system handle data types automatically based on database schema

### Dependencies

The table updater requires:
- `pandas>=1.5.0` - For CSV processing
- `duckdb>=0.9.0` - For database operations

### Integration with Existing Workflow

The table updater complements the existing tax data utility:
- **Existing Tool**: Generates CSV files for manual review and import
- **Table Updater**: Automates the import process with validation and error handling
- **Combined Use**: Generate updates with the main tool, then process with the table updater

## Future Enhancements

- Additional job types (New Jurisdiction, Jurisdiction Update)
- Enhanced table updater features (parallel processing, web interface)
- Batch processing capabilities
- Enhanced validation rules
- GUI interface option

## Troubleshooting

### General Issues
- **Database Connection Issues**: Verify the database path in `src/config.py`
- **Job File Not Found**: Ensure the file follows the naming convention:
  - `rate_update_YYMMDD.csv` for Rate Update jobs
  - `new_tax_YYMMDD.csv` for New Tax jobs  
  - `new_authority_YYMMDD.csv` for New Authority jobs
- **Permission Errors**: Ensure write permissions to the `/output` directory
- **Import Errors**: Verify all dependencies are installed via `pip install -r requirements.txt`
- **Authority ID Issues**: Ensure the tax_authority table exists and contains valid numeric tax_auth_id values

### Table Updater Issues
- **No Update Folders Found**: Create a timestamped folder in `table_updates/` following the `YYMMDD_update` format
- **Schema Validation Failures**: Ensure CSV column names exactly match target table column names (case-insensitive)
- **Missing Filtering Criteria**: Add the target table to `filtering_criteria.json` with appropriate filter fields
- **File Permission Errors**: Ensure write permissions to the job folder for database copy and error log creation
- **Multiple Match Errors**: Review filter fields in `filtering_criteria.json` to ensure they uniquely identify records
- **CSV Parsing Errors**: Verify CSV files are properly formatted and don't contain corrupted data
- **Database Copy Issues**: Ensure the source database path in `config.py` is correct and accessible
- **Large File Processing**: For very large files (>100k rows), consider splitting into smaller files for better error tracking