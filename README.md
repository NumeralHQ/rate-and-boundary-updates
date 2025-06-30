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
└── src/
    ├── __init__.py
    ├── main.py                     # Main script entry point
    ├── config.py                   # Configuration constants
    ├── db_handler.py               # Database connection and queries
    ├── file_handler.py             # File I/O operations
    └── logger.py                   # Error and warning logging
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
1. You will be asked to select a job type. Enter `1` for a Rate Update.
2. The script will find the latest applicable job file and ask for your confirmation. Enter `Y` to proceed.
3. You will be prompted to enter an effective date:
   - Enter a specific date in MM/DD/YYYY format (e.g., `12/31/2025`)
   - Or enter `0` to use today's date

### Step 4: Review Output
Upon completion, the script will print a summary and the path to a new folder in the `/output` directory.

Navigate to this folder to find:
- `{job_type}_output.csv`: The generated data rows ready for review and import
  - Each row includes a `status` column (first column) indicating processing results
  - Status values: `"Success"` (no issues) or warning/error descriptions
- `errors.json`: (If generated) A file containing detailed warnings and errors for debugging

## Job File Format (rate_update_*.csv)

The job file for rate updates requires the following columns. The fields `tax_type`, `tax_cat`, and `new_rate` are mandatory for the script to run, while other fields will produce more specific results.

| Column      | Description                    | Type    | Example           |
|-------------|--------------------------------|---------|-------------------|
| geocode     | Specific geocode to target     | VARCHAR | 060750000         |
| state       | State abbreviation             | VARCHAR | CA                |
| county      | County name                    | VARCHAR | SAN FRANCISCO     |
| city        | City name                      | VARCHAR | SAN FRANCISCO     |
| description | Tax description to match       | VARCHAR | CITY SALES TAX    |
| tax_type    | Tax type to match (required)   | VARCHAR | 04                |
| tax_cat     | Tax category to match (required)| VARCHAR | 02                |
| old_rate    | The expected current rate in DB| DECIMAL | 1.5               |
| new_rate    | The new rate to apply (required)| DECIMAL | 1.75              |

**Note on Rates**: Rates are represented as percentages (e.g., 1.5 for 1.5%). The script will automatically handle the conversion to the decimal format required by the database (e.g., 0.015).

**Note on Tax Codes**: Both `tax_type` and `tax_cat` are automatically formatted as 2-digit codes with leading zeros (e.g., 4 becomes "04"). Ensure your CSV contains the numeric values that correspond to your database schema.

## Output File Format

The generated output CSV file contains a `status` column as the first column, followed by all columns from the detail table with updated values.

### Status Column Values

| Status | Description |
|--------|-------------|
| `Success` | Row processed without any issues |
| `Warning: rate mismatch` | The old_rate in job file doesn't match database rate |
| `Warning: failed to compare rates` | Error occurred while comparing rates |
| `Error: invalid new_rate` | The new_rate value is invalid or malformed |

**Note**: Rows missing required fields (`tax_type`, `tax_cat`, `new_rate`) are skipped entirely and do not appear in the output file. These errors are logged in the errors.json file.

**Note**: Multiple issues are separated by line breaks within the same status cell.

## Features

- **Interactive CLI**: Guides users through job selection and confirmation
- **Custom Effective Dates**: Users can specify exact effective dates or use today's date
- **Automatic File Discovery**: Finds the latest job file based on date in filename
- **Dynamic Database Queries**: Handles incomplete location data gracefully
- **Rate Validation**: Warns when old rates don't match database values
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

## Example Usage

1. Place a job file like `rate_update_250627.csv` in the `/job` directory
2. Run `python src/main.py`
3. Select option `1` for Rate Update
4. Confirm processing when prompted
5. Review the generated output files in the timestamped folder
6. Check the `status` column in the output CSV to identify any issues with specific rows
7. Use `errors.json` for detailed debugging information if needed

## Future Enhancements

- Additional job types (New Jurisdiction, Jurisdiction Update)
- Batch processing capabilities
- Enhanced validation rules
- GUI interface option

## Troubleshooting

- **Database Connection Issues**: Verify the database path in `src/config.py`
- **Job File Not Found**: Ensure the file follows the naming convention `rate_update_YYMMDD.csv`
- **Permission Errors**: Ensure write permissions to the `/output` directory
- **Import Errors**: Verify all dependencies are installed via `pip install -r requirements.txt`