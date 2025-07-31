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

## Future Enhancements

- Additional job types (New Jurisdiction, Jurisdiction Update)
- Batch processing capabilities
- Enhanced validation rules
- GUI interface option

## Troubleshooting

- **Database Connection Issues**: Verify the database path in `src/config.py`
- **Job File Not Found**: Ensure the file follows the naming convention:
  - `rate_update_YYMMDD.csv` for Rate Update jobs
  - `new_tax_YYMMDD.csv` for New Tax jobs  
  - `new_authority_YYMMDD.csv` for New Authority jobs
- **Permission Errors**: Ensure write permissions to the `/output` directory
- **Import Errors**: Verify all dependencies are installed via `pip install -r requirements.txt`
- **Authority ID Issues**: Ensure the tax_authority table exists and contains valid numeric tax_auth_id values