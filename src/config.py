# src/config.py
import os

# --- File Paths ---
# Use os.path.join for cross-platform compatibility.
# Assume the script is run from the root `tax_data_utility/` directory.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # This should resolve to the project root

DATABASE_PATH = r"C:\Users\Gregg\Documents\tax_db_tables\tax_database.duckdb"
JOB_FOLDER = os.path.join(BASE_DIR, "job")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "output")

# --- Job Configuration ---
JOB_TYPE_MAPPING = {
    "1": {
        "name": "Rate Update",
        "file_prefix": "rate_update"
    },
    "2": {
        "name": "New Tax",
        "file_prefix": "new_tax"
    },
    "3": {
        "name": "New Jurisdiction",
        "file_prefix": "new_jurisdiction"
    },
    "4": {
        "name": "Jurisdiction Update",
        "file_prefix": "jurisdiction_update"
    }
}

# --- Database Schema ---
# This helps in ensuring the output CSV has the correct column order
# Note: 'status' is added as the first column for output tracking
DETAIL_TABLE_SCHEMA = [
    'status', 'geocode', 'tax_type', 'tax_cat', 'tax_auth_id', 'effective', 'description',
    'pass_flag', 'pass_type', 'base_type', 'date_flag', 'rounding', 'location',
    'report_to', 'max_tax', 'unit_type', 'max_type', 'thresh_type',
    'unit_and_or_tax', 'formula', 'tier', 'tax_rate', 'min_tax_base',
    'max_tax_base', 'fee', 'min_unit_base', 'max_unit_base'
]

# --- New Tax Job Configuration ---
NEW_TAX_DEFAULTS = {
    'tax_cat': '01',
    'pass_flag': '01',
    'pass_type': '',  # Leave blank
    'base_type': '00',
    'date_flag': '02',
    'rounding': '00',
    'location': '',   # Leave blank
    'report_to': None,  # Leave blank
    'max_tax': 0,
    'unit_type': '99',
    'max_type': '99',
    'thresh_type': '09',
    'unit_and_or_tax': '',  # Leave blank
    'formula': '01',
    'tier': 0,
    'min_tax_base': 0,
    'max_tax_base': 0,
    'fee': 0,
    'min_unit_base': 0,
    'max_unit_base': 0
}

NEW_TAX_REQUIRED_FIELDS = ['tax_type', 'tax_rate', 'tax_auth_id', 'description'] 