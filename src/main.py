# src/main.py
# This file ties everything together.

# --- Imports ---
import datetime
import pandas as pd
import os
import sys
from decimal import Decimal

# Add the project root to Python path to handle imports when running directly
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import config, db_handler, file_handler, logger

# --- Helper Functions ---
def get_effective_date_from_user():
    """
    Prompt user for effective date and return parsed datetime.
    Returns None if user cancels or provides invalid input after multiple attempts.
    """
    max_attempts = 3
    attempts = 0
    
    while attempts < max_attempts:
        try:
            user_input = input("\nEnter the Effective Date you would like to use 'MM/DD/YYYY', or enter '0' to set as today's date: ").strip()
            
            if user_input == '0':
                # Use today's date
                today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                return today
            
            # Try to parse MM/DD/YYYY format
            effective_date = datetime.datetime.strptime(user_input, '%m/%d/%Y')
            return effective_date
            
        except ValueError:
            attempts += 1
            remaining = max_attempts - attempts
            if remaining > 0:
                print(f"Invalid date format. Please use MM/DD/YYYY format (e.g., 12/31/2025) or enter '0' for today. {remaining} attempts remaining.")
            else:
                print("Too many invalid attempts. Please restart the application.")
                return None
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            return None
    
    return None

# --- Processing Functions ---
def process_rate_update_job(db_connection, job_df: pd.DataFrame, effective_date: datetime.datetime) -> list:
    """
    Process rate update job with existing logic.
    Returns list of output rows with status tracking.
    """
    output_rows = []
    
    print("\nProcessing rows...")
    
    for index, job_row in job_df.iterrows():
        row_number = index + 1
        print(f"Processing row {row_number}/{len(job_df)}", end="\r")
        
        # Validate required fields
        if pd.isna(job_row.get('tax_type')):
            logger.log_error(f"Row {row_number}: Missing required field 'tax_type'. Skipping.", 
                            {"row_number": row_number, "row_data": job_row.to_dict()})
            continue
        
        if pd.isna(job_row.get('tax_cat')):
            logger.log_error(f"Row {row_number}: Missing required field 'tax_cat'. Skipping.", 
                            {"row_number": row_number, "row_data": job_row.to_dict()})
            continue
        
        if pd.isna(job_row.get('new_rate')):
            logger.log_error(f"Row {row_number}: Missing required field 'new_rate'. Skipping.", 
                            {"row_number": row_number, "row_data": job_row.to_dict()})
            continue
        
        # Get list of geocodes from db_handler.
        geocodes = db_handler.get_geocodes_from_db(db_connection, job_row)
        
        # If no geocodes, log an error and continue to next row.
        if not geocodes:
            logger.log_error(f"Row {row_number}: No geocodes found for criteria. Skipping.", 
                            {"row_number": row_number, "criteria": job_row.to_dict()})
            continue
        
        # Get matching detail rows from db_handler.
        # Ensure tax_type and tax_cat are formatted as 2-digit strings (e.g., 4 -> "04")
        tax_type_raw = job_row['tax_type']
        tax_cat_raw = job_row['tax_cat']
        
        if pd.notna(tax_type_raw):
            # Convert to string and pad with leading zero if needed
            tax_type_formatted = str(int(tax_type_raw)).zfill(2)
        else:
            tax_type_formatted = ""
        
        if pd.notna(tax_cat_raw):
            # Convert to string and pad with leading zero if needed
            tax_cat_formatted = str(int(tax_cat_raw)).zfill(2)
        else:
            tax_cat_formatted = ""
        
        detail_df = db_handler.get_detail_rows_from_db(
            db_connection, 
            geocodes, 
            tax_type_formatted,
            tax_cat_formatted,
            job_row.get('description')
        )
        
        if detail_df.empty:
            logger.log_error(f"Row {row_number}: No detail rows found for geocodes, tax_type, and tax_cat. Skipping.", 
                             {"row_number": row_number, "geocodes": geocodes, 
                              "tax_type_raw": job_row['tax_type'], "tax_type_formatted": tax_type_formatted,
                              "tax_cat_raw": job_row['tax_cat'], "tax_cat_formatted": tax_cat_formatted})
            continue
        
        # Process each detail row
        for _, detail_row in detail_df.iterrows():
            # Initialize status tracking for this output row
            status_issues = []
            
            # Rate Validation: Compare job_row['old_rate'] / 100 with detail_row['tax_rate']
            if pd.notna(job_row.get('old_rate')):
                try:
                    csv_old_rate = Decimal(str(job_row['old_rate'])) / 100
                    db_tax_rate = Decimal(str(detail_row['tax_rate']))
                    
                    if csv_old_rate != db_tax_rate:
                        # Log to errors.json as before
                        logger.log_warning(
                            f"Row {row_number}: Rate mismatch for geocode {detail_row['geocode']}. "
                            f"CSV old_rate: {csv_old_rate}, DB tax_rate: {db_tax_rate}",
                            {
                                "row_number": row_number,
                                "geocode": detail_row['geocode'],
                                "csv_old_rate": float(csv_old_rate),
                                "db_tax_rate": float(db_tax_rate)
                            }
                        )
                        # Add to status for this output row
                        status_issues.append("Warning: rate mismatch")
                        
                except (ValueError, TypeError) as e:
                    # Log to errors.json as before
                    logger.log_warning(f"Row {row_number}: Error when comparing rates: {str(e)}", 
                                       {"row_number": row_number, "error": str(e)})
                    # Add to status for this output row
                    status_issues.append("Warning: failed to compare rates")
            
            # Create a copy of the detail_row and update fields
            new_row = detail_row.copy()
            
            # Set 'effective' to the user-specified date in the correct format: 'YYYY-MM-DD 00:00:00.000'
            new_row['effective'] = effective_date.strftime('%Y-%m-%d 00:00:00.000')
            
            # Set 'tax_rate' to job_row['new_rate'] / 100
            # Note: new_rate is already validated as non-null in the required fields check
            try:
                new_rate_decimal = Decimal(str(job_row['new_rate'])) / 100
                new_row['tax_rate'] = float(new_rate_decimal)
            except (ValueError, TypeError) as e:
                # Log to errors.json as before
                logger.log_error(f"Row {row_number}: Invalid new_rate value: {job_row.get('new_rate')}", 
                                 {"row_number": row_number, "new_rate": job_row.get('new_rate'), "error": str(e)})
                # Add to status for this output row
                status_issues.append("Error: invalid new_rate")
                continue
            
            # Set status based on issues encountered
            if status_issues:
                new_row['status'] = '\n'.join(status_issues)
            else:
                new_row['status'] = 'Success'
            
            # Append the new, updated row to the output list
            output_rows.append(new_row)
    
    return output_rows

def process_new_tax_job(db_connection, job_df: pd.DataFrame, effective_date: datetime.datetime) -> list:
    """
    Process new tax job with field defaulting and multiple geocode handling.
    Returns list of output rows with status tracking.
    """
    output_rows = []
    
    print("\nProcessing rows...")
    
    for index, job_row in job_df.iterrows():
        row_number = index + 1
        print(f"Processing row {row_number}/{len(job_df)}", end="\r")
        
        # Validate required fields for new tax job
        required_fields_valid = True
        for field in config.NEW_TAX_REQUIRED_FIELDS:
            if pd.isna(job_row.get(field)):
                logger.log_error(f"Row {row_number}: Missing required field '{field}'. Skipping.", 
                               {"row_number": row_number, "row_data": job_row.to_dict()})
                required_fields_valid = False
                break
        
        if not required_fields_valid:
            continue
        
        # Get list of geocodes using enhanced lookup for new tax
        geocodes = db_handler.get_geocodes_for_new_tax(db_connection, job_row)
        
        # If no geocodes found, log error and continue
        if not geocodes:
            logger.log_error(f"Row {row_number}: No geocodes found for criteria. Skipping.", 
                           {"row_number": row_number, "criteria": job_row.to_dict()})
            continue
        
        # Process each geocode found - create one output row per geocode
        for geocode in geocodes:
            # Initialize status tracking for this output row
            status_issues = []
            
            # Create new detail row from scratch
            new_row = {}
            
            # Set geocode
            new_row['geocode'] = geocode
            
            # Set values from job CSV or apply defaults
            for field in config.DETAIL_TABLE_SCHEMA:
                if field == 'status':
                    continue  # Handle status separately
                elif field == 'geocode':
                    continue  # Already set above
                elif field == 'effective':
                    # Handle effective date precedence
                    if pd.notna(job_row.get('effective')) and str(job_row.get('effective')).strip():
                        try:
                            # Parse CSV date - assume MM/DD/YYYY format like user input
                            csv_date_str = str(job_row['effective']).strip()
                            parsed_date = datetime.datetime.strptime(csv_date_str, '%m/%d/%Y')
                            new_row['effective'] = parsed_date.strftime('%Y-%m-%d 00:00:00.000')
                        except ValueError:
                            # If can't parse CSV date, use user provided date and add warning
                            new_row['effective'] = effective_date.strftime('%Y-%m-%d 00:00:00.000')
                            status_issues.append("Warning: invalid effective date format")
                    else:
                        # Use user provided effective date (no warning per user request)
                        new_row['effective'] = effective_date.strftime('%Y-%m-%d 00:00:00.000')
                elif field in job_row and pd.notna(job_row[field]):
                    # Use value from job CSV
                    value = job_row[field]
                    
                    # Apply 2-digit formatting for specific fields
                    if field in ['tax_type', 'tax_cat', 'pass_flag', 'base_type', 'date_flag', 
                                'rounding', 'unit_type', 'max_type', 'thresh_type', 'formula']:
                        try:
                            new_row[field] = str(int(value)).zfill(2)
                        except (ValueError, TypeError):
                            new_row[field] = str(value).zfill(2)
                    elif field == 'tax_rate':
                        # Convert percentage to decimal
                        try:
                            tax_rate_decimal = Decimal(str(value)) / 100
                            new_row[field] = float(tax_rate_decimal)
                        except (ValueError, TypeError) as e:
                            logger.log_warning(f"Row {row_number}: Invalid tax_rate value: {value}", 
                                             {"row_number": row_number, "tax_rate": value, "error": str(e)})
                            status_issues.append("Warning: invalid tax_rate")
                            new_row[field] = 0  # Default fallback
                    else:
                        new_row[field] = value
                else:
                    # Apply default value
                    if field in config.NEW_TAX_DEFAULTS:
                        new_row[field] = config.NEW_TAX_DEFAULTS[field]
                    else:
                        new_row[field] = None  # For fields not in defaults
            
            # Set status based on issues encountered
            if status_issues:
                new_row['status'] = '\n'.join(status_issues)
            else:
                new_row['status'] = 'Success'
            
            # Append the new row to output list
            output_rows.append(new_row)
    
    return output_rows

# --- Main Application Logic ---
def run():
    db_connection = None
    
    try:
        # 1. User Interaction: Prompt for job type.
        print("Tax Data Update Utility")
        print("=" * 40)
        print("Select a job type:")
        
        for key, value in config.JOB_TYPE_MAPPING.items():
            print(f"{key}. {value['name']}")
        
        user_choice = input("\nEnter your choice: ").strip()
        
        if user_choice not in config.JOB_TYPE_MAPPING:
            print("Invalid choice. Exiting.")
            return
        
        selected_job = config.JOB_TYPE_MAPPING[user_choice]
        job_name = selected_job['name']
        job_prefix = selected_job['file_prefix']
        
        print(f"\nSelected: {job_name}")
        
        # 2. Find Job File: Use file_handler to find the latest job file.
        print(f"Searching for job files in: {config.JOB_FOLDER}")
        job_file_path = file_handler.find_latest_job_file(config.JOB_FOLDER, job_prefix)
        
        if not job_file_path:
            logger.log_error(f"CRITICAL: No job file found for type '{job_prefix}'.", is_critical=True)
            return
        
        print(f"Found job file: {os.path.basename(job_file_path)}")
        
        # 3. Confirm Job: Ask user for Y/N confirmation. Exit if 'N'.
        confirm = input(f"Confirm processing of '{os.path.basename(job_file_path)}' for {job_name} Job (Y/N): ").strip().lower()
        
        if confirm != 'y':
            print("Job cancelled by user.")
            return
        
        # Get effective date from user
        effective_date = get_effective_date_from_user()
        if effective_date is None:
            print("Job cancelled due to invalid date input.")
            return
        
        print(f"Using effective date: {effective_date.strftime('%m/%d/%Y')}")
        print("\nProcessing job...")
        
        # 4. Setup:
        # Connect to DuckDB using db_handler.
        print(f"Connecting to database: {config.DATABASE_PATH}")
        db_connection = db_handler.connect_to_duckdb(config.DATABASE_PATH)
        
        if not db_connection:
            return  # Error already logged as critical
        
        # Create the timestamped output directory using file_handler.
        output_dir = file_handler.create_output_directory(config.OUTPUT_FOLDER)
        if not output_dir:
            return  # Error already logged as critical
        
        print(f"Output directory created: {output_dir}")
        
        # Read the job CSV into a DataFrame.
        job_df = file_handler.read_csv_to_dataframe(job_file_path)
        if job_df is None:
            return  # Error already logged as critical
        
        print(f"Job file loaded: {len(job_df)} rows to process")
        
        # Initialize an empty list for the final output rows.
        output_rows = []
        
        # 5. Route to appropriate processing function based on job type
        print("\nProcessing job...")
        
        if job_prefix == "rate_update":
            output_rows = process_rate_update_job(db_connection, job_df, effective_date)
        elif job_prefix == "new_tax":
            output_rows = process_new_tax_job(db_connection, job_df, effective_date)
        else:
            logger.log_error(f"Unsupported job type: {job_prefix}", is_critical=True)
            return
        
        print(f"\nProcessing complete. Generated {len(output_rows)} output rows.")
        
        # 6. Finalize:
        if output_rows:
            # Convert the list of output rows into a pandas DataFrame
            output_df = pd.DataFrame(output_rows)
            
            # Write it to CSV using file_handler
            output_file_path = os.path.join(output_dir, f"{job_prefix}_output.csv")
            file_handler.write_dataframe_to_csv(output_file_path, output_df, config.DETAIL_TABLE_SCHEMA)
            print(f"Output saved to: {output_file_path}")
        
        # If any logs were generated, write them to errors.json
        if logger.get_logs():
            errors_file_path = os.path.join(output_dir, "errors.json")
            structured_logs = logger.get_structured_logs(len(job_df))
            file_handler.write_structured_logs_to_json(errors_file_path, structured_logs)
            print(f"Errors/warnings saved to: {errors_file_path}")
        
        # 7. Report to User: Print a summary of the job completion
        print_summary(output_dir, len(job_df), len(output_rows), 
                     logger.count_rows_with_warnings(), logger.count_rows_with_errors(),
                     logger.count_warnings(), logger.count_errors(), effective_date)
        
    except SystemExit as e:
        # This is raised by log_error(is_critical=True)
        print(f"\nA critical error occurred: {e}")
        # Save any logs that were generated before the exit
        if logger.get_logs():
            try:
                emergency_output_dir = file_handler.create_output_directory(config.OUTPUT_FOLDER)
                if emergency_output_dir:
                    errors_file_path = os.path.join(emergency_output_dir, "errors.json")
                    structured_logs = logger.get_structured_logs(0)  # Unknown total rows at this point
                    file_handler.write_structured_logs_to_json(errors_file_path, structured_logs)
                    print(f"Error log saved to: {errors_file_path}")
            except Exception:
                pass  # Don't let error saving fail the error handling
    
    except Exception as e:
        logger.log_error(f"Unexpected error in main application: {str(e)}", {"error": str(e)})
        print(f"\nAn unexpected error occurred: {e}")
    
    finally:
        # Any cleanup code, like closing the DB connection, goes here
        if db_connection:
            try:
                db_connection.close()
            except Exception:
                pass  # Don't let connection close errors fail the cleanup
        
        print("\nScript finished.")

def print_summary(output_path: str, processed_rows: int, added_rows: int, 
                 rows_with_warnings: int, rows_with_errors: int,
                 total_warnings: int, total_errors: int, effective_date: datetime.datetime):
    """Print a summary of the job execution."""
    print("\n" + "=" * 50)
    print("JOB COMPLETE")
    print("=" * 50)
    print(f"Output Path: {output_path}")
    print(f"- {processed_rows} rows processed from the job file.")
    print(f"- {added_rows} new rows added to output CSV.")
    print(f"- Effective date applied: {effective_date.strftime('%m/%d/%Y')}")
    
    # Calculate percentages
    if processed_rows > 0:
        warning_percentage = (rows_with_warnings / processed_rows) * 100
        error_percentage = (rows_with_errors / processed_rows) * 100
        
        print(f"- {rows_with_warnings} rows with warnings ({warning_percentage:.1f}%)")
        print(f"- {rows_with_errors} rows with errors ({error_percentage:.1f}%)")
        
        if total_warnings > 0 or total_errors > 0:
            print(f"- Total: {total_warnings} warnings, {total_errors} errors")
    else:
        print(f"- {total_warnings} warnings logged.")
        print(f"- {total_errors} errors encountered.")
    
    print("=" * 50)

if __name__ == "__main__":
    run() 