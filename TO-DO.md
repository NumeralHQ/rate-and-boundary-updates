To-Do List: Phase 1 - Foundation & Rate Update
This document outlines the tasks required to complete Phase 1 of the Tax Data Update Utility project.
Milestone 1: Project Setup & Configuration
[ ] Initialize Repository & Structure
[ ] Initialize git in the project root.
[ ] Create the initial directory structure: /src, /job, /output, /tests.
[ ] Add a .gitignore file (for __pycache__, /output/*, etc.).
[ ] Set up Dependencies
[ ] Create a requirements.txt file.
[ ] Add duckdb and pandas to requirements.txt.
[ ] Create Configuration File
[ ] Create src/config.py to store constants.
[ ] Add DATABASE_PATH, JOB_FOLDER, and OUTPUT_FOLDER variables to config.py.
Milestone 2: Core Modules Development
[ ] Database Handler Module (src/db_handler.py)
[ ] Implement connect_to_duckdb() function with robust try/except error handling.
[ ] Implement get_geocodes_from_db(conn, criteria):
[ ] Build a dynamic WHERE clause based on non-empty values in criteria (geocode, state, county, city).
[ ] Execute the query and return a list of unique geocodes.
[ ] Implement get_detail_rows_from_db(conn, geocodes, tax_type, description):
[ ] Build the query using WHERE geocode IN (...) AND tax_type = ?.
[ ] Conditionally add the AND description = ? clause if description is provided.
[ ] Execute and return results (e.g., as a list of dictionaries or a pandas DataFrame).
[ ] File & I/O Handler Module (src/file_handler.py)
[ ] Implement find_latest_job_file(folder, prefix):
[ ] Scan the directory for files matching the pattern.
[ ] Parse YYMMDD from filenames to find the most recent one.
[ ] Implement read_csv(file_path) using pandas to return a DataFrame.
[ ] Implement create_output_directory(base_folder):
[ ] Generate timestamped folder name (YYMMDD-HHMMSS_job).
[ ] Create the directory and return its path.
[ ] Implement write_csv(path, data, schema_columns).
[ ] Ensure output columns match the detail table schema order.
[ ] Implement write_json(path, data).
[ ] Logging Module (src/logger.py)
[ ] Initialize a global list to store log entries (e.g., LOGS = []).
[ ] Implement log_warning(message, context) function.
[ ] Implement log_error(message, context) function.
[ ] Each function should append a structured dictionary ({'level': '...', 'message': '...'}) to the global list.
Milestone 3: Application Logic (src/main.py)
[ ] Implement CLI User Flow
[ ] Create the initial job selection prompt.
[ ] Add logic to find the job file using the file handler.
[ ] Create the confirmation prompt (Y/N).
[ ] Handle user cancellation gracefully.
[ ] Develop Main Processing Loop
[ ] Read the confirmed job file into a pandas DataFrame.
[ ] Iterate through each row of the DataFrame.
[ ] For each row, call get_geocodes_from_db.
[ ] If geocodes are found, call get_detail_rows_from_db.
[ ] Implement Row Processing & Generation
[ ] Loop through the detail_rows returned from the database.
[ ] Rate Validation: Compare old_rate from CSV with tax_rate from DB. Log a warning on mismatch.
[ ] Data Transformation:
[ ] Convert old_rate and new_rate from percentage to decimal format (divide by 100).
[ ] Get the current timestamp for the effective date.
[ ] Create New Row: Make a copy of the database row and update the effective and tax_rate fields.
[ ] Append the new, updated row to an output list.
[ ] Finalize and Report
[ ] After the loop, call write_csv to save the final output file.
[ ] If the logs list is not empty, call write_json to save the errors.json file.
[ ] Implement print_summary() function that displays the final statistics (rows processed, rows added, warnings, etc.).
[ ] Ensure the database connection is closed at the end.
Milestone 4: Testing and Documentation
[ ] Create Test Assets
[ ] Create a sample rate_update_test.csv file in /job with various cases (all fields populated, some fields empty, rate mismatch).
[ ] (Optional) Create a small, self-contained test version of tax_database.duckdb for repeatable tests.
[ ] Perform Testing
[ ] Conduct manual end-to-end run-throughs to verify the entire process.
[ ] Check that the output CSV has the correct schema and data.
[ ] Check that errors.json correctly logs warnings and errors.
[ ] Finalize Documentation
[ ] Review and update the README.md to reflect the final state of the script and its usage.
