# src/file_handler.py
import os
import re
import json
import datetime
import pandas as pd
from src.logger import log_error

def find_latest_job_file(folder: str, prefix: str) -> str | None:
    """
    Find files like 'rate_update_250627.csv'.
    Return the full path of the one with the most recent date in its name.
    Use regex to safely extract the date part.
    """
    try:
        if not os.path.exists(folder):
            log_error(f"Job folder '{folder}' does not exist.", is_critical=True)
            return None
        
        # Pattern to match files like 'rate_update_250627.csv'
        pattern = rf"{prefix}_(\d{{6}})\.csv$"
        
        matching_files = []
        for filename in os.listdir(folder):
            match = re.search(pattern, filename)
            if match:
                date_str = match.group(1)
                full_path = os.path.join(folder, filename)
                matching_files.append((date_str, full_path))
        
        if not matching_files:
            return None
        
        # Sort by date string (YYMMDD format sorts correctly as strings)
        matching_files.sort(key=lambda x: x[0], reverse=True)
        
        # Return the path of the most recent file
        return matching_files[0][1]
        
    except Exception as e:
        log_error(f"Error finding job file: {str(e)}", is_critical=True)
        return None

def read_csv_to_dataframe(file_path: str) -> pd.DataFrame | None:
    """
    Read the CSV into a pandas DataFrame.
    Handle potential file not found or read errors.
    """
    try:
        if not os.path.exists(file_path):
            log_error(f"Job file '{file_path}' does not exist.", is_critical=True)
            return None
        
        df = pd.read_csv(file_path)
        
        # Convert empty strings to None for proper null handling
        df = df.replace('', None)
        
        return df
        
    except Exception as e:
        log_error(f"Error reading CSV file '{file_path}': {str(e)}", is_critical=True)
        return None

def create_output_directory(base_folder: str) -> str:
    """
    Create a timestamped subfolder (e.g., '250627-115530_job').
    Return the path to this new directory.
    """
    try:
        # Create timestamp in YYMMDD-HHMMSS format
        now = datetime.datetime.now()
        timestamp = now.strftime("%y%m%d-%H%M%S")
        
        output_dir = os.path.join(base_folder, f"{timestamp}_job")
        
        os.makedirs(output_dir, exist_ok=True)
        
        return output_dir
        
    except Exception as e:
        log_error(f"Error creating output directory: {str(e)}", is_critical=True)
        return None

def write_dataframe_to_csv(path: str, df: pd.DataFrame, columns: list):
    """
    Write the pandas DataFrame to a CSV file.
    Ensure the columns are in the exact order specified by `columns`.
    """
    try:
        # Ensure all required columns exist in the DataFrame
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            log_error(f"Missing columns in output data: {missing_columns}", is_critical=True)
            return
        
        # Reorder columns to match the schema
        df_ordered = df[columns]
        
        # Write to CSV
        df_ordered.to_csv(path, index=False)
        
    except Exception as e:
        log_error(f"Error writing CSV file '{path}': {str(e)}", is_critical=True)

def write_logs_to_json(path: str, logs: list):
    """
    Write the list of log dictionaries to a JSON file.
    """
    try:
        with open(path, 'w') as f:
            json.dump(logs, f, indent=2)
            
    except Exception as e:
        log_error(f"Error writing logs to JSON file '{path}': {str(e)}")

def write_structured_logs_to_json(path: str, structured_logs: dict):
    """
    Write the structured log dictionary to a JSON file.
    """
    try:
        with open(path, 'w') as f:
            json.dump(structured_logs, f, indent=2)
            
    except Exception as e:
        log_error(f"Error writing structured logs to JSON file '{path}': {str(e)}") 