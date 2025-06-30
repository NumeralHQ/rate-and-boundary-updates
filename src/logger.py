# src/logger.py
import datetime

# This list will be imported and appended to by other modules
LOGS = []

def log_warning(message: str, context: dict = None):
    """Logs a warning message."""
    LOGS.append({
        "level": "WARNING",
        "timestamp": datetime.datetime.now().isoformat(),
        "message": message,
        "context": context or {}
    })

def log_error(message: str, context: dict = None, is_critical: bool = False):
    """Logs an error message. Critical errors halt execution."""
    LOGS.append({
        "level": "ERROR",
        "timestamp": datetime.datetime.now().isoformat(),
        "message": message,
        "context": context or {}
    })
    if is_critical:
        # The main script will handle saving logs and exiting
        raise SystemExit(message)

def get_logs():
    """Returns all collected logs."""
    return LOGS

def get_structured_logs(total_rows_processed: int = 0):
    """Returns logs structured by row number with summary statistics."""
    row_details = {}
    total_warnings = 0
    total_errors = 0
    rows_with_warnings = set()
    rows_with_errors = set()
    
    for log_entry in LOGS:
        level = log_entry['level']
        context = log_entry.get('context', {})
        row_number = context.get('row_number')
        
        if row_number is not None:
            row_key = str(row_number)
            
            if row_key not in row_details:
                row_details[row_key] = {
                    "warnings": [],
                    "errors": []
                }
            
            entry = {
                "timestamp": log_entry['timestamp'],
                "message": log_entry['message'],
                "context": context
            }
            
            if level == 'WARNING':
                row_details[row_key]["warnings"].append(entry)
                rows_with_warnings.add(row_number)
                total_warnings += 1
            elif level == 'ERROR':
                row_details[row_key]["errors"].append(entry)
                rows_with_errors.add(row_number)
                total_errors += 1
    
    # Sort row_details: errors first, then warnings only
    sorted_row_details = {}
    
    # First, add rows with errors (sorted by row number)
    rows_with_errors_list = []
    rows_with_warnings_only = []
    
    for row_key, details in row_details.items():
        if len(details["errors"]) > 0:
            rows_with_errors_list.append((int(row_key), row_key, details))
        elif len(details["warnings"]) > 0:
            rows_with_warnings_only.append((int(row_key), row_key, details))
    
    # Sort by row number within each category
    rows_with_errors_list.sort(key=lambda x: x[0])
    rows_with_warnings_only.sort(key=lambda x: x[0])
    
    # Add to sorted dictionary: errors first, then warnings only
    for _, row_key, details in rows_with_errors_list:
        sorted_row_details[row_key] = details
    
    for _, row_key, details in rows_with_warnings_only:
        sorted_row_details[row_key] = details
    
    return {
        "summary": {
            "total_rows_processed": total_rows_processed,
            "rows_with_warnings": len(rows_with_warnings),
            "rows_with_errors": len(rows_with_errors),
            "total_warnings": total_warnings,
            "total_errors": total_errors
        },
        "row_details": sorted_row_details
    }

def count_warnings():
    return sum(1 for log in LOGS if log['level'] == 'WARNING')

def count_errors():
    return sum(1 for log in LOGS if log['level'] == 'ERROR')

def count_rows_with_warnings():
    """Count unique rows that have warnings."""
    rows_with_warnings = set()
    for log in LOGS:
        if log['level'] == 'WARNING':
            row_number = log.get('context', {}).get('row_number')
            if row_number is not None:
                rows_with_warnings.add(row_number)
    return len(rows_with_warnings)

def count_rows_with_errors():
    """Count unique rows that have errors."""
    rows_with_errors = set()
    for log in LOGS:
        if log['level'] == 'ERROR':
            row_number = log.get('context', {}).get('row_number')
            if row_number is not None:
                rows_with_errors.add(row_number)
    return len(rows_with_errors) 