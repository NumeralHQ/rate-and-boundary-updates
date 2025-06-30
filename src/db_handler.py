# src/db_handler.py
import duckdb
import pandas as pd
from src.logger import log_error

def connect_to_duckdb(path: str):
    """
    Connect to the DuckDB database at the given path.
    Handle connection errors and log them as critical.
    """
    try:
        conn = duckdb.connect(path)
        return conn
    except Exception as e:
        log_error(f"Failed to connect to DuckDB at '{path}': {str(e)}", is_critical=True)
        return None

def get_geocodes_from_db(conn, criteria: pd.Series) -> list[str]:
    """
    `criteria` is a row from the job file DataFrame.
    Build a "SELECT geocode FROM geocode" query.
    Dynamically add WHERE clauses for non-empty fields in criteria:
    'geocode', 'state', 'county', 'city'.
    Return a list of unique geocode strings.
    """
    try:
        base_query = "SELECT DISTINCT geocode FROM geocode"
        where_clauses = []
        params = []
        
        # Check each criteria field and add to WHERE clause if not null/empty
        filter_fields = ['geocode', 'state', 'county', 'city']
        
        for field in filter_fields:
            if field in criteria and pd.notna(criteria[field]) and str(criteria[field]).strip():
                where_clauses.append(f"{field} = ?")
                params.append(str(criteria[field]).strip())
        
        if where_clauses:
            query = f"{base_query} WHERE {' AND '.join(where_clauses)}"
        else:
            query = base_query
        
        # Execute query with parameters
        result = conn.execute(query, params).fetchall()
        
        # Extract geocodes from result tuples
        geocodes = [row[0] for row in result if row[0]]
        
        return geocodes
        
    except Exception as e:
        log_error(f"Error querying geocodes from database: {str(e)}")
        return []

def get_detail_rows_from_db(conn, geocodes: list, tax_type: str, tax_cat: str, description: str | None) -> pd.DataFrame:
    """
    Build a "SELECT * FROM detail" query.
    Filter using "WHERE geocode IN (...) AND tax_type = ? AND tax_cat = ?".
    If `description` is not null/empty, add "AND description = ?".
    Use parameterized queries to prevent SQL injection.
    Return a pandas DataFrame of the results.
    """
    try:
        if not geocodes:
            return pd.DataFrame()
        
        # Build the base query
        base_query = "SELECT * FROM detail"
        
        # Build the WHERE clause for geocodes, tax_type, and tax_cat
        geocode_placeholders = ','.join(['?' for _ in geocodes])
        where_clause = f"geocode IN ({geocode_placeholders}) AND tax_type = ? AND tax_cat = ?"
        
        params = geocodes + [tax_type, tax_cat]
        
        # Add description filter if provided
        if description and pd.notna(description) and str(description).strip():
            where_clause += " AND description = ?"
            params.append(str(description).strip())
        
        query = f"{base_query} WHERE {where_clause}"
        
        # Execute query and return as DataFrame
        result = conn.execute(query, params).fetchdf()
        
        return result
        
    except Exception as e:
        log_error(f"Error querying detail rows from database: {str(e)}")
        return pd.DataFrame()

def get_geocodes_for_new_tax(conn, criteria: pd.Series) -> list[str]:
    """
    Enhanced geocode lookup for new tax job type.
    Handles:
    - Comma-separated geocodes in 'geocode' field
    - tax_district field filtering
    - Dynamic criteria (state, county, city, tax_district)
    """
    try:
        geocodes = []
        
        # 1. Handle direct geocode list from CSV
        if pd.notna(criteria.get('geocode')) and str(criteria.get('geocode')).strip():
            input_geocodes = [gc.strip() for gc in str(criteria['geocode']).split(',')]
            if input_geocodes:
                # Validate these geocodes exist in geocode table
                placeholders = ','.join(['?' for _ in input_geocodes])
                query = f"SELECT DISTINCT geocode FROM geocode WHERE geocode IN ({placeholders})"
                result = conn.execute(query, input_geocodes).fetchall()
                geocodes.extend([row[0] for row in result])
        
        # 2. Handle criteria-based lookup (state, county, city, tax_district)
        filter_fields = ['state', 'county', 'city', 'tax_district']
        where_clauses = []
        params = []
        
        for field in filter_fields:
            if field in criteria and pd.notna(criteria[field]) and str(criteria[field]).strip():
                where_clauses.append(f"{field} = ?")
                params.append(str(criteria[field]).strip())
        
        if where_clauses:
            query = f"SELECT DISTINCT geocode FROM geocode WHERE {' AND '.join(where_clauses)}"
            result = conn.execute(query, params).fetchall()
            geocodes.extend([row[0] for row in result])
        
        # Remove duplicates and return
        return list(set(geocodes))
        
    except Exception as e:
        log_error(f"Error querying geocodes for new tax from database: {str(e)}")
        return [] 