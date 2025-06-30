# New Tax Job Type Implementation Plan

## Overview
Implement a "New Tax" job type that follows the same workflow structure as the existing "Rate Update" job type while adding new functionality for creating tax records in the detail table schema.

## 1. Configuration Updates

### 1.1 Update `src/config.py`
- **Extend `JOB_TYPE_MAPPING`** to include new job type:
  ```python
  JOB_TYPE_MAPPING = {
      "1": {
          "name": "Rate Update",
          "file_prefix": "rate_update"
      },
      "2": {
          "name": "New Tax",
          "file_prefix": "new_tax"
      }
  }
  ```

- **Add default values configuration**:
  ```python
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
  ```

- **Add required fields configuration**:
  ```python
  NEW_TAX_REQUIRED_FIELDS = ['tax_type', 'tax_rate', 'tax_auth_id', 'description']
  ```

## 2. Database Handler Extensions

### 2.1 Extend `src/db_handler.py`
- **Create new function `get_geocodes_for_new_tax()`**:
  - Handle comma-separated geocodes in the geocode field
  - Support `tax_district` field in filtering criteria
  - Extend existing dynamic filtering logic
  - Parse comma-separated geocodes and validate each one exists
  - Return comprehensive list of unique geocodes

**Function signature:**
```python
def get_geocodes_for_new_tax(conn, criteria: pd.Series) -> list[str]:
    """
    Enhanced geocode lookup for new tax job type.
    Handles:
    - Comma-separated geocodes in 'geocode' field
    - tax_district field filtering
    - Dynamic criteria (state, county, city, tax_district)
    """
```

**Implementation approach:**
- Reuse existing `get_geocodes_from_db()` logic for base filtering
- Add preprocessing for comma-separated geocodes
- Add tax_district to filter_fields list
- Combine direct geocode lookups with criteria-based searches
- Remove duplicates from final result

## 3. File Handler Extensions

### 3.1 Extend `src/file_handler.py`
- **No changes required** - existing functions work for new job type:
  - `find_latest_job_file()` - works with any prefix
  - `read_csv_to_dataframe()` - handles any CSV schema
  - `create_output_directory()` - creates timestamped directories
  - `write_dataframe_to_csv()` - writes with correct column order
  - `write_structured_logs_to_json()` - handles any error structure

## 4. Main Application Logic Updates

### 4.1 Extend `src/main.py`
- **Add new function `process_new_tax_job()`**:
  - Handle new tax specific validation and processing
  - Apply default values for missing fields
  - Create output rows for each geocode
  - Maintain same status tracking as rate update

**Key processing differences from rate update:**
- More complex required field validation
- No rate comparison (since creating new records)
- Field defaulting logic
- Multiple output rows per input row (one per geocode)

**Function signature:**
```python
def process_new_tax_job(db_connection, job_df: pd.DataFrame, effective_date: datetime.datetime, output_dir: str) -> list:
    """
    Process new tax job with field defaulting and multiple geocode handling.
    Returns list of output rows with status tracking.
    """
```

- **Modify main `run()` function**:
  - Add job type branching logic after effective date selection
  - Route to appropriate processing function based on job type
  - Maintain same output file naming convention

**Implementation approach:**
```python
# In run() function, after effective date selection:
if job_prefix == "rate_update":
    output_rows = process_rate_update_job(db_connection, job_df, effective_date)
elif job_prefix == "new_tax":
    output_rows = process_new_tax_job(db_connection, job_df, effective_date, output_dir)
```

### 4.2 New Tax Processing Logic

**Step-by-step process:**
1. **Validate required fields** (tax_type, tax_rate, tax_auth_id, description)
2. **Format tax codes** (tax_type, tax_cat with zfill(2))
3. **Get geocodes** using enhanced lookup function
4. **For each geocode found:**
   - Create new detail row
   - Apply job CSV values where provided
   - Apply defaults for missing values
   - Set effective date (job CSV value → user input → today)
   - Set status to "Success" (no rate validation needed)
   - Add to output list
5. **Handle no geocodes found** as error
6. **Return output rows** with status tracking

## 5. Validation and Error Handling

### 5.1 Required Field Validation
- **Reuse existing pattern** from rate update job
- **New validation function**:
```python
def validate_new_tax_required_fields(job_row: pd.Series, row_number: int) -> bool:
    """Validate required fields for new tax job type."""
    for field in config.NEW_TAX_REQUIRED_FIELDS:
        if pd.isna(job_row.get(field)):
            logger.log_error(f"Row {row_number}: Missing required field '{field}'. Skipping.", 
                           {"row_number": row_number, "row_data": job_row.to_dict()})
            return False
    return True
```

### 5.2 Error Scenarios
- **Missing required fields** → Skip row, log error
- **No geocodes found** → Skip row, log error  
- **Invalid tax_rate format** → Add status warning, continue processing
- **Invalid effective date** → Use fallback date, add status warning

## 6. Output File Structure

### 6.1 Output CSV Schema
- **Same as rate update**: Status column first, then all detail table columns
- **File naming**: `new_tax_output.csv`
- **Status values**:
  - `"Success"` - Row created without issues
  - `"Warning: invalid tax_rate"` - Rate format issue but row created
  - `"Warning: invalid effective date"` - Date issue but fallback used

### 6.2 Error Reporting
- **Reuse existing structure** from logger.py
- **Same sorting**: Errors before warnings
- **Same summary metrics**: Row counts and percentages

## 7. Testing Strategy

### 7.1 Unit Testing Approach
- **Test geocode parsing** with comma-separated values
- **Test field defaulting** logic
- **Test validation** for required fields
- **Test output generation** for multiple geocodes per input row

### 7.2 Integration Testing
- **Verify job type selection** works for both types
- **Verify file discovery** works with new_tax prefix
- **Verify output structure** matches existing pattern
- **Verify no regression** in rate update functionality

## 8. Documentation Updates

### 8.1 README.md Updates
- **Add new job type** to features list
- **Document new job file format** with schema and required fields
- **Add example usage** for new tax job type
- **Update job type selection** in how-to-run section

### 8.2 New Tax Job File Format Documentation
```markdown
## Job File Format (new_tax_*.csv)

Required fields: `tax_type`, `tax_rate`, `tax_auth_id`, `description`

| Column | Description | Type | Required | Default |
|--------|-------------|------|----------|---------|
| geocode | Comma-separated geocodes | VARCHAR | No | From lookup |
| state | State abbreviation | VARCHAR | No | - |
| county | County name | VARCHAR | No | - |
| city | City name | VARCHAR | No | - |
| tax_district | Tax district name | VARCHAR | No | - |
| tax_type | Tax type (required) | VARCHAR | Yes | - |
| tax_cat | Tax category | VARCHAR | No | 01 |
| tax_auth_id | Tax authority ID (required) | VARCHAR | Yes | - |
| effective | Effective date | DATE | No | User input/today |
| description | Tax description (required) | VARCHAR | Yes | - |
| ... | (remaining fields) | ... | No | (see defaults) |
```

## 9. Implementation Order

### Phase 1: Core Infrastructure
1. Update `config.py` with new job type and defaults
2. Extend `db_handler.py` with enhanced geocode lookup
3. Add job type routing in `main.py`

### Phase 2: Processing Logic
1. Implement `process_new_tax_job()` function
2. Add field validation and defaulting logic
3. Implement status tracking for new tax job

### Phase 3: Testing and Documentation
1. Create test job files
2. Test both job types to ensure no regression
3. Update documentation
4. Add error handling edge cases

## 10. Risk Mitigation

### 10.1 Backwards Compatibility
- **No changes to existing functions** used by rate update
- **Additive changes only** to shared modules
- **Separate processing paths** for each job type

### 10.2 Code Quality
- **Reuse existing patterns** for consistency
- **Maintain same error handling** approach
- **Follow same logging and output** conventions

## 11. Implementation Details - Clarified

### 11.1 Effective Date Precedence Logic
```python
# Priority order:
# 1. Job CSV effective date (if specified)
# 2. User input effective date (if job CSV blank)
# 3. Today's date (fallback)

if pd.notna(job_row.get('effective')) and str(job_row.get('effective')).strip():
    effective_date = parse_csv_date(job_row['effective'])
else:
    effective_date = user_provided_effective_date  # From user input or today
```

### 11.2 Tax_district Handling
- **tax_district** is treated as another filter field in the geocode table
- No special validation required - if query yields no results, log as error
- Same dynamic filtering approach as existing fields

### 11.3 Comma-separated Geocode Logic
```python
# Parse comma-separated geocodes from input CSV
if pd.notna(job_row.get('geocode')) and str(job_row.get('geocode')).strip():
    input_geocodes = [gc.strip() for gc in str(job_row['geocode']).split(',')]
    # Use: WHERE geocode IN (?, ?, ?) with parameterized query
else:
    input_geocodes = []
```

### 11.4 Default Value Formatting
- **All 2-character fields** automatically padded with leading zeros using `zfill(2)`
- **Applied to**: tax_type, tax_cat, pass_flag, base_type, date_flag, rounding, unit_type, max_type, thresh_type, formula
- **Example**: `'1'` becomes `'01'`, `'9'` becomes `'09'`

### 11.5 Error Processing Approach
- **No error threshold** - process all rows regardless of error count
- **Log all errors** for post-processing review
- **Continue processing** even if many rows fail geocode lookup

## 12. Enhanced Database Query Logic

### 12.1 Geocode Lookup Strategy
```python
def get_geocodes_for_new_tax(conn, criteria: pd.Series) -> list[str]:
    geocodes = []
    
    # 1. Handle direct geocode list from CSV
    if pd.notna(criteria.get('geocode')):
        input_geocodes = [gc.strip() for gc in str(criteria['geocode']).split(',')]
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
```

This implementation plan now includes all clarified requirements and maintains consistency with existing functionality while adding the new tax job type capabilities as specified. 