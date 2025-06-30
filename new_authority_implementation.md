# New Authority Job Type Implementation Plan

## Overview
Implement a "New Authority" job type that follows the same workflow structure as existing job types while adding new functionality for creating tax authority records. This job type creates sequential tax authority IDs and determines authority levels based on jurisdictional hierarchy.

## 1. Configuration Updates

### 1.1 Update `src/config.py`
- **Job Type Already Added**: User has already added "New Authority" as option 3
- **Add authority hierarchy configuration**:
  ```python
  # Authority hierarchy and formatting rules
  AUTHORITY_HIERARCHY = ['country', 'state', 'county', 'city', 'district']
  
  AUTHORITY_TYPE_MAPPING = {
      'country': '0',
      'state': '1', 
      'county': '2',
      'city': '3',
      'district': '4'
  }
  
  AUTHORITY_NAME_FORMATS = {
      'country': '{country}',
      'state': '{state}, STATE OF',
      'county': '{county}, COUNTY OF', 
      'city': '{city}, CITY OF',
      'district': '{parent}, {district}'  # parent = city or county (lowest non-null)
  }
  
  # Tax authority table schema for output
  TAX_AUTHORITY_SCHEMA = [
      'status', 'tax_auth_id', 'country', 'state', 'authority_name', 'tax_auth_type'
  ]
  ```

## 2. Database Handler Extensions

### 2.1 Extend `src/db_handler.py`
- **Add function to get next tax authority ID**:
  ```python
  def get_next_tax_auth_id(conn) -> int:
      """
      Get the next sequential tax_auth_id by finding the maximum existing ID.
      Returns starting ID for new authority records.
      """
      try:
          query = "SELECT MAX(CAST(tax_auth_id AS INTEGER)) FROM tax_authority"
          result = conn.execute(query).fetchone()
          
          if result and result[0] is not None:
              return int(result[0]) + 1
          else:
              return 1  # Start at 1 if no records exist
              
      except Exception as e:
          log_error(f"Error getting next tax authority ID: {str(e)}", is_critical=True)
          return None
  ```

**Implementation approach:**
- Query tax_authority table for maximum tax_auth_id
- Handle edge case where table is empty
- Return next sequential ID to start processing
- Use CAST to ensure numeric comparison

## 3. File Handler Extensions

### 3.1 Extend `src/file_handler.py`
- **No changes required** - existing functions work for new authority job:
  - `find_latest_job_file()` - works with "new_authority" prefix
  - `read_csv_to_dataframe()` - handles authority CSV schema
  - `create_output_directory()` - creates timestamped directories
  - `write_dataframe_to_csv()` - writes with TAX_AUTHORITY_SCHEMA order
  - `write_structured_logs_to_json()` - handles any error structure

## 4. Main Application Logic Updates

### 4.1 Extend `src/main.py`
- **Add new function `process_new_authority_job()`**:
  - Handle authority level detection
  - Generate sequential tax_auth_ids
  - Apply authority name formatting rules
  - Validate and default field values
  - Track status for each output row

**Key processing differences from other job types:**
- No effective date collection or processing
- No required field validation (all fields optional)
- Sequential ID generation from database
- Authority level detection based on hierarchy
- Custom name formatting logic

**Function signature:**
```python
def process_new_authority_job(db_connection, job_df: pd.DataFrame) -> list:
    """
    Process new authority job with authority level detection and sequential ID assignment.
    Returns list of output rows with status tracking.
    """
```

- **Modify main `run()` function**:
  - Skip effective date collection for new_authority job type
  - Route to authority processing function
  - Use TAX_AUTHORITY_SCHEMA for output file naming

**Implementation approach:**
```python
# In run() function, modify effective date section:
if job_prefix == "new_authority":
    # Skip effective date collection for authority job
    pass
else:
    # Get effective date from user (existing logic)
    effective_date = get_effective_date_from_user()
    # ... existing validation

# In processing section:
if job_prefix == "rate_update":
    output_rows = process_rate_update_job(db_connection, job_df, effective_date)
elif job_prefix == "new_tax":
    output_rows = process_new_tax_job(db_connection, job_df, effective_date)
elif job_prefix == "new_authority":
    output_rows = process_new_authority_job(db_connection, job_df)
```

### 4.2 Authority Processing Logic

**Step-by-step process:**
1. **Get starting tax_auth_id** from database
2. **For each input row:**
   - Detect authority level (lowest provided jurisdiction)
   - Generate sequential tax_auth_id
   - Apply formatting rules for authority_name
   - Set tax_auth_type based on level
   - Validate and default country field
   - Validate state field based on authority level
   - Track warnings for defaults/missing data
   - Set status based on issues encountered

**Authority Level Detection Algorithm:**
```python
def detect_authority_level(row: pd.Series) -> str:
    """
    Detect the lowest level of jurisdiction provided.
    Returns: 'country', 'state', 'county', 'city', or 'district'
    """
    hierarchy = ['district', 'city', 'county', 'state', 'country']
    
    for level in hierarchy:
        if pd.notna(row.get(level)) and str(row.get(level)).strip():
            return level
    
    # Default to country if nothing provided
    return 'country'
```

**Authority Name Generation:**
```python
def generate_authority_name(row: pd.Series, auth_level: str) -> str:
    """
    Generate authority name based on level and formatting rules.
    """
    if auth_level == 'country':
        return str(row.get('country', 'US')).upper()
    elif auth_level == 'state':
        return f"{str(row.get('state')).upper()}, STATE OF"
    elif auth_level == 'county':
        return f"{str(row.get('county')).upper()}, COUNTY OF"
    elif auth_level == 'city':
        return f"{str(row.get('city')).upper()}, CITY OF"
    elif auth_level == 'district':
        # Use city if available, otherwise county as parent
        parent = row.get('city') if pd.notna(row.get('city')) else row.get('county')
        return f"{str(parent).upper()}, {str(row.get('district')).upper()}"
```

## 5. Validation and Error Handling

### 5.1 Field Validation Logic
- **No required fields** - all inputs are optional
- **Country defaulting**: Default to 'US' if not provided, add warning
- **State validation**: Warning if no state provided for non-country authorities
- **Hierarchy validation**: Ensure provided fields make logical sense

### 5.2 Status Tracking
- **"Success"** - Authority created without issues
- **"Warning: defaulted country to US"** - Country field was empty
- **"Warning: missing state for non-country authority"** - State missing for state/county/city/district level
- **Multiple warnings** - Concatenated with line breaks

## 6. Output File Structure

### 6.1 Output CSV Schema
- **File naming**: `new_authority_output.csv`
- **Schema**: Status column first, then tax_authority table columns
- **Column order**: status, tax_auth_id, country, state, authority_name, tax_auth_type

### 6.2 Error Reporting
- **Reuse existing structure** from logger.py
- **Same sorting**: Errors before warnings
- **Same summary metrics**: Row counts and percentages

## 7. Flow Modifications

### 7.1 Skip Effective Date Collection
```python
# Modified effective date section in run() function
effective_date = None
if job_prefix != "new_authority":
    # Get effective date from user
    effective_date = get_effective_date_from_user()
    if effective_date is None:
        print("Job cancelled due to invalid date input.")
        return
    
    print(f"Using effective date: {effective_date.strftime('%m/%d/%Y')}")
```

### 7.2 Processing Function Routing
```python
# Route to appropriate processing function
if job_prefix == "rate_update":
    output_rows = process_rate_update_job(db_connection, job_df, effective_date)
elif job_prefix == "new_tax":
    output_rows = process_new_tax_job(db_connection, job_df, effective_date)
elif job_prefix == "new_authority":
    output_rows = process_new_authority_job(db_connection, job_df)
else:
    logger.log_error(f"Unsupported job type: {job_prefix}", is_critical=True)
    return
```

### 7.3 Output File Configuration
```python
# Use appropriate schema for CSV output
if job_prefix == "new_authority":
    schema = config.TAX_AUTHORITY_SCHEMA
    output_file_path = os.path.join(output_dir, f"{job_prefix}_output.csv")
else:
    schema = config.DETAIL_TABLE_SCHEMA
    output_file_path = os.path.join(output_dir, f"{job_prefix}_output.csv")

file_handler.write_dataframe_to_csv(output_file_path, output_df, schema)
```

## 8. Testing Strategy

### 8.1 Test Cases
- **Single level authorities**: Country only, state only, etc.
- **Multi-level hierarchies**: Country + state + county combinations
- **Missing country**: Verify 'US' default and warning
- **Missing state**: Verify warning for non-country authorities
- **District authorities**: Test parent selection (city vs county)
- **Sequential IDs**: Verify proper ID incrementing
- **Authority name formatting**: Test all formatting rules

### 8.2 Integration Testing
- **Job type selection**: Verify option 3 works correctly
- **No effective date**: Verify skipped date collection
- **File discovery**: Test with new_authority prefix
- **Output structure**: Verify tax_authority schema

## 9. Documentation Updates

### 9.1 README.md Updates
- **Add new job type** to step-by-step instructions
- **Document authority job file format** with field descriptions
- **Add example usage** for new authority job
- **Update features list** to include authority creation

### 9.2 New Authority Job File Format Documentation
```markdown
## Job File Format (new_authority_*.csv)

All fields are optional. Authority level is determined by the lowest level of jurisdiction provided.

| Column | Description | Type | Required | Hierarchy Level |
|--------|-------------|------|----------|-----------------|
| country | Country name | VARCHAR | No | 0 |
| state | State name | VARCHAR | No | 1 |
| county | County name | VARCHAR | No | 2 |
| city | City name | VARCHAR | No | 3 |
| district | District name | VARCHAR | No | 4 |

**Authority Level Detection**: The system determines authority type based on the lowest (most specific) level provided.

**Authority Name Formatting**:
- Country: `{COUNTRY}`
- State: `{STATE}, STATE OF`
- County: `{COUNTY}, COUNTY OF`
- City: `{CITY}, CITY OF`
- District: `{CITY/COUNTY}, {DISTRICT}`
```

## 10. Implementation Order

### Phase 1: Core Infrastructure
1. Update `config.py` with authority constants and schema
2. Add `get_next_tax_auth_id()` function to `db_handler.py`
3. Modify main flow to skip effective date for authority job

### Phase 2: Processing Logic
1. Implement authority level detection logic
2. Implement authority name formatting logic
3. Implement `process_new_authority_job()` function
4. Add routing logic for new authority job type

### Phase 3: Testing and Documentation
1. Create test job files with various authority levels
2. Test sequential ID generation and authority detection
3. Update documentation with new job type information
4. Verify no regression in existing job types

## 11. Risk Mitigation

### 11.1 Backwards Compatibility
- **No changes to existing functions** used by other job types
- **Additive changes only** to shared modules
- **Separate processing path** for authority job type

### 11.2 Data Integrity
- **Sequential ID generation** prevents duplicate IDs
- **Authority level validation** ensures logical hierarchy
- **Field validation** prevents malformed authority records

## 12. Implementation Details

### 12.1 Sequential ID Management
```python
# Get starting ID and increment for each output row
starting_id = db_handler.get_next_tax_auth_id(db_connection)
current_id = starting_id

for index, job_row in job_df.iterrows():
    # ... processing logic ...
    new_row['tax_auth_id'] = str(current_id)
    current_id += 1
    # ... continue processing ...
```

### 12.2 Warning Generation Logic
```python
def validate_authority_fields(row: pd.Series, auth_level: str) -> list:
    """Generate list of warnings for authority record."""
    warnings = []
    
    # Check country default
    if pd.isna(row.get('country')) or not str(row.get('country')).strip():
        warnings.append("Warning: defaulted country to US")
    
    # Check state requirement for non-country authorities
    if auth_level != 'country':
        if pd.isna(row.get('state')) or not str(row.get('state')).strip():
            warnings.append("Warning: missing state for non-country authority")
    
    return warnings
```

This implementation plan maintains consistency with existing functionality while adding the specialized authority creation workflow as specified. 