# Fee Change Implementation Plan for Rate Update Job

## Overview
Enhance the rate_update job type to support fee changes alongside rate changes. This will add two new required columns (`old_fee` and `new_fee`) and implement validation logic similar to the existing rate change functionality.

## Requirements Analysis

### New CSV Columns
- **old_fee**: Expected current fee value in database (DECIMAL, Required)
- **new_fee**: New fee value to apply (DECIMAL, Required)
- **Format**: Decimal values representing dollar amounts (e.g., 1.25 = $1.25)

### Validation Logic
- Compare CSV `old_fee` with database `fee` field
- If mismatch detected: Add warning/error to status column
- Update database record with `new_fee` value
- Process row regardless of fee mismatch (similar to rate validation)

## Implementation Changes Required

### 1. Source Code Updates

#### `src/main.py` - process_rate_update_job()
**Required Field Validation** (around lines 67-80):
```python
# Add validation for new required fields
if pd.isna(job_row.get('old_fee')):
    logger.log_error(f"Row {row_number}: Missing required field 'old_fee'. Skipping.", 
                    {"row_number": row_number, "row_data": job_row.to_dict()})
    continue

if pd.isna(job_row.get('new_fee')):
    logger.log_error(f"Row {row_number}: Missing required field 'new_fee'. Skipping.", 
                    {"row_number": row_number, "row_data": job_row.to_dict()})
    continue
```

**Fee Validation Logic** (after rate validation, around lines 129-155):
```python
# Fee Validation: Compare job_row['old_fee'] with detail_row['fee']
if pd.notna(job_row.get('old_fee')):
    try:
        csv_old_fee = Decimal(str(job_row['old_fee']))
        db_fee = Decimal(str(detail_row['fee']))
        
        if csv_old_fee != db_fee:
            logger.log_warning(
                f"Row {row_number}: Fee mismatch for geocode {detail_row['geocode']}. "
                f"CSV old_fee: {csv_old_fee}, DB fee: {db_fee}",
                {
                    "row_number": row_number,
                    "geocode": detail_row['geocode'],
                    "csv_old_fee": float(csv_old_fee),
                    "db_fee": float(db_fee)
                }
            )
            status_issues.append("Warning: fee mismatch")
            
    except (ValueError, TypeError) as e:
        logger.log_warning(f"Row {row_number}: Error when comparing fees: {str(e)}", 
                           {"row_number": row_number, "error": str(e)})
        status_issues.append("Warning: failed to compare fees")
```

**Fee Update Logic** (after tax_rate update, around lines 162-173):
```python
# Set 'fee' to job_row['new_fee']
try:
    new_fee_decimal = Decimal(str(job_row['new_fee']))
    
    # Validate fee is non-negative
    if new_fee_decimal < 0:
        logger.log_error(f"Row {row_number}: Fee cannot be negative: {job_row.get('new_fee')}", 
                         {"row_number": row_number, "new_fee": job_row.get('new_fee')})
        status_issues.append("Error: negative fee not allowed")
        continue
    
    new_row['fee'] = float(new_fee_decimal)
except (ValueError, TypeError) as e:
    logger.log_error(f"Row {row_number}: Invalid new_fee value: {job_row.get('new_fee')}", 
                     {"row_number": row_number, "new_fee": job_row.get('new_fee'), "error": str(e)})
    status_issues.append("Error: invalid new_fee")
    continue
```

### 2. Documentation Updates

#### `README.md` - Rate Update Job Table
Add new required columns to the job file format table:

| Column | Description | Type | Required | Default | Example |
|--------|-------------|------|----------|---------|---------|
| old_fee | The expected current fee in DB | DECIMAL | Yes | - | 1.25 |
| new_fee | The new fee to apply | DECIMAL | Yes | - | 1.50 |

#### Status Column Values - Rate Update Job
Add new status messages:
- `Warning: fee mismatch` - The old_fee in job file doesn't match database fee
- `Warning: failed to compare fees` - Error occurred while comparing fees
- `Error: invalid new_fee` - The new_fee value is invalid or malformed
- `Error: negative fee not allowed` - The new_fee value is negative (fees must be >= 0)

### 3. Sample Data Updates

#### `job/rate_update_*.csv` 
Update sample CSV files to include the new columns:
```csv
geocode,state,county,city,description,tax_type,tax_cat,old_rate,new_rate,old_fee,new_fee
,AK,HOONAH-ANGOON,PELICAN,,04,01,4,6,0,0.25
```

## Implementation Steps

1. **Update Source Code**
   - Modify `src/main.py` - process_rate_update_job() function
   - Add required field validation for old_fee and new_fee
   - Add fee validation logic (similar to rate validation)
   - Add fee update logic
   - Update status tracking for fee-related warnings/errors

2. **Update Documentation**
   - Modify `README.md` to reflect new required columns
   - Update status column documentation
   - Add notes about fee format (decimal dollar amounts)

3. **Update Sample Data**
   - Add old_fee and new_fee columns to existing rate_update CSV files
   - Provide realistic example values

4. **Testing**
   - Test with matching fees (should succeed)
   - Test with mismatched fees (should warn but process)
   - Test with invalid fee values (should error and skip)
   - Test with missing fee fields (should error and skip)

## Technical Considerations

### Data Types and Precision
- Use `Decimal` for precise fee calculations (same as rates)
- Store as `float` in output (consistent with existing fields)
- No conversion needed (fees are already in dollar amounts, unlike rates which are percentages)

### Error Handling
- Fee validation failures should be warnings (not errors) to allow processing
- Invalid fee values should be errors that skip the row
- Missing required fee fields should be errors that skip the row
- Fees must be non-negative (>= 0) - validate and error if negative

### Backward Compatibility
- This is a breaking change for rate_update job files
- No backward compatibility needed - existing CSV files are stale and won't be reused
- All rate_update CSV files will be updated with new fee columns

## Requirements Clarified

1. **Error vs Warning**: ✅ Fee mismatches should be warnings (allowing processing to continue) - same as rate mismatches
2. **Backward Compatibility**: ✅ No backward compatibility needed - existing CSV files are stale
3. **Fee Validation**: ✅ Fees must be non-negative (>= 0) - validate and error if negative  
4. **Multiple Issues**: ✅ Both rate and fee warnings should appear in status field when both occur

## Success Criteria

- ✅ Rate update jobs can process both rate and fee changes simultaneously
- ✅ Fee validation works similarly to rate validation
- ✅ Proper error handling for invalid or missing fee data
- ✅ Status column accurately reflects fee-related issues
- ✅ Documentation clearly explains new fee functionality
- ✅ Sample data demonstrates proper usage

## Risk Assessment

**Low Risk**:
- Code changes follow existing patterns
- Similar validation logic already exists for rates

**Medium Risk**:
- Breaking change requires updating all existing CSV files
- Increased complexity in validation logic

**Mitigation**:
- Thorough testing with various fee scenarios
- Clear documentation and examples
- Consider providing migration tools or scripts