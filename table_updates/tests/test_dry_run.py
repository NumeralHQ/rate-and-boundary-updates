"""
Test dry run functionality for the table updater
"""

import pytest
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock
import sys
from io import StringIO

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from table_updates.table_updater import TableUpdater


class TestDryRun:
    """Test class for dry run functionality"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def sample_filtering_criteria(self):
        """Sample filtering criteria for testing"""
        return {
            "detail": {
                "filter_fields": ["geocode", "tax_type", "tax_cat", "tax_auth_id", "effective"]
            },
            "product_item": {
                "filter_fields": ["group", "item"]
            },
            "matrix": {
                "filter_fields": ["geocode", "group", "item", "tax_type", "tax_cat", "customer", "provider"]
            }
        }
    
    @pytest.fixture
    def updater_with_temp_dir(self, temp_dir, sample_filtering_criteria):
        """Create TableUpdater instance with temporary directory"""
        updater = TableUpdater()
        updater.table_updates_folder = temp_dir
        updater.filtering_criteria = sample_filtering_criteria
        return updater
    
    def create_sample_csv_files(self, temp_dir):
        """Create sample CSV files for testing"""
        # Create detail append file
        detail_append_content = """geocode,tax_type,tax_cat,tax_auth_id,effective,description,tax_rate,fee
US0800000000,18,FF,12005,7/1/2025,RETAIL DELIVERY FEE,0,0.28
US08001A0017,18,FF,12005,7/1/2025,RETAIL DELIVERY FEE,0,0.28"""
        
        with open(os.path.join(temp_dir, "detail_append_1.csv"), 'w') as f:
            f.write(detail_append_content)
        
        # Create product_item update file
        product_item_content = """group,item,description
7777,000,Technology | Cloud Computing | Software as a Service
7777,001,Technology | Cloud Computing | Software as a Service | Server Inside"""
        
        with open(os.path.join(temp_dir, "product_item_update_1.csv"), 'w') as f:
            f.write(product_item_content)
        
        # Create matrix update file
        matrix_content = """geocode,group,item,tax_type,tax_cat,customer,provider,rate_value
US0800000000,1000,001,04,01,C,P,0.05
US08001A0017,1000,001,04,01,C,P,0.06"""
        
        with open(os.path.join(temp_dir, "matrix_update_1.csv"), 'w') as f:
            f.write(matrix_content)
    
    def test_dry_run_no_database_operations(self, updater_with_temp_dir, temp_dir):
        """Test that dry run doesn't perform any database operations"""
        updater = updater_with_temp_dir
        self.create_sample_csv_files(temp_dir)
        
        # Mock database connection to ensure it's never called
        with patch('duckdb.connect') as mock_connect:
            # Process in dry run mode
            updater.process_csv_files(temp_dir, "dummy.db", dry_run=True)
            
            # Database connection should never be called in dry run
            mock_connect.assert_not_called()
    
    def test_dry_run_validates_filenames(self, updater_with_temp_dir, temp_dir, capsys):
        """Test that dry run validates CSV filenames and shows results"""
        updater = updater_with_temp_dir
        self.create_sample_csv_files(temp_dir)
        
        # Add an invalid filename
        with open(os.path.join(temp_dir, "invalid_filename.csv"), 'w') as f:
            f.write("col1,col2\nval1,val2")
        
        # Process in dry run mode
        updater.process_csv_files(temp_dir, "dummy.db", dry_run=True)
        
        # Capture output
        captured = capsys.readouterr()
        
        # Should show processing information
        assert "Found 4 CSV files to process" in captured.out
        assert "Processing: detail_append_1.csv" in captured.out
        assert "Processing: product_item_update_1.csv" in captured.out
        assert "Processing: matrix_update_1.csv" in captured.out
        assert "Processing: invalid_filename.csv" in captured.out
        
        # Should show dry run messages
        assert "DRY RUN: Would append" in captured.out
        assert "DRY RUN: Would update" in captured.out
        
        # Should skip invalid filename
        assert "SKIPPED: Invalid filename format" in captured.out
    
    def test_dry_run_shows_operation_details(self, updater_with_temp_dir, temp_dir, capsys):
        """Test that dry run shows detailed operation information"""
        updater = updater_with_temp_dir
        self.create_sample_csv_files(temp_dir)
        
        # Process in dry run mode
        updater.process_csv_files(temp_dir, "dummy.db", dry_run=True)
        
        # Capture output
        captured = capsys.readouterr()
        
        # Check for detailed operation information
        assert "Table: detail, Job Type: append" in captured.out
        assert "Table: product_item, Job Type: update" in captured.out
        assert "Table: matrix, Job Type: update" in captured.out
        
        # Check for dry run specific messages
        assert "DRY RUN: Would validate schema for table detail" in captured.out
        assert "DRY RUN: Would append 2 rows to detail" in captured.out
        assert "DRY RUN: Would update product_item using filter fields: ['group', 'item']" in captured.out
        assert "DRY RUN: Would process 2 rows" in captured.out
    
    def test_dry_run_filter_criteria_validation(self, updater_with_temp_dir, temp_dir, capsys):
        """Test that dry run validates filter criteria for update operations"""
        updater = updater_with_temp_dir
        
        # Create CSV for a table not in filtering criteria
        unknown_table_content = "col1,col2\nval1,val2"
        with open(os.path.join(temp_dir, "unknown_table_update_1.csv"), 'w') as f:
            f.write(unknown_table_content)
        
        # Process in dry run mode
        updater.process_csv_files(temp_dir, "dummy.db", dry_run=True)
        
        # Capture output
        captured = capsys.readouterr()
        
        # Should detect missing filter criteria
        assert "SKIPPED: No filtering criteria for table unknown_table" in captured.out
    
    def test_dry_run_no_error_file_creation(self, updater_with_temp_dir, temp_dir):
        """Test that dry run creates error files for validation errors"""
        updater = updater_with_temp_dir
        
        # Create invalid filename that will trigger error logging
        with open(os.path.join(temp_dir, "invalid_format.csv"), 'w') as f:
            f.write("col1,col2\nval1,val2")
        
        # Process in dry run mode
        updater.process_csv_files(temp_dir, "dummy.db", dry_run=True)
        
        # Error file should still be created for filename validation errors
        error_file = os.path.join(temp_dir, "errors.json")
        assert os.path.exists(error_file)
    
    def test_dry_run_row_counting_accuracy(self, updater_with_temp_dir, temp_dir, capsys):
        """Test that dry run accurately counts rows in CSV files"""
        updater = updater_with_temp_dir
        
        # Create CSV with known number of rows
        large_csv_content = "group,item,description\n"
        for i in range(10):  # 10 data rows
            large_csv_content += f"777{i},00{i},Test Item {i}\n"
        
        with open(os.path.join(temp_dir, "product_item_append_1.csv"), 'w') as f:
            f.write(large_csv_content)
        
        # Process in dry run mode
        updater.process_csv_files(temp_dir, "dummy.db", dry_run=True)
        
        # Capture output
        captured = capsys.readouterr()
        
        # Should show correct row count
        assert "DRY RUN: Would append 10 rows to product_item" in captured.out
    
    def test_dry_run_handles_empty_csv_files(self, updater_with_temp_dir, temp_dir, capsys):
        """Test that dry run handles empty CSV files gracefully"""
        updater = updater_with_temp_dir
        
        # Create empty CSV file (only headers)
        empty_csv_content = "group,item,description\n"
        with open(os.path.join(temp_dir, "product_item_append_1.csv"), 'w') as f:
            f.write(empty_csv_content)
        
        # Process in dry run mode
        updater.process_csv_files(temp_dir, "dummy.db", dry_run=True)
        
        # Capture output
        captured = capsys.readouterr()
        
        # Should handle empty file gracefully
        assert "DRY RUN: Would append 0 rows to product_item" in captured.out
    
    @patch('pandas.read_csv')
    def test_dry_run_csv_reading_error(self, mock_read_csv, updater_with_temp_dir, temp_dir, capsys):
        """Test dry run handling of CSV reading errors during row counting"""
        updater = updater_with_temp_dir
        
        # Mock CSV reading to raise an error
        mock_read_csv.side_effect = Exception("CSV parsing error")
        
        # Create a CSV file
        with open(os.path.join(temp_dir, "product_item_append_1.csv"), 'w') as f:
            f.write("group,item,description\ntest,test,test")
        
        # Process in dry run mode
        updater.process_csv_files(temp_dir, "dummy.db", dry_run=True)
        
        # Capture output
        captured = capsys.readouterr()
        
        # Should handle error gracefully and show 0 rows
        assert "DRY RUN: Would append 0 rows to product_item" in captured.out
    
    def test_dry_run_mixed_job_types(self, updater_with_temp_dir, temp_dir, capsys):
        """Test dry run with mixed append and update job types"""
        updater = updater_with_temp_dir
        
        # Create multiple files with different job types
        append_content = "geocode,tax_type,description\nUS123,04,Test Tax"
        update_content = "group,item,description\n7777,001,Updated Item"
        
        with open(os.path.join(temp_dir, "detail_append_1.csv"), 'w') as f:
            f.write(append_content)
        
        with open(os.path.join(temp_dir, "detail_append_2.csv"), 'w') as f:
            f.write(append_content)
        
        with open(os.path.join(temp_dir, "product_item_update_1.csv"), 'w') as f:
            f.write(update_content)
        
        with open(os.path.join(temp_dir, "product_item_update_2.csv"), 'w') as f:
            f.write(update_content)
        
        # Process in dry run mode
        updater.process_csv_files(temp_dir, "dummy.db", dry_run=True)
        
        # Capture output
        captured = capsys.readouterr()
        
        # Should process all files and show appropriate messages
        assert captured.out.count("DRY RUN: Would append") == 2
        assert captured.out.count("DRY RUN: Would update") == 2
        assert "Job Type: append" in captured.out
        assert "Job Type: update" in captured.out
    
    def test_dry_run_preserves_original_files(self, updater_with_temp_dir, temp_dir):
        """Test that dry run doesn't modify original CSV files"""
        updater = updater_with_temp_dir
        
        # Create CSV file and record its content
        original_content = "group,item,description\n7777,001,Original Content"
        csv_file = os.path.join(temp_dir, "product_item_update_1.csv")
        
        with open(csv_file, 'w') as f:
            f.write(original_content)
        
        # Get file modification time
        original_mtime = os.path.getmtime(csv_file)
        
        # Process in dry run mode
        updater.process_csv_files(temp_dir, "dummy.db", dry_run=True)
        
        # File should be unchanged
        with open(csv_file, 'r') as f:
            current_content = f.read()
        
        assert current_content == original_content
        assert os.path.getmtime(csv_file) == original_mtime
    
    def test_dry_run_output_formatting(self, updater_with_temp_dir, temp_dir, capsys):
        """Test that dry run output is properly formatted and informative"""
        updater = updater_with_temp_dir
        self.create_sample_csv_files(temp_dir)
        
        # Process in dry run mode
        updater.process_csv_files(temp_dir, "dummy.db", dry_run=True)
        
        # Capture output
        captured = capsys.readouterr()
        
        # Check output structure and formatting
        lines = captured.out.split('\n')
        
        # Should contain processing headers for each file
        processing_lines = [line for line in lines if line.startswith("Processing:")]
        assert len(processing_lines) == 3  # 3 CSV files
        
        # Should contain table/job type information
        table_lines = [line for line in lines if "Table:" in line and "Job Type:" in line]
        assert len(table_lines) == 3
        
        # Should contain dry run operation messages
        dry_run_lines = [line for line in lines if "DRY RUN:" in line]
        assert len(dry_run_lines) >= 6  # At least 2 per file (schema + operation)


if __name__ == "__main__":
    pytest.main([__file__])