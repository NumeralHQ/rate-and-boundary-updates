"""
Test error handling functionality for the table updater
"""

import pytest
import os
import json
import tempfile
import shutil
from unittest.mock import patch, MagicMock
import sys
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from table_updates.table_updater import TableUpdater


class TestErrorHandling:
    """Test class for error handling scenarios"""
    
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
    
    def test_invalid_csv_filename_format(self, updater_with_temp_dir, temp_dir):
        """Test error handling for invalid CSV filename formats"""
        updater = updater_with_temp_dir
        
        # Test various invalid filename formats
        invalid_filenames = [
            "invalid_file.csv",
            "missing_jobtype_1.csv", 
            "table_name_invalid_type_1.csv",
            "table_name_update.csv",
            "table_name_update_abc.csv"
        ]
        
        for filename in invalid_filenames:
            with pytest.raises(ValueError, match="Invalid filename format"):
                updater.parse_csv_filename(filename)
    
    def test_valid_csv_filename_parsing(self, updater_with_temp_dir):
        """Test successful parsing of valid CSV filenames"""
        updater = updater_with_temp_dir
        
        # Test valid filename formats
        test_cases = [
            ("detail_append_1.csv", ("detail", "append", "1")),
            ("product_item_update_2.csv", ("product_item", "update", "2")),
            ("matrix_append_999.csv", ("matrix", "append", "999")),
            ("some_table_name_update_0.csv", ("some_table_name", "update", "0"))
        ]
        
        for filename, expected in test_cases:
            result = updater.parse_csv_filename(filename)
            assert result == expected
    
    def test_missing_filtering_criteria(self, updater_with_temp_dir, temp_dir):
        """Test error when filtering criteria is missing for update job"""
        updater = updater_with_temp_dir
        
        # Create a CSV file for a table not in filtering criteria
        csv_content = "col1,col2\nval1,val2\n"
        csv_path = os.path.join(temp_dir, "unknown_table_update_1.csv")
        with open(csv_path, 'w') as f:
            f.write(csv_content)
        
        # Mock database path (won't be used due to missing criteria)
        db_path = "dummy.db"
        
        # Process should log error and continue
        updater.process_csv_files(temp_dir, db_path, dry_run=False)
        
        # Check error was logged
        error_file = os.path.join(temp_dir, "errors.json")
        assert os.path.exists(error_file)
        
        with open(error_file, 'r') as f:
            error_data = json.load(f)
        
        assert error_data["total_errors"] == 1
        assert "No filtering criteria found for table: unknown_table" in error_data["errors"][0]["error"]
    
    @patch('duckdb.connect')
    def test_schema_validation_failure(self, mock_connect, updater_with_temp_dir, temp_dir):
        """Test schema validation failure handling"""
        updater = updater_with_temp_dir
        
        # Mock database connection and table schema
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = [
            ('group',), ('item',), ('description',)  # Table has these columns
        ]
        
        # Create CSV with extra invalid column
        csv_content = "group,item,description,invalid_column\n7777,001,Test Item,Invalid Value\n"
        csv_path = os.path.join(temp_dir, "product_item_update_1.csv")
        with open(csv_path, 'w') as f:
            f.write(csv_content)
        
        db_path = "dummy.db"
        
        # Process should detect schema mismatch
        updater.process_csv_files(temp_dir, db_path, dry_run=False)
        
        # Check error was logged
        error_file = os.path.join(temp_dir, "errors.json")
        assert os.path.exists(error_file)
        
        with open(error_file, 'r') as f:
            error_data = json.load(f)
        
        assert error_data["total_errors"] == 1
        assert "CSV schema validation failed" in error_data["errors"][0]["error"]
        assert "invalid_column" in str(error_data["errors"][0]["missing_fields"])
    
    @patch('duckdb.connect')
    def test_multiple_matching_records_error(self, mock_connect, updater_with_temp_dir, temp_dir):
        """Test error handling when multiple records match filter criteria"""
        updater = updater_with_temp_dir
        
        # Mock database connection
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Mock schema validation and count queries
        def execute_side_effect(query, params=None):
            mock_result = MagicMock()
            if "DESCRIBE" in query:
                # Return proper schema format: (column_name, column_type, null, key, default, extra)
                mock_result.fetchall.return_value = [
                    ('group', 'VARCHAR', None, None, None, None),
                    ('item', 'VARCHAR', None, None, None, None),
                    ('description', 'VARCHAR', None, None, None, None)
                ]
            elif "SELECT COUNT(*)" in query:
                mock_result.fetchone.return_value = (2,)  # Return 2 matches
            return mock_result
        
        mock_conn.execute.side_effect = execute_side_effect
        
        # Create valid CSV
        csv_content = "group,item,description\n7777,001,Test Item\n"
        csv_path = os.path.join(temp_dir, "product_item_update_1.csv")
        with open(csv_path, 'w') as f:
            f.write(csv_content)
        
        db_path = "dummy.db"
        
        # Process should detect multiple matches
        updater.process_csv_files(temp_dir, db_path, dry_run=False)
        
        # Check error was logged
        error_file = os.path.join(temp_dir, "errors.json")
        assert os.path.exists(error_file)
        
        with open(error_file, 'r') as f:
            error_data = json.load(f)
        
        assert error_data["total_errors"] == 1
        assert "Multiple matching records found" in error_data["errors"][0]["error"]
        assert error_data["errors"][0]["match_count"] == 2
    
    def test_no_csv_files_in_folder(self, updater_with_temp_dir, temp_dir):
        """Test handling when no CSV files exist in job folder"""
        updater = updater_with_temp_dir
        
        # Create some non-CSV files
        with open(os.path.join(temp_dir, "not_a_csv.txt"), 'w') as f:
            f.write("This is not a CSV file")
        
        # Should handle gracefully without errors
        updater.process_csv_files(temp_dir, "dummy.db", dry_run=True)
        
        # No error file should be created
        error_file = os.path.join(temp_dir, "errors.json")
        assert not os.path.exists(error_file)
    
    def test_nonexistent_job_folder(self, updater_with_temp_dir):
        """Test error handling for non-existent job folder"""
        updater = updater_with_temp_dir
        
        with pytest.raises(ValueError, match="Job folder not found"):
            updater.process_csv_files("/nonexistent/folder", "dummy.db", dry_run=True)
    
    def test_find_latest_update_folder_no_folders(self, temp_dir):
        """Test error when no valid update folders exist"""
        updater = TableUpdater()
        updater.table_updates_folder = temp_dir
        
        # Create some invalid folders
        os.mkdir(os.path.join(temp_dir, "invalid_folder"))
        os.mkdir(os.path.join(temp_dir, "not_update_folder"))
        
        with pytest.raises(ValueError, match="No valid update folders found"):
            updater.find_latest_update_folder()
    
    def test_find_latest_update_folder_success(self, temp_dir):
        """Test successful finding of latest update folder"""
        updater = TableUpdater()
        updater.table_updates_folder = temp_dir
        
        # Create valid update folders
        os.mkdir(os.path.join(temp_dir, "250101_update"))
        os.mkdir(os.path.join(temp_dir, "250102_update"))
        os.mkdir(os.path.join(temp_dir, "250103_update"))
        
        # Should return the latest one
        result = updater.find_latest_update_folder()
        expected = os.path.join(temp_dir, "250103_update")
        assert result == expected
    
    @patch('duckdb.connect')
    def test_database_connection_error(self, mock_connect, updater_with_temp_dir, temp_dir):
        """Test handling of database connection errors"""
        updater = updater_with_temp_dir
        
        # Mock connection failure
        mock_connect.side_effect = Exception("Database connection failed")
        
        # Create valid CSV
        csv_content = "group,item,description\n7777,001,Test Item\n"
        csv_path = os.path.join(temp_dir, "product_item_update_1.csv")
        with open(csv_path, 'w') as f:
            f.write(csv_content)
        
        db_path = "dummy.db"
        
        # Process should handle connection error
        updater.process_csv_files(temp_dir, db_path, dry_run=False)
        
        # Check error was logged
        error_file = os.path.join(temp_dir, "errors.json")
        assert os.path.exists(error_file)
        
        with open(error_file, 'r') as f:
            error_data = json.load(f)
        
        assert error_data["total_errors"] >= 1
        # Should have either schema validation error or processing error
        error_messages = [error["error"] for error in error_data["errors"]]
        assert any("Database connection failed" in msg or "Failed to get table schema" in msg 
                  for msg in error_messages)
    
    def test_error_log_file_creation_and_updates(self, updater_with_temp_dir, temp_dir):
        """Test that error log file is created and updated properly"""
        updater = updater_with_temp_dir
        
        # Log first error
        error1 = {"file": "test1.csv", "error": "First error"}
        updater.log_error(error1, temp_dir)
        
        # Check file was created
        error_file = os.path.join(temp_dir, "errors.json")
        assert os.path.exists(error_file)
        
        with open(error_file, 'r') as f:
            data = json.load(f)
        
        assert data["total_errors"] == 1
        assert len(data["errors"]) == 1
        assert data["errors"][0]["error"] == "First error"
        
        # Log second error
        error2 = {"file": "test2.csv", "error": "Second error"}
        updater.log_error(error2, temp_dir)
        
        # Check file was updated
        with open(error_file, 'r') as f:
            data = json.load(f)
        
        assert data["total_errors"] == 2
        assert len(data["errors"]) == 2
        assert data["errors"][1]["error"] == "Second error"
    
    @patch('pandas.read_csv')
    @patch('duckdb.connect')
    def test_csv_parsing_error(self, mock_connect, mock_read_csv, updater_with_temp_dir, temp_dir):
        """Test handling of CSV parsing errors"""
        updater = updater_with_temp_dir
        
        # Mock database connection to succeed
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [('col1',), ('col2',)]
        mock_connect.return_value = mock_conn
        
        # Mock CSV parsing failure - this should happen first
        mock_read_csv.side_effect = pd.errors.EmptyDataError("No columns to parse from file")
        
        csv_path = os.path.join(temp_dir, "empty_file_update_1.csv")
        with open(csv_path, 'w') as f:
            f.write("")  # Empty file
        
        # Should return False for schema validation
        result = updater.validate_csv_schema(csv_path, "test_table", "dummy.db")
        assert result is False
        
        # Check error was logged
        error_file = os.path.join(temp_dir, "errors.json")
        assert os.path.exists(error_file)
        
        with open(error_file, 'r') as f:
            error_data = json.load(f)
        
        assert error_data["total_errors"] == 1
        assert "Schema validation error" in error_data["errors"][0]["error"]


if __name__ == "__main__":
    pytest.main([__file__])