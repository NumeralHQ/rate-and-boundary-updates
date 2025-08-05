"""
Test complete table update functionality
"""

import pytest
import os
import json
import tempfile
import shutil
from unittest.mock import patch, MagicMock, call
import sys
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from table_updates.table_updater import TableUpdater


class TestCompleteTableUpdate:
    """Test class for complete table update functionality"""
    
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
    
    def create_test_database_mock(self):
        """Create a comprehensive database mock"""
        mock_conn = MagicMock()
        
        # Mock table schemas - format: (column_name, column_type, null, key, default, extra)
        schema_responses = {
            "detail": [
                ('geocode', 'VARCHAR', None, None, None, None),
                ('tax_type', 'VARCHAR', None, None, None, None),
                ('tax_cat', 'VARCHAR', None, None, None, None),
                ('tax_auth_id', 'BIGINT', None, None, None, None),
                ('effective', 'DATE', None, None, None, None),
                ('description', 'VARCHAR', None, None, None, None),
                ('tax_rate', 'DOUBLE', None, None, None, None),
                ('fee', 'DOUBLE', None, None, None, None)
            ],
            "product_item": [
                ('group', 'VARCHAR', None, None, None, None),
                ('item', 'VARCHAR', None, None, None, None),
                ('description', 'VARCHAR', None, None, None, None)
            ],
            "matrix": [
                ('geocode', 'VARCHAR', None, None, None, None),
                ('group', 'VARCHAR', None, None, None, None),
                ('item', 'VARCHAR', None, None, None, None),
                ('tax_type', 'VARCHAR', None, None, None, None),
                ('tax_cat', 'VARCHAR', None, None, None, None),
                ('customer', 'VARCHAR', None, None, None, None),
                ('provider', 'VARCHAR', None, None, None, None),
                ('rate_value', 'DOUBLE', None, None, None, None)
            ]
        }
        
        def execute_side_effect(query, params=None):
            mock_result = MagicMock()
            
            if "DESCRIBE" in query:
                # Extract table name from DESCRIBE query (handle quoted names)
                table_name = query.split("DESCRIBE ")[1].strip().strip('"')
                mock_result.fetchall.return_value = schema_responses.get(table_name, [])
            elif "SELECT COUNT(*)" in query:
                # Mock count results for update operations
                if "geocode = ? AND group = ?" in query:
                    # Matrix table - return 1 for single match
                    mock_result.fetchone.return_value = (1,)
                elif "group = ? AND item = ?" in query:
                    # Product_item table - return 1 for single match by default
                    mock_result.fetchone.return_value = (1,)
                else:
                    # Default to single match
                    mock_result.fetchone.return_value = (1,)
            elif "CREATE TEMP TABLE" in query:
                # Append operation temp table creation
                mock_result.fetchone.return_value = None
            elif "SELECT COUNT(*) FROM temp_" in query:
                # Row count from temp table
                mock_result.fetchone.return_value = (2,)  # 2 rows inserted
            else:
                # Other operations (INSERT, UPDATE, etc.)
                mock_result.fetchone.return_value = None
                mock_result.fetchall.return_value = []
            
            return mock_result
        
        mock_conn.execute.side_effect = execute_side_effect
        return mock_conn
    
    @patch('duckdb.connect')
    def test_complete_append_operation(self, mock_connect, updater_with_temp_dir, temp_dir):
        """Test complete append operation workflow"""
        updater = updater_with_temp_dir
        mock_conn = self.create_test_database_mock()
        mock_connect.return_value = mock_conn
        
        # Create append CSV file
        csv_content = """geocode,tax_type,tax_cat,tax_auth_id,effective,description,tax_rate,fee
US0800000000,18,FF,12005,7/1/2025,RETAIL DELIVERY FEE,0,0.28
US08001A0017,18,FF,12005,7/1/2025,RETAIL DELIVERY FEE,0,0.28"""
        
        csv_path = os.path.join(temp_dir, "detail_append_1.csv")
        with open(csv_path, 'w') as f:
            f.write(csv_content)
        
        db_path = "test.db"
        
        # Process the files
        updater.process_csv_files(temp_dir, db_path, dry_run=False)
        
        # Verify database operations were called
        mock_connect.assert_called_with(db_path)
        
        # Verify the append operation SQL calls
        execute_calls = mock_conn.execute.call_args_list
        
        # Should have schema validation, temp table creation, and insert
        assert any("DESCRIBE" in str(call) and "detail" in str(call) for call in execute_calls)
        assert any("INSERT INTO detail" in str(call) for call in execute_calls)
    
    @patch('duckdb.connect')
    def test_complete_update_operation(self, mock_connect, updater_with_temp_dir, temp_dir):
        """Test complete update operation workflow"""
        updater = updater_with_temp_dir
        mock_conn = self.create_test_database_mock()
        mock_connect.return_value = mock_conn
        
        # Create update CSV file
        csv_content = """group,item,description
7777,001,Updated Technology Item
7777,002,New Technology Item"""
        
        csv_path = os.path.join(temp_dir, "product_item_update_1.csv")
        with open(csv_path, 'w') as f:
            f.write(csv_content)
        
        db_path = "test.db"
        
        # Process the files
        updater.process_csv_files(temp_dir, db_path, dry_run=False)
        
        # Verify database operations
        execute_calls = mock_conn.execute.call_args_list
        
        # Should have schema validation and count checks
        assert any("DESCRIBE" in str(call) and "product_item" in str(call) for call in execute_calls)
        assert any("SELECT COUNT(*) as count FROM product_item WHERE" in str(call) for call in execute_calls)
    
    @patch('duckdb.connect')
    def test_mixed_operations_workflow(self, mock_connect, updater_with_temp_dir, temp_dir, capsys):
        """Test workflow with both append and update operations"""
        updater = updater_with_temp_dir
        mock_conn = self.create_test_database_mock()
        mock_connect.return_value = mock_conn
        
        # Create multiple CSV files
        append_content = """geocode,tax_type,tax_cat,description
US123,04,01,Test Tax Entry"""
        
        update_content = """group,item,description
7777,001,Updated Item"""
        
        with open(os.path.join(temp_dir, "detail_append_1.csv"), 'w') as f:
            f.write(append_content)
        
        with open(os.path.join(temp_dir, "product_item_update_1.csv"), 'w') as f:
            f.write(update_content)
        
        db_path = "test.db"
        
        # Process the files
        updater.process_csv_files(temp_dir, db_path, dry_run=False)
        
        # Capture output
        captured = capsys.readouterr()
        
        # Should show processing for both files
        assert "Processing: detail_append_1.csv" in captured.out
        assert "Processing: product_item_update_1.csv" in captured.out
        assert "Job Type: append" in captured.out
        assert "Job Type: update" in captured.out
    
    @patch('shutil.copy2')
    @patch('os.path.exists')
    @patch('os.remove')
    def test_database_duplication(self, mock_remove, mock_exists, mock_copy, updater_with_temp_dir, temp_dir):
        """Test database duplication functionality"""
        updater = updater_with_temp_dir
        
        source_db = "/path/to/source.db"
        timestamp = "250801"
        
        # Mock that source file exists, target doesn't exist initially
        def exists_side_effect(path):
            if path == source_db:
                return True
            return False
        mock_exists.side_effect = exists_side_effect
        
        # Test successful duplication
        result = updater.duplicate_database(source_db, temp_dir, timestamp)
        
        expected_target = os.path.join(temp_dir, f"tax_db_{timestamp}.duckdb")
        assert result == expected_target
        
        # Verify copy was called with correct parameters
        mock_copy.assert_called_once_with(source_db, expected_target)
        # Remove should not be called since target doesn't exist
        mock_remove.assert_not_called()
    
    def test_database_duplication_missing_source(self, updater_with_temp_dir, temp_dir):
        """Test error handling when source database doesn't exist"""
        updater = updater_with_temp_dir
        
        nonexistent_db = "/nonexistent/database.db"
        timestamp = "250801"
        
        # Should raise FileNotFoundError
        with pytest.raises(FileNotFoundError, match="Source database not found"):
            updater.duplicate_database(nonexistent_db, temp_dir, timestamp)
    
    @patch('duckdb.connect')
    def test_schema_validation_success(self, mock_connect, updater_with_temp_dir, temp_dir):
        """Test successful schema validation"""
        updater = updater_with_temp_dir
        mock_conn = self.create_test_database_mock()
        mock_connect.return_value = mock_conn
        
        # Create CSV with valid schema
        csv_content = "group,item,description\n7777,001,Test Item"
        csv_path = os.path.join(temp_dir, "test.csv")
        with open(csv_path, 'w') as f:
            f.write(csv_content)
        
        # Should return True for valid schema
        result = updater.validate_csv_schema(csv_path, "product_item", "test.db")
        assert result is True
    
    @patch('duckdb.connect')
    def test_update_operation_single_match(self, mock_connect, updater_with_temp_dir, temp_dir):
        """Test update operation with single matching record"""
        updater = updater_with_temp_dir
        mock_conn = self.create_test_database_mock()
        mock_connect.return_value = mock_conn
        
        # Create update CSV
        csv_content = "group,item,description\n7777,001,Updated Description"
        csv_path = os.path.join(temp_dir, "product_item_update_1.csv")
        with open(csv_path, 'w') as f:
            f.write(csv_content)
        
        db_path = "test.db"
        filter_fields = ["group", "item"]
        
        # Process update
        updater.process_update_job(csv_path, "product_item", db_path, filter_fields)
        
        # Should perform update operation
        execute_calls = mock_conn.execute.call_args_list
        assert any("UPDATE product_item SET" in str(call) for call in execute_calls)
    
    @patch('duckdb.connect')
    def test_update_operation_no_match_append(self, mock_connect, updater_with_temp_dir, temp_dir):
        """Test update operation that becomes append when no match found"""
        updater = updater_with_temp_dir
        mock_conn = self.create_test_database_mock()
        
        # Override count query to return 0 (no match)
        def execute_side_effect(query, params=None):
            mock_result = MagicMock()
            if "SELECT COUNT(*)" in query:
                mock_result.fetchone.return_value = (0,)  # No match
            elif "DESCRIBE" in query:
                # Return proper schema format: (column_name, column_type, null, key, default, extra)
                mock_result.fetchall.return_value = [
                    ('group', 'VARCHAR', None, None, None, None),
                    ('item', 'VARCHAR', None, None, None, None),
                    ('description', 'VARCHAR', None, None, None, None)
                ]
            else:
                mock_result.fetchone.return_value = None
            return mock_result
        
        mock_conn.execute.side_effect = execute_side_effect
        mock_connect.return_value = mock_conn
        
        # Create update CSV
        csv_content = "group,item,description\n9999,999,New Item"
        csv_path = os.path.join(temp_dir, "product_item_update_1.csv")
        with open(csv_path, 'w') as f:
            f.write(csv_content)
        
        db_path = "test.db"
        filter_fields = ["group", "item"]
        
        # Process update
        updater.process_update_job(csv_path, "product_item", db_path, filter_fields)
        
        # Should perform insert operation instead
        execute_calls = mock_conn.execute.call_args_list
        assert any("INSERT INTO product_item" in str(call) for call in execute_calls)
    
    @patch('duckdb.connect')
    def test_row_insertion_helper(self, mock_connect, updater_with_temp_dir):
        """Test the _insert_row helper method"""
        updater = updater_with_temp_dir
        mock_conn = MagicMock()
        
        # Create a sample row
        row_data = pd.Series({
            'group': '7777',
            'item': '001',
            'description': 'Test Item'
        })
        
        # Test insertion
        updater._insert_row(mock_conn, "product_item", row_data)
        
        # Verify INSERT query was called
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        query = call_args[0][0]
        values = call_args[0][1]
        
        assert "INSERT INTO product_item" in query
        assert '"group","item","description"' in query
        assert values == ['7777', '001', 'Test Item']
    
    @patch('duckdb.connect')
    def test_row_update_helper(self, mock_connect, updater_with_temp_dir):
        """Test the _update_row helper method"""
        updater = updater_with_temp_dir
        mock_conn = MagicMock()
        
        # Create a sample row
        row_data = pd.Series({
            'group': '7777',
            'item': '001',
            'description': 'Updated Item'
        })
        
        where_clause = "group = ? AND item = ?"
        where_params = ['7777', '001']
        
        # Test update
        updater._update_row(mock_conn, "product_item", row_data, where_clause, where_params)
        
        # Verify UPDATE query was called
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        
        assert "UPDATE product_item SET" in query
        assert where_clause in query
        # Params should include SET values + WHERE values
        assert len(params) == 5  # 3 SET values + 2 WHERE values
    
    def test_latest_folder_detection(self, temp_dir):
        """Test detection of latest timestamped folder"""
        updater = TableUpdater()
        updater.table_updates_folder = temp_dir
        
        # Create multiple timestamped folders
        folders = ["250101_update", "250102_update", "250103_update", "250131_update"]
        for folder in folders:
            os.mkdir(os.path.join(temp_dir, folder))
        
        # Should find the latest one
        result = updater.find_latest_update_folder()
        expected = os.path.join(temp_dir, "250131_update")
        assert result == expected
    
    @patch('duckdb.connect')
    def test_processing_with_error_recovery(self, mock_connect, updater_with_temp_dir, temp_dir):
        """Test that processing continues after individual file errors"""
        updater = updater_with_temp_dir
        
        # First connection succeeds, second fails
        mock_conn_good = self.create_test_database_mock()
        mock_conn_bad = MagicMock()
        mock_conn_bad.execute.side_effect = Exception("Database error")
        
        # Provide enough mock connections for schema validation and processing
        mock_connect.side_effect = [mock_conn_good, mock_conn_good, mock_conn_bad, mock_conn_good, mock_conn_good]
        
        # Create multiple CSV files
        good_csv = "group,item,description\n7777,001,Good Item"
        bad_csv = "group,item,description\n8888,002,Bad Item"
        
        with open(os.path.join(temp_dir, "product_item_update_1.csv"), 'w') as f:
            f.write(good_csv)
        
        with open(os.path.join(temp_dir, "product_item_update_2.csv"), 'w') as f:
            f.write(bad_csv)
        
        db_path = "test.db"
        
        # Process should continue despite error in second file
        updater.process_csv_files(temp_dir, db_path, dry_run=False)
        
        # Error should be logged
        error_file = os.path.join(temp_dir, "errors.json")
        assert os.path.exists(error_file)
        
        with open(error_file, 'r') as f:
            error_data = json.load(f)
        
        assert error_data["total_errors"] >= 1
    
    @patch('duckdb.connect')
    def test_chunked_processing_large_file(self, mock_connect, updater_with_temp_dir, temp_dir):
        """Test chunked processing of large CSV files"""
        updater = updater_with_temp_dir
        mock_conn = self.create_test_database_mock()
        mock_connect.return_value = mock_conn
        
        # Create large CSV file (more than chunk size)
        csv_content = "group,item,description\n"
        for i in range(1500):  # More than default chunk size of 1000
            csv_content += f"777{i:04d},{i:03d},Item {i}\n"
        
        csv_path = os.path.join(temp_dir, "product_item_update_1.csv")
        with open(csv_path, 'w') as f:
            f.write(csv_content)
        
        db_path = "test.db"
        filter_fields = ["group", "item"]
        
        # Process should handle large file in chunks
        updater.process_update_job(csv_path, "product_item", db_path, filter_fields)
        
        # Should have made multiple database calls for chunks
        execute_calls = mock_conn.execute.call_args_list
        count_queries = [call for call in execute_calls if "SELECT COUNT(*)" in str(call)]
        
        # Should have processed all 1500 rows
        assert len(count_queries) == 1500
    
    def test_filtering_criteria_loading_success(self, temp_dir):
        """Test successful loading of filtering criteria"""
        # Create valid filtering criteria file
        criteria = {
            "detail": {"filter_fields": ["geocode", "tax_type"]},
            "product_item": {"filter_fields": ["group", "item"]}
        }
        
        criteria_path = os.path.join(temp_dir, "filtering_criteria.json")
        with open(criteria_path, 'w') as f:
            json.dump(criteria, f)
        
        # Create updater
        updater = TableUpdater()
        updater.table_updates_folder = temp_dir
        updater.load_filtering_criteria()
        
        # Should load criteria successfully
        assert updater.filtering_criteria == criteria
    
    def test_filtering_criteria_loading_failure(self, temp_dir):
        """Test error handling when filtering criteria file is missing"""
        updater = TableUpdater()
        updater.table_updates_folder = temp_dir
        
        # Should exit when file is missing
        with pytest.raises(SystemExit):
            updater.load_filtering_criteria()


if __name__ == "__main__":
    pytest.main([__file__])