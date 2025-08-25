#!/usr/bin/env python3
"""
Test suite for iceberg_demo_basic.py
Tests the basic command-line demo functionality.
"""

import unittest
import tempfile
import os
import shutil
import sys
from unittest.mock import patch, MagicMock
from pathlib import Path

# Add the parent directory to sys.path to import our demo module
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import the functions we want to test
from iceberg_demo_basic import (
    cleanup_warehouse,
    setup_iceberg_environment,
    insert_initial_data,
    perform_upsert_operation,
    perform_delete_operation
)


class TestBasicDemo(unittest.TestCase):
    """Test suite for the basic Iceberg demo"""
    
    def setUp(self):
        """Set up test environment"""
        self.test_dir = tempfile.mkdtemp(prefix="iceberg_basic_test_")
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)
        
    def tearDown(self):
        """Clean up test environment"""
        os.chdir(self.original_cwd)
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_cleanup_warehouse(self):
        """Test warehouse cleanup functionality"""
        # Create a fake warehouse directory
        warehouse_path = "local_warehouse"
        os.makedirs(warehouse_path, exist_ok=True)
        
        # Create some files in it
        with open(os.path.join(warehouse_path, "test_file.txt"), "w") as f:
            f.write("test")
        
        # Verify it exists
        self.assertTrue(os.path.exists(warehouse_path))
        
        # Run cleanup
        cleanup_warehouse()
        
        # Verify it's gone
        self.assertFalse(os.path.exists(warehouse_path))
    
    @patch('builtins.input', return_value='')  # Mock user input
    @patch('iceberg_demo_basic.interactive_prompt')  # Mock the interactive prompt
    def test_setup_iceberg_environment(self, mock_prompt):
        """Test Iceberg environment setup"""
        try:
            catalog, table = setup_iceberg_environment()
            
            # Verify catalog was created
            self.assertIsNotNone(catalog)
            self.assertIsNotNone(table)
            
            # Verify warehouse directory was created
            self.assertTrue(os.path.exists("local_warehouse"))
            
            # Verify table schema
            schema = table.schema()
            self.assertEqual(len(schema.fields), 3)
            
            # Check field names and types
            field_names = [field.name for field in schema.fields]
            self.assertIn("id", field_names)
            self.assertIn("name", field_names)
            self.assertIn("metadata", field_names)
            
        except Exception as e:
            self.fail(f"Environment setup failed: {e}")
    
    @patch('builtins.input', return_value='')
    @patch('iceberg_demo_basic.interactive_prompt')
    def test_full_demo_workflow(self, mock_prompt):
        """Test the complete demo workflow"""
        try:
            # Setup
            catalog, table = setup_iceberg_environment()
            
            # Initial insert
            snapshot_id_v1 = insert_initial_data(table)
            self.assertIsNotNone(snapshot_id_v1)
            
            # Verify data was inserted
            df = table.scan().to_pandas()
            self.assertEqual(len(df), 4)
            self.assertIn("Alice", df["name"].values)
            
            # Upsert operation
            snapshot_id_v2 = perform_upsert_operation(table)
            self.assertIsNotNone(snapshot_id_v2)
            self.assertNotEqual(snapshot_id_v1, snapshot_id_v2)
            
            # Verify upsert results
            df = table.scan().to_pandas()
            self.assertEqual(len(df), 4)  # Overwrite operation
            
            # Delete operation
            snapshot_id_v3 = perform_delete_operation(table)
            self.assertIsNotNone(snapshot_id_v3)
            self.assertNotEqual(snapshot_id_v2, snapshot_id_v3)
            
            # Verify delete results
            df = table.scan().to_pandas()
            self.assertEqual(len(df), 3)
            
            # Verify snapshots are different
            self.assertEqual(len({snapshot_id_v1, snapshot_id_v2, snapshot_id_v3}), 3)
            
        except Exception as e:
            self.fail(f"Full workflow test failed: {e}")


if __name__ == "__main__":
    unittest.main()
