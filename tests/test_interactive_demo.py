#!/usr/bin/env python3
"""
Test suite for iceberg_demo_interactive.py
Tests the Rich TUI interactive demo functionality.
"""

import unittest
import tempfile
import os
import shutil
import sys
from unittest.mock import patch, MagicMock, Mock
from pathlib import Path
import time

# Add the parent directory to sys.path to import our demo module
sys.path.insert(0, str(Path(__file__).parent.parent))

from iceberg_demo_interactive import IcebergTUI


class TestInteractiveDemo(unittest.TestCase):
    """Test suite for the Interactive Iceberg demo"""
    
    def setUp(self):
        """Set up test environment"""
        self.test_dir = tempfile.mkdtemp(prefix="iceberg_interactive_test_")
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)
        
        # Create TUI instance with test configuration
        self.tui = IcebergTUI()
        self.tui.warehouse_path = os.path.join(self.test_dir, "test_warehouse")
        
    def tearDown(self):
        """Clean up test environment"""
        os.chdir(self.original_cwd)
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_tui_initialization(self):
        """Test TUI initialization"""
        self.assertEqual(self.tui.current_step, 0)
        self.assertEqual(self.tui.total_steps, 7)
        self.assertEqual(len(self.tui.snapshots), 0)
        self.assertIsNone(self.tui.catalog)
        self.assertIsNone(self.tui.table)
    
    def test_progress_panel_creation(self):
        """Test progress panel creation"""
        self.tui.current_step = 3
        panel = self.tui.create_progress_panel()
        
        self.assertIsNotNone(panel)
        self.assertIn("Step 3/7", str(panel))
    
    def test_status_panel_creation(self):
        """Test status panel creation"""
        panel = self.tui.create_status_panel("Test message")
        
        self.assertIsNotNone(panel)
        self.assertIn("Test message", str(panel))
    
    def test_content_panel_creation(self):
        """Test content panel creation"""
        panel = self.tui.create_content_panel("Test Title", "Test content")
        
        self.assertIsNotNone(panel)
        self.assertIn("Test Title", str(panel))
    
    def test_layout_creation(self):
        """Test layout creation"""
        layout = self.tui.create_layout()
        
        self.assertIsNotNone(layout)
        # Check that required layout sections exist
        self.assertIn("main", layout)
        self.assertIn("bottom", layout)
        self.assertIn("content", layout["main"])
        self.assertIn("sidebar", layout["main"])
    
    def test_file_tree_panel_no_warehouse(self):
        """Test file tree panel when warehouse doesn't exist"""
        panel = self.tui.create_file_tree_panel()
        
        self.assertIsNotNone(panel)
        self.assertIn("not yet created", str(panel))
    
    def test_file_tree_panel_with_warehouse(self):
        """Test file tree panel with existing warehouse"""
        # Create a fake warehouse structure
        warehouse_path = self.tui.warehouse_path
        os.makedirs(warehouse_path, exist_ok=True)
        
        # Create some test files
        with open(os.path.join(warehouse_path, "test.parquet"), "w") as f:
            f.write("test")
        
        panel = self.tui.create_file_tree_panel()
        
        self.assertIsNotNone(panel)
        self.assertIn("local_warehouse", str(panel))
    
    @patch('builtins.input', return_value='')  # Mock user input to prevent blocking
    def test_cleanup_warehouse(self, mock_input):
        """Test warehouse cleanup"""
        # Create test warehouse
        os.makedirs(self.tui.warehouse_path, exist_ok=True)
        with open(os.path.join(self.tui.warehouse_path, "test.txt"), "w") as f:
            f.write("test")
        
        self.assertTrue(os.path.exists(self.tui.warehouse_path))
        
        # Run cleanup
        self.tui.cleanup_warehouse()
        
        self.assertFalse(os.path.exists(self.tui.warehouse_path))
    
    @patch('builtins.input', return_value='')
    @patch('rich.live.Live')
    def test_step_1_setup(self, mock_live, mock_input):
        """Test step 1 setup functionality"""
        try:
            self.tui.step_1_setup()
            
            # Verify step was incremented
            self.assertEqual(self.tui.current_step, 1)
            
            # Verify catalog and table were created
            self.assertIsNotNone(self.tui.catalog)
            self.assertIsNotNone(self.tui.table)
            
            # Verify warehouse was created
            self.assertTrue(os.path.exists(self.tui.warehouse_path))
            
        except Exception as e:
            self.fail(f"Step 1 setup failed: {e}")
    
    @patch('builtins.input', return_value='')
    @patch('rich.live.Live')
    def test_step_2_insert(self, mock_live, mock_input):
        """Test step 2 insert functionality"""
        # First run setup
        self.tui.step_1_setup()
        
        try:
            self.tui.step_2_insert()
            
            # Verify step was incremented
            self.assertEqual(self.tui.current_step, 2)
            
            # Verify data was inserted
            df = self.tui.table.scan().to_pandas()
            self.assertEqual(len(df), 4)
            
            # Verify snapshot was recorded
            self.assertEqual(len(self.tui.snapshots), 1)
            
        except Exception as e:
            self.fail(f"Step 2 insert failed: {e}")
    
    @patch('builtins.input', return_value='')
    @patch('rich.live.Live')
    def test_full_workflow_mocked(self, mock_live, mock_input):
        """Test the complete workflow with mocked UI"""
        try:
            # Run through all steps
            self.tui.step_1_setup()
            self.tui.step_2_insert()
            self.tui.step_3_upsert()
            self.tui.step_4_delete()
            
            # Verify final state
            self.assertEqual(self.tui.current_step, 4)
            self.assertEqual(len(self.tui.snapshots), 3)
            
            # Verify final data
            df = self.tui.table.scan().to_pandas()
            self.assertEqual(len(df), 3)  # After delete
            
            # Verify all snapshots are unique
            self.assertEqual(len(set(self.tui.snapshots)), 3)
            
        except Exception as e:
            self.fail(f"Full workflow test failed: {e}")


class TestTUIComponents(unittest.TestCase):
    """Test individual TUI components"""
    
    def setUp(self):
        """Set up test environment"""
        self.tui = IcebergTUI()
    
    def test_data_panel_with_empty_dataframe(self):
        """Test data panel with empty dataframe"""
        import pandas as pd
        df = pd.DataFrame()
        
        panel = self.tui.create_data_panel(df, "Test Title")
        self.assertIsNotNone(panel)
        self.assertIn("No data", str(panel))
    
    def test_data_panel_with_data(self):
        """Test data panel with actual data"""
        import pandas as pd
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })
        
        panel = self.tui.create_data_panel(df, "Test Data")
        self.assertIsNotNone(panel)
        self.assertIn("Alice", str(panel))


if __name__ == "__main__":
    # Run tests with verbose output
    unittest.main(verbosity=2)
