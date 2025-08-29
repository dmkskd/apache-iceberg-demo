#!/usr/bin/env python3
"""
Test suite for iceberg_tui_tutorial.py
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

from iceberg_tui_tutorial import IcebergTUI


class TestInteractiveDemo(unittest.TestCase):
    """Test suite for the Interactive Iceberg demo"""
    
    def setUp(self):
        """Set up test environment"""
        self.test_dir = tempfile.mkdtemp(prefix="iceberg_interactive_test_")
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)
        
        # Create TUI instance with test configuration
        self.tui = IcebergTUI(test_mode=True) # Run in test mode
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

    @patch('rich.live.Live') # Mock rich.live.Live to prevent actual TUI rendering
    @patch('builtins.input', return_value='') # Mock user input
    def test_tui_full_workflow_non_interactive(self, mock_input, mock_live):
        """
        Test the complete TUI workflow in non-interactive mode.
        This tests the logical flow and state changes, not the UI rendering.
        """
        try:
            self.tui.run() # Run the entire demo
            
            # Assert on final state after all steps
            self.assertEqual(self.tui.current_step, self.tui.total_steps)
            self.assertIsNotNone(self.tui.catalog)
            self.assertIsNotNone(self.tui.table)
            self.assertEqual(len(self.tui.snapshots), 3) # 3 snapshots created (insert, upsert, delete)

            # Verify final data in the table
            final_df = self.tui.table.scan().to_pandas()
            self.assertEqual(len(final_df), 3) # After delete operation
            
            # Verify specific data points if needed
            self.assertIn("Alice", final_df["name"].values)
            self.assertIn("Charlie", final_df["name"].values)
            self.assertIn("Robert", final_df["name"].values)
            self.assertNotIn("Diana", final_df["name"].values) # Diana was deleted
            self.assertNotIn("Eve", final_df["name"].values)   # Eve was deleted
            self.assertNotIn("Frank", final_df["name"].values) # Frank was deleted

        except Exception as e:
            self.fail(f"TUI full workflow test failed: {e}")


if __name__ == "__main__":
    unittest.main(verbosity=2)