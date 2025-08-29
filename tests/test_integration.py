#!/usr/bin/env python3
"""
Integration tests for the Apache Iceberg Demo Project.
Tests end-to-end functionality and integration between components.
"""

import unittest
import tempfile
import os
import shutil
import sys
import subprocess
import time
from pathlib import Path


class TestIntegration(unittest.TestCase):
    """Integration tests for the demo project"""
    
    def setUp(self):
        """Set up test environment"""
        self.test_dir = tempfile.mkdtemp(prefix="iceberg_integration_test_")
        self.original_cwd = os.getcwd()
        self.project_root = Path(__file__).parent.parent
        
    def tearDown(self):
        """Clean up test environment"""
        os.chdir(self.original_cwd)
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_cli_tutorial_runs_without_error(self):
        """Test that the CLI tutorial can be imported and key functions exist"""
        # Change to project directory
        os.chdir(self.project_root)
        
        try:
            # Import the CLI tutorial module
            sys.path.insert(0, str(self.project_root))
            import iceberg_cli_tutorial
            
            # Check that main functions exist
            self.assertTrue(hasattr(iceberg_cli_tutorial, 'cleanup_warehouse'))
            self.assertTrue(hasattr(iceberg_cli_tutorial, 'setup_iceberg_environment'))
            self.assertTrue(hasattr(iceberg_cli_tutorial, 'insert_initial_data'))
            self.assertTrue(hasattr(iceberg_cli_tutorial, 'perform_upsert_operation'))
            self.assertTrue(hasattr(iceberg_cli_tutorial, 'perform_delete_operation'))
            self.assertTrue(hasattr(iceberg_cli_tutorial, 'main'))
            
        except ImportError as e:
            self.fail(f"Failed to import CLI tutorial: {e}")
        except Exception as e:
            self.fail(f"Unexpected error testing CLI tutorial: {e}")
    
    def test_tui_tutorial_runs_without_error(self):
        """Test that the TUI tutorial can be imported and key classes exist"""
        # Change to project directory
        os.chdir(self.project_root)
        
        try:
            # Import the TUI tutorial module
            sys.path.insert(0, str(self.project_root))
            import iceberg_tui_tutorial
            
            # Check that main class exists
            self.assertTrue(hasattr(iceberg_tui_tutorial, 'IcebergTUI'))
            self.assertTrue(hasattr(iceberg_tui_tutorial, 'main'))
            
            # Create instance to test initialization
            tui = iceberg_tui_tutorial.IcebergTUI()
            self.assertIsNotNone(tui)
            self.assertEqual(tui.current_step, 0)
            self.assertEqual(tui.total_steps, 7)
            
        except ImportError as e:
            self.fail(f"Failed to import TUI tutorial: {e}")
        except Exception as e:
            self.fail(f"Unexpected error testing TUI tutorial: {e}")
    
    def test_project_structure(self):
        """Test that the project has the expected structure"""
        # Check main demo files
        cli_tutorial = self.project_root / "iceberg_cli_tutorial.py"
        tui_tutorial = self.project_root / "iceberg_tui_tutorial.py"
        readme = self.project_root / "README.md"
        pyproject = self.project_root / "pyproject.toml"
        
        self.assertTrue(cli_tutorial.exists(), "CLI tutorial file missing")
        self.assertTrue(tui_tutorial.exists(), "TUI tutorial file missing")
        self.assertTrue(readme.exists(), "README.md missing")
        self.assertTrue(pyproject.exists(), "pyproject.toml missing")
        
        # Check test directory
        tests_dir = self.project_root / "tests"
        self.assertTrue(tests_dir.exists(), "Tests directory missing")
        self.assertTrue((tests_dir / "__init__.py").exists(), "Tests __init__.py missing")
    
    def test_dependencies_available(self):
        """Test that required dependencies can be imported"""
        required_packages = [
            'pandas',
            'pyarrow',
            'pyiceberg',
            'rich',
            'fastavro'
        ]
        
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                self.fail(f"Required package {package} not available")
    
    def test_warehouse_creation_and_cleanup(self):
        """Test warehouse creation and cleanup across both demos"""
        # Change to test directory
        os.chdir(self.test_dir)
        
        # Test CLI tutorial warehouse functions
        sys.path.insert(0, str(self.project_root))
        import iceberg_cli_tutorial
        
        # Test cleanup when no warehouse exists
        iceberg_cli_tutorial.cleanup_warehouse()
        self.assertFalse(os.path.exists("local_warehouse"))
        
        # Create a fake warehouse and test cleanup
        os.makedirs("local_warehouse", exist_ok=True)
        with open("local_warehouse/test.txt", "w") as f:
            f.write("test")
        
        self.assertTrue(os.path.exists("local_warehouse"))
        iceberg_cli_tutorial.cleanup_warehouse()
        self.assertFalse(os.path.exists("local_warehouse"))


class TestPerformance(unittest.TestCase):
    """Performance and resource tests"""
    
    def setUp(self):
        """Set up test environment"""
        self.project_root = Path(__file__).parent.parent
    
    def test_import_time(self):
        """Test that modules import in reasonable time"""
        import time
        
        # Test CLI tutorial import time
        start_time = time.time()
        sys.path.insert(0, str(self.project_root))
        import iceberg_cli_tutorial
        cli_import_time = time.time() - start_time
        
        # Test TUI tutorial import time
        start_time = time.time()
        import iceberg_tui_tutorial
        tui_import_time = time.time() - start_time
        
        # Imports should be reasonably fast (less than 5 seconds each)
        self.assertLess(cli_import_time, 5.0, "CLI tutorial import too slow")
        self.assertLess(tui_import_time, 5.0, "TUI tutorial import too slow")
    
    def test_memory_usage_basic(self):
        """Test that CLI tutorial doesn't use excessive memory"""
        try:
            import psutil
            import os
            
            # Get initial memory usage
            process = psutil.Process(os.getpid())
            initial_memory = process.memory_info().rss / 1024 / 1024  # MB
            
            # Import and create CLI tutorial components
            sys.path.insert(0, str(self.project_root))
            import iceberg_cli_tutorial
            
            # Check memory increase
            final_memory = process.memory_info().rss / 1024 / 1024  # MB
            memory_increase = final_memory - initial_memory
            
            # Memory increase should be reasonable (less than 500MB)
            self.assertLess(memory_increase, 500, "Excessive memory usage in CLI tutorial")
            
        except ImportError:
            # psutil not available, skip memory testing
            self.skipTest("psutil not available for memory testing")


if __name__ == "__main__":
    # Run tests with detailed output
    unittest.main(verbosity=2)