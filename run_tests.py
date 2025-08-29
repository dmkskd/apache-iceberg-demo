#!/usr/bin/env python3
"""
Main test runner for the Apache Iceberg Demo Project.
Runs all tests and provides comprehensive reporting.
"""

import unittest
import sys
import os
from pathlib import Path


def discover_and_run_tests():
    """Discover and run all tests in the tests directory"""
    
    # Add the project root to the Python path
    project_root = Path(__file__).parent
    sys.path.insert(0, str(project_root))
    
    # Set up test discovery
    test_dir = project_root / "tests"
    loader = unittest.TestLoader()
    
    # Discover all tests
    suite = loader.discover(
        start_dir=str(test_dir),
        pattern="test_*.py",
        top_level_dir=str(project_root)
    )
    
    # Create test runner with detailed output
    runner = unittest.TextTestRunner(
        verbosity=2,
        descriptions=True,
        failfast=False,
        buffer=True  # Capture stdout/stderr during tests
    )
    
    # Run the tests
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    
    if result.failures:
        print(f"\nFAILURES ({len(result.failures)}):")
        for test, traceback in result.failures:
            print(f"  - {test}")
    
    if result.errors:
        print(f"\nERRORS ({len(result.errors)}):")
        for test, traceback in result.errors:
            print(f"  - {test}")
    
    if result.skipped:
        print(f"\nSKIPPED ({len(result.skipped)}):")
        for test, reason in result.skipped:
            print(f"  - {test}: {reason}")
    
    # Return success/failure
    return result.wasSuccessful()


def run_specific_test(test_module=None):
    """Run a specific test module"""
    if test_module:
        # Run specific test module
        suite = unittest.TestLoader().loadTestsFromName(f"tests.{test_module}")
    else:
        # Run all tests
        return discover_and_run_tests()
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Run tests for the Apache Iceberg Demo Project"
    )
    parser.add_argument(
        "--module", "-m",
        help="Run specific test module (e.g., test_cli_tutorial)",
        default=None
    )
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="List available test modules"
    )
    
    args = parser.parse_args()
    
    if args.list:
        # List available test modules
        test_dir = Path(__file__).parent / "tests"
        test_files = list(test_dir.glob("test_*.py"))
        
        print("Available test modules:")
        for test_file in test_files:
            module_name = test_file.stem
            print(f"  - {module_name}")
        return
    
    # Run tests
    if args.module:
        success = run_specific_test(args.module)
    else:
        success = discover_and_run_tests()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
