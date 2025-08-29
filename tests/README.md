# Test Suite Overview

This directory contains the automated test suite for the Apache Iceberg Demo Project. These tests ensure the correctness and reliability of the demo scripts and their underlying Iceberg operations.

## Test Files

### `test_basic_demo.py`
- **Purpose:** Contains unit tests for the core functions of the `iceberg_cli_tutorial.py` script. It verifies individual components and the overall logical flow of the command-line tutorial.
- **Key Tests:**
    - `test_cleanup_warehouse`: Verifies the warehouse cleanup functionality.
    - `test_setup_iceberg_environment`: Checks the initial setup of the Iceberg environment (catalog, table, schema).
    - `test_full_demo_workflow`: Runs through the entire CLI tutorial workflow (insert, upsert, delete) and asserts on data states and snapshot IDs.

### `test_iceberg.py`
- **Purpose:** Provides unit tests for fundamental Iceberg concepts and operations, independent of the specific demo scripts. These tests validate core `pyiceberg` interactions.
- **Key Tests:**
    - `test_maptype_creation`: Verifies correct creation of `MapType` with field IDs.
    - `test_schema_creation`: Checks schema definition with `MapType`.
    - `test_catalog_creation`: Tests the creation of an Iceberg catalog.
    - `test_table_creation_and_operations`: Validates the complete table lifecycle including insert and delete operations.
    - `test_time_travel`: Confirms time travel functionality by querying historical snapshots.

### `test_integration.py`
- **Purpose:** Contains integration tests that verify the end-to-end functionality and interaction between different components of the demo project. It ensures that the demo scripts work correctly in a broader context.
- **Key Tests:**
    - `test_cli_tutorial_runs_without_error`: Ensures the `iceberg_cli_tutorial.py` can be imported and its main functions are accessible.
    - `test_tui_tutorial_runs_without_error`: Ensures the `iceberg_tui_tutorial.py` can be imported and its main class is accessible.
    - `test_project_structure`: Verifies the existence of key project files (`.py` scripts, `README.md`, `pyproject.toml`, `tests/` directory).
    - `test_dependencies_available`: Checks if all required Python packages can be imported.
    - `test_warehouse_creation_and_cleanup`: Tests the creation and cleanup of the local Iceberg warehouse across demo types.
    - `test_import_time`: Measures and asserts on the import time of the main demo modules.
    - `test_memory_usage_basic`: (Skipped if `psutil` is not available) Attempts to measure memory usage of the CLI tutorial.

### `test_interactive_demo.py`
- **Purpose:** Tests the core logical flow and state changes of the `iceberg_tui_tutorial.py` script in a non-interactive, mocked environment. It focuses on the correctness of the underlying Iceberg operations triggered by the TUI.
- **Key Tests:**
    - `test_tui_initialization`: Verifies the initial state of the `IcebergTUI` object.
    - `test_cleanup_warehouse`: Tests the warehouse cleanup utility function used by the TUI.
    - `test_tui_full_workflow_non_interactive`: Runs the entire TUI demo in a mocked, non-interactive mode and asserts on the final state of the Iceberg table (number of snapshots, final data content).

## Running Tests

For instructions on how to run the tests, please refer to the "Testing" section in the main `README.md` file.
