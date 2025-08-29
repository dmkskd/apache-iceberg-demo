# Apache Iceberg Demo Project

This project demonstrates Apache Iceberg's core features through two different demo scripts that showcase ACID operations, schema evolution, time travel, and file structure visualization.

## 📋 Prerequisites

- Python 3.8+

## 🚀 Quick Start

1.  **Clone the repository:**
    ```bash
    git clone <repo_url>
    cd <repo_name>
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -e .
    ```
    This command reads the `pyproject.toml` file and installs the necessary packages in editable mode.

## 🎯 Demo Scripts

This project contains two scripts to demonstrate Apache Iceberg's features.

### 1. Interactive CLI Tutorial (`iceberg_cli_tutorial.py`) - Recommended

This script provides a step-by-step, interactive tutorial through the command line. It's the most stable and comprehensive demo in this project.

**Features:**
- **Interactive & Educational:** Pauses at each step to explain the concepts and operations.
- **Detailed Analysis:** After each operation (INSERT, UPSERT, DELETE), it provides a deep dive into the Iceberg internals, showing exactly which files were created and how they are linked.
- **Clickable Links:** Includes links to the official Iceberg specification for different file types, allowing you to learn more about the concepts.

**To run the tutorial:**
```bash
python iceberg_cli_tutorial.py
```

### 2. TUI Demonstration (`iceberg_demo_interactive.py`) - Experimental

This script showcases Iceberg concepts using a Terminal User Interface (TUI) built with the Rich library.

**Note:** This TUI demo is currently **experimental** and less stable than the CLI tutorial. While it demonstrates a more advanced UI, it may have some rendering or stability issues.

**Features:**
- Rich TUI with panels and layouts.
- Visual progress tracking.

**To run the TUI demo:**
```bash
python iceberg_demo_interactive.py
```

## 🔍 What You'll Learn

Both demos demonstrate these Apache Iceberg concepts:

### 1. **ACID Operations**

- **INSERT**: Adding initial data to create the first table snapshot
- **UPSERT**: Updating existing records and adding new ones
- **DELETE**: Removing specific records while preserving history

### 2. **Schema Evolution**

- Safe column addition without breaking existing data
- Backward compatibility maintenance
- Metadata version management

### 3. **Time Travel**

- Query historical versions of your data
- Access any snapshot by timestamp or snapshot ID
- Understand data lineage and changes over time

### 4. **File Structure & Metadata**

- Parquet data files organization
- Manifest files for change tracking
- Metadata.json evolution with each operation
- Catalog integration

## 📁 Generated Files

After running either demo, you'll find:

```text
local_warehouse/
├── catalog.db                 # Catalog database
└── test_db/
    └── users/
        ├── data/             # Parquet data files
        │   ├── 00000-*.parquet
        │   └── 00001-*.parquet
        └── metadata/         # Iceberg metadata
            ├── *.metadata.json
            ├── *.avro (manifests)
            └── snap-*.avro
```

## 🛠 Technical Details

- **Storage**: Local file system (configurable)
- **Format**: Parquet with Snappy compression
- **Catalog**: SQLite-based catalog
- **Schema**: Users table with id, name, metadata fields
- **Operations**: Copy-on-Write (COW) strategy

## 🎨 Demo Comparison

| Feature | Interactive CLI Tutorial | TUI Demonstration |
|---------|--------------------------|-------------------|
| User Interaction | Step-by-step             | Step-by-step      |
| Visual Interface | Plain text               | Rich TUI panels   |
| Progress Tracking | Text output              | Visual progress bar |
| File Inspection | Detailed analysis        | Tree visualization |
| Data Preview | Simple print             | Formatted tables  |
| Learning Pace | User-controlled          | User-controlled   |
| Best For | Learning, exploration    | Visual demo       |

## 🧪 Testing

The project includes comprehensive tests to ensure everything works correctly:

### Running Tests

```bash
# Run all tests
python run_tests.py

# Run specific test module
python run_tests.py --module test_basic_demo

# List available test modules
python run_tests.py --list

# Or use unittest directly
python -m unittest discover tests/ -v
```

### Test Structure

```text
tests/
├── __init__.py                 # Test package initialization
├── test_basic_demo.py         # Tests for iceberg_demo_basic.py
├── test_interactive_demo.py   # Tests for iceberg_demo_interactive.py
└── test_integration.py        # End-to-end integration tests
```

### Test Coverage

- **Unit Tests**: Test individual functions and components
- **Integration Tests**: Test end-to-end workflows
- **UI Tests**: Test Rich TUI components and layouts
- **Error Handling**: Test error conditions and edge cases

## 🚀 Next Steps

After running the demos:

1. Explore the generated files in `local_warehouse/`
2. Run the tests to understand the codebase: `python run_tests.py`
3. Try modifying the schema or data in the scripts
4. Experiment with different Iceberg configurations
5. Integrate with your own data sources

## 📚 Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [Iceberg Table Format Specification](https://iceberg.apache.org/spec/)

## 🤝 Contributing

Feel free to enhance these demos or add new features! Both scripts are designed to be educational and easily extensible.

### Development Setup

1. Clone the repository and install dependencies
2. Run tests to ensure everything works: `python run_tests.py`
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass before submitting