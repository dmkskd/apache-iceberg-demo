# Apache Iceberg Demo Project

This project demonstrates Apache Iceberg's core features through two different demo scripts that showcase ACID operations, schema evolution, time travel, and file structure visualization.

## 📋 Prerequisites

- Python 3.8+
- [uv](https://docs.astral.sh/uv/) package manager (recommended) or pip

## 🚀 Quick Start

1. **Clone and setup:**

   ```bash
   git clone <repository-url>
   cd my-iceberg-test
   ```

2. **Install dependencies:**

   ```bash
   # Using uv (recommended)
   uv install

   # Or using pip
   pip install -r requirements.txt
   ```

## 🎯 Demo Scripts

### 1. Basic Demo (`iceberg_demo_basic.py`)

A straightforward command-line demonstration that runs through all Iceberg operations sequentially.

**Features:**

- Simple terminal output
- Sequential execution of all steps
- Basic file structure inspection
- Suitable for automated runs or CI/CD

**Run it:**

```bash
uv run iceberg_demo_basic.py
# or
python iceberg_demo_basic.py
```

### 2. Interactive Demo (`iceberg_demo_interactive.py`)

An enhanced Rich TUI (Terminal User Interface) demonstration with step-by-step interaction.

**Features:**

- Beautiful Rich TUI with panels and progress tracking
- Step-by-step user-controlled progression
- Real-time file structure visualization
- Interactive data inspection
- Metadata deep-dive capabilities
- Progress tracking with numbered steps

**Run it:**

```bash
uv run iceberg_demo_interactive.py
# or
python iceberg_demo_interactive.py
```

**Controls:**

- Press `ENTER` to proceed to the next step
- Type `q` + `ENTER` to quit at any time

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

| Feature | Basic Demo | Interactive Demo |
|---------|------------|------------------|
| User Interaction | None (automated) | Step-by-step |
| Visual Interface | Plain text | Rich TUI panels |
| Progress Tracking | Text output | Visual progress bar |
| File Inspection | Basic listing | Tree visualization |
| Data Preview | Simple print | Formatted tables |
| Learning Pace | Fast | User-controlled |
| Best For | CI/CD, scripting | Learning, exploration |

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
