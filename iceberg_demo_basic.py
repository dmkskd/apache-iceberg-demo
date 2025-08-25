# COMPREHENSIVE APACHE ICEBERG DEMONSTRATION
# ===============================================
# This script demonstrates Iceberg's ACID operations and internal mechanics:
#
# OPERATIONS DEMONSTRATED:
# - V1: Initial INSERT (4 rows) 
# - V2: UPSERT operation (add 2 new rows + modify 2 existing = 6 total, 2 modified)
# - V3: DELETE operation (remove 3 rows = 3 remaining)
#
# INTERNALS EXPLORED:
# - Parquet file creation and management
# - Metadata.json evolution 
# - Manifest list and manifest file contents
# - Snapshot management and time travel
# - How clients determine current vs historical versions

# 1. Import necessary libraries
# OPERATIONS DEMONSTRATED:
# - V1: Initial INSERT (4 rows) 
# - V2: UPSERT operation (add 2 new rows + modify 2 existing = 6 total, 2 modified)
# - V3: DELETE operation (remove 3 rows = 3 remaining)
#
# INTERNALS EXPLORED:
# - Parquet file creation and management
# - Metadata.json evolution 
# - Manifest list and manifest file contents
# - Snapshot management and time travel
# - How clients determine current vs historical versions

import os
import shutil
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, MapType
import duckdb
from pyiceberg.io import load_file_io
# Use the 'fastavro' library to reliably read Avro files, as pyarrow.avro
# is causing ModuleNotFoundError on some systems.
import fastavro
import pyiceberg.transforms as transforms
# Import the expressions module for filtering
from pyiceberg import expressions


def interactive_prompt(step_number, title, description, about_to_do):
    """Interactive prompt to explain and confirm each step"""
    print(f"\n{'='*80}")
    print(f"STEP {step_number}: {title}")
    print(f"{'='*80}")
    print(f"\nDESCRIPTION:")
    print(f"{description}")
    print(f"\nWHAT WE'RE ABOUT TO DO:")
    print(f"{about_to_do}")
    print(f"\n{'-'*80}")
    
    user_input = input("Press ENTER to continue, or type 'q' to quit: ").strip().lower()
    if user_input == 'q':
        print("Exiting demonstration.")
        exit()
    print()


def cleanup_warehouse():
    """Clean up the warehouse directory"""
    warehouse_path = "local_warehouse"
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)
        print(f"✅ Cleaned up {warehouse_path}")


def setup_iceberg_environment():
    """Set up the Iceberg catalog and create initial table"""
    
    interactive_prompt(
        1, 
        "ICEBERG ENVIRONMENT SETUP",
        "We'll create a local Iceberg catalog using SQLite and define our table schema.",
        "• Clean up any existing warehouse\n• Create a new catalog\n• Define a users table with id, name, and metadata columns"
    )
    
    # Clean up existing warehouse
    cleanup_warehouse()
    
    # Create catalog configuration
    warehouse_path = "local_warehouse"
    catalog_config = {
        "uri": f"sqlite:///{warehouse_path}/catalog.db",
        "warehouse": f"file://{os.path.abspath(warehouse_path)}"
    }
    
    # Initialize catalog
    catalog = load_catalog("default", **catalog_config)
    print(f"✅ Created catalog with warehouse at: {os.path.abspath(warehouse_path)}")
    
    # Define table schema
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "name", StringType(), required=True),
        NestedField(3, "metadata", MapType(StringType(), StringType()), required=False)
    )
    
    # Create the table
    table = catalog.create_table("test_db.users", schema)
    print(f"✅ Created table: test_db.users")
    print(f"✅ Schema: id (Long), name (String), metadata (Map<String,String>)")
    
    return catalog, table


def insert_initial_data(table):
    """Insert initial data (V1)"""
    
    interactive_prompt(
        2, 
        "INITIAL DATA INSERT (V1)",
        "We'll insert 4 users into our empty table. This creates the first snapshot.",
        "• Insert 4 users: Alice, Bob, Charlie, Diana\n• Create first Parquet file\n• Generate manifest files\n• Update metadata.json"
    )
    
    # Prepare data
    data_v1 = {
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "metadata": [
            {"city": "New York", "dept": "Engineering"}, 
            {"city": "San Francisco", "dept": "Marketing"}, 
            {"city": "London", "dept": "Sales"},
            {"city": "Toronto", "dept": "Engineering"}
        ]
    }
    
    df_v1 = pd.DataFrame(data_v1)
    print("📊 Data to insert:")
    print(df_v1)
    
    # Convert to PyArrow table
    pa_table_v1 = pa.Table.from_pandas(df_v1, schema=table.schema().as_arrow(), preserve_index=False)
    
    # Insert data
    table.append(pa_table_v1)
    
    # Get snapshot info
    snapshot_id_v1 = table.current_snapshot().snapshot_id
    print(f"\n✅ V1 Data inserted successfully!")
    print(f"✅ Snapshot ID: {snapshot_id_v1}")
    print(f"✅ Records: 4")
    
    return snapshot_id_v1


def perform_upsert_operation(table):
    """Perform upsert operation (V2)"""
    
    interactive_prompt(
        3, 
        "UPSERT OPERATION (V2)",
        "We'll update some existing records and add new ones. Iceberg handles this as an atomic operation.",
        "• Update Bob → Robert (promotion)\n• Update Diana's department\n• Add Eve and Frank\n• Result: 6 total users"
    )
    
    # Prepare upsert data
    data_v2 = {
        "id": [2, 4, 5, 6],  # 2,4 = updates, 5,6 = new
        "name": ["Robert", "Diana", "Eve", "Frank"],  # Bob becomes Robert
        "metadata": [
            {"city": "San Francisco", "dept": "Engineering"},  # Bob promoted to Engineering
            {"city": "Toronto", "dept": "Marketing"},          # Diana moves to Marketing
            {"city": "Paris", "dept": "Engineering"},           # New: Eve
            {"city": "Berlin", "dept": "Sales"}                # New: Frank
        ]
    }
    
    df_v2 = pd.DataFrame(data_v2)
    print("📊 Upsert data (updates + new records):")
    print(df_v2)
    
    # Convert to PyArrow table
    pa_table_v2 = pa.Table.from_pandas(df_v2, schema=table.schema().as_arrow(), preserve_index=False)
    
    # Perform upsert (this overwrites the entire table with merged data)
    table.overwrite(pa_table_v2)
    
    # Get snapshot info
    snapshot_id_v2 = table.current_snapshot().snapshot_id
    print(f"\n✅ V2 Upsert completed successfully!")
    print(f"✅ Snapshot ID: {snapshot_id_v2}")
    print(f"✅ Records: 6 (2 updated, 2 added, 2 unchanged)")
    
    return snapshot_id_v2


def perform_delete_operation(table):
    """Perform delete operation (V3)"""
    
    interactive_prompt(
        4, 
        "DELETE OPERATION (V3)",
        "We'll delete specific records based on criteria. This preserves data history.",
        "• Delete users with id > 3 (Diana, Eve, Frank)\n• Keep Alice, Charlie, Robert\n• Result: 3 remaining users"
    )
    
    # Show current data before delete
    current_df = table.scan().to_pandas()
    print("📊 Current data before delete:")
    print(current_df)
    
    # Perform delete using filter (keep only id <= 3)
    table.delete(expressions.GreaterThan("id", 3))
    
    # Get snapshot info
    snapshot_id_v3 = table.current_snapshot().snapshot_id
    print(f"\n✅ V3 Delete completed successfully!")
    print(f"✅ Snapshot ID: {snapshot_id_v3}")
    print(f"✅ Records remaining: 3")
    
    # Show final data
    final_df = table.scan().to_pandas()
    print("\n📊 Final data after delete:")
    print(final_df)
    
    return snapshot_id_v3


def explore_file_structure():
    """Explore the generated file structure"""
    
    interactive_prompt(
        5, 
        "FILE STRUCTURE EXPLORATION",
        "Let's examine the files Iceberg created and understand the internal structure.",
        "• Inspect Parquet data files\n• Examine metadata.json evolution\n• Look at manifest files\n• Understand snapshot tracking"
    )
    
    warehouse_path = "local_warehouse"
    print(f"🔍 Exploring warehouse structure: {warehouse_path}")
    
    for root, dirs, files in os.walk(warehouse_path):
        level = root.replace(warehouse_path, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"{indent}{os.path.basename(root)}/")
        sub_indent = ' ' * 2 * (level + 1)
        for file in files:
            file_path = os.path.join(root, file)
            file_size = os.path.getsize(file_path)
            print(f"{sub_indent}{file} ({file_size:,} bytes)")


def demonstrate_time_travel(table, snapshot_ids):
    """Demonstrate time travel capabilities"""
    
    interactive_prompt(
        6, 
        "TIME TRAVEL DEMONSTRATION",
        "Iceberg's time travel lets us query historical versions of our data.",
        "• Query V1: Original 4 users\n• Query V2: After upsert (6 users)\n• Query V3: After delete (3 users)\n• Show how each snapshot represents a point in time"
    )
    
    print("🕰️  Time Travel Through Snapshots:")
    
    for i, snapshot_id in enumerate(snapshot_ids, 1):
        print(f"\n📸 Version {i} (Snapshot {snapshot_id}):")
        historical_df = table.scan().use_ref(str(snapshot_id)).to_pandas()
        print(historical_df)
        print(f"   Records: {len(historical_df)}")


def analyze_metadata_files():
    """Analyze metadata files to understand Iceberg internals"""
    
    interactive_prompt(
        7, 
        "METADATA ANALYSIS",
        "Let's dive deep into Iceberg's metadata files to understand how it tracks changes.",
        "• Read metadata.json files\n• Examine manifest lists\n• Inspect manifest files\n• Understand snapshot relationships"
    )
    
    metadata_dir = "local_warehouse/test_db/users/metadata"
    
    if not os.path.exists(metadata_dir):
        print("❌ Metadata directory not found")
        return
    
    # Find metadata files
    metadata_files = [f for f in os.listdir(metadata_dir) if f.endswith('.metadata.json')]
    metadata_files.sort()
    
    print(f"📁 Found {len(metadata_files)} metadata files:")
    for file in metadata_files:
        print(f"   • {file}")
    
    # Analyze the latest metadata file
    if metadata_files:
        latest_metadata = metadata_files[-1]
        metadata_path = os.path.join(metadata_dir, latest_metadata)
        
        print(f"\n🔍 Analyzing latest metadata: {latest_metadata}")
        
        try:
            import json
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            
            print(f"   Format version: {metadata.get('format-version', 'Unknown')}")
            print(f"   Table UUID: {metadata.get('table-uuid', 'Unknown')}")
            print(f"   Current schema ID: {metadata.get('current-schema-id', 'Unknown')}")
            print(f"   Current snapshot ID: {metadata.get('current-snapshot-id', 'Unknown')}")
            
            snapshots = metadata.get('snapshots', [])
            print(f"   Total snapshots: {len(snapshots)}")
            
            for i, snapshot in enumerate(snapshots, 1):
                timestamp = snapshot.get('timestamp-ms', 0)
                import datetime
                dt = datetime.datetime.fromtimestamp(timestamp / 1000)
                print(f"     Snapshot {i}: {snapshot.get('snapshot-id')} ({dt.strftime('%Y-%m-%d %H:%M:%S')})")
                
        except Exception as e:
            print(f"   ❌ Error reading metadata: {e}")


def main():
    """Main demonstration function"""
    print("🚀 APACHE ICEBERG COMPREHENSIVE DEMONSTRATION")
    print("=" * 50)
    print("This demo shows ACID operations, file structure, and internals")
    print("You'll see INSERT, UPSERT, DELETE, and time travel capabilities")
    
    try:
        # Set up environment
        catalog, table = setup_iceberg_environment()
        
        # Track snapshots for time travel
        snapshot_ids = []
        
        # Perform operations
        snapshot_ids.append(insert_initial_data(table))
        snapshot_ids.append(perform_upsert_operation(table))
        snapshot_ids.append(perform_delete_operation(table))
        
        # Explore and analyze
        explore_file_structure()
        demonstrate_time_travel(table, snapshot_ids)
        analyze_metadata_files()
        
        print("\n" + "=" * 80)
        print("🎉 DEMONSTRATION COMPLETE!")
        print("=" * 80)
        print("✅ You've seen Iceberg's ACID operations in action")
        print("✅ Explored the internal file structure") 
        print("✅ Demonstrated time travel capabilities")
        print("✅ Analyzed metadata and snapshot evolution")
        print("\n💡 Key takeaway: Iceberg provides ACID guarantees while maintaining")
        print("   complete data lineage and the ability to query any historical version!")
        
    except Exception as e:
        print(f"\n❌ Error during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
