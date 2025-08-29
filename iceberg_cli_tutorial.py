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

import os
import shutil
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, MapType
from pyiceberg.io import load_file_io
import fastavro
from pyiceberg import expressions
import json

def interactive_prompt(step_number, title, description, about_to_do):
    """Interactive prompt to explain and confirm each step"""
    print(f"\n{'='*80}")
    print(f"STEP {step_number}: {title}")
    print(f"{ '='*80}")
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
    cleanup_warehouse()
    os.makedirs("local_warehouse", exist_ok=True)
    warehouse_path = "local_warehouse"
    catalog_config = {
        "uri": f"sqlite:///{warehouse_path}/catalog.db",
        "warehouse": f"file://{os.path.abspath(warehouse_path)}"
    }
    catalog = load_catalog("default", **catalog_config)
    print(f"✅ Created catalog with warehouse at: {os.path.abspath(warehouse_path)}")
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "name", StringType(), required=True),
        NestedField(3, "metadata", MapType(key_id=4, key_type=StringType(), value_id=5, value_type=StringType()), required=False)
    )
    catalog.create_namespace("test_db")
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
    pa_table_v1 = pa.Table.from_pandas(df_v1, schema=table.schema().as_arrow(), preserve_index=False)
    table.append(pa_table_v1)
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
    data_v2 = {
        "id": [2, 4, 5, 6],
        "name": ["Robert", "Diana", "Eve", "Frank"],
        "metadata": [
            {"city": "San Francisco", "dept": "Engineering"},
            {"city": "Toronto", "dept": "Marketing"},
            {"city": "Paris", "dept": "Engineering"},
            {"city": "Berlin", "dept": "Sales"}
        ]
    }
    df_v2 = pd.DataFrame(data_v2)
    print("📊 Upsert data (updates + new records):")
    print(df_v2)
    pa_table_v2 = pa.Table.from_pandas(df_v2, schema=table.schema().as_arrow(), preserve_index=False)
    table.upsert(pa_table_v2, join_cols=['id'])
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
    current_df = table.scan().to_pandas()
    print("📊 Current data before delete:")
    print(current_df)
    table.delete(expressions.GreaterThan("id", 3))
    snapshot_id_v3 = table.current_snapshot().snapshot_id
    print(f"\n✅ V3 Delete completed successfully!")
    print(f"✅ Snapshot ID: {snapshot_id_v3}")
    print(f"✅ Records remaining: 3")
    final_df = table.scan().to_pandas()
    print("\n📊 Final data after delete:")
    print(final_df)
    return snapshot_id_v3

def _link(url, text):
    """Create a clickable hyperlink for terminals that support it."""
    return f"\x1b]8;;{url}\x07{text}\x1b]8;;\x07"

def analyze_iceberg_state(table, step_name):
    """
    Analyzes and prints the current state of the Iceberg table's file structure and metadata.
    """
    print(f"\n{'~'*80}")
    print(f"🔬 Analyzing Iceberg State after: {step_name}")
    print(f"{ '~'*80}")

    warehouse_path = "local_warehouse"
    
    print("\n📁 Current File Structure:")
    for root, _, files in os.walk(warehouse_path):
        level = root.replace(warehouse_path, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"{indent}{os.path.basename(root)}/")
        sub_indent = ' ' * 2 * (level + 1)
        for f in sorted(files):
            print(f"{sub_indent}{f}")

    if not table.metadata_location:
        print("\n" + "📝 Table is empty. No metadata file yet.")
        return

    metadata_location = table.metadata_location.replace('file://', '')
    
    metadata_link = _link("https://iceberg.apache.org/spec/#table-metadata", "Latest Metadata File")
    print(f"\n📄 1. {metadata_link}: {os.path.basename(metadata_location)}")
    with open(metadata_location, 'r') as f:
        metadata = json.load(f)
    
    current_snapshot_id = metadata.get('current-snapshot-id')
    if not current_snapshot_id:
        print("   - No current snapshot found. The table is empty.")
        return

    print(f"   - Points to current snapshot ID: {current_snapshot_id}")

    current_snapshot = next((s for s in metadata['snapshots'] if s['snapshot-id'] == current_snapshot_id), None)
    if not current_snapshot:
        print("   - Could not find current snapshot in metadata.")
        return

    manifest_list_path = current_snapshot['manifest-list'].replace('file://', '')
    manifest_list_link = _link("https://iceberg.apache.org/spec/#manifest-lists", "Manifest List")
    print(f"\n📜 2. {manifest_list_link}: {os.path.basename(manifest_list_path)}")
    print(f"   - This file lists all the 'manifest files' for snapshot {current_snapshot_id}.")
    
    manifest_files_info = []
    with open(manifest_list_path, 'rb') as f:
        reader = fastavro.reader(f)
        for manifest_file in reader:
            manifest_files_info.append(manifest_file)
            print(f"   - Contains manifest file: {os.path.basename(manifest_file['manifest_path'])}")
            print(f"     - Records: {manifest_file['added_rows_count']} added, {manifest_file['deleted_rows_count']} deleted")

    manifest_files_link = _link("https://iceberg.apache.org/spec/#manifests", "Manifest Files")
    print(f"\n🧾 3. {manifest_files_link}:")
    print("   - These files track individual data files (.parquet) and their status.")
    for info in manifest_files_info:
        manifest_path = info['manifest_path'].replace('file://', '')
        print(f"\n   Analyzing: {os.path.basename(manifest_path)}")
        with open(manifest_path, 'rb') as f:
            reader = fastavro.reader(f)
            for record in reader:
                status = record.get('status')
                status_map = {0: "EXISTING", 1: "ADDED", 2: "DELETED"}
                file_path = record['data_file']['file_path'].replace('file://', '')
                print(f"     - Data File: {os.path.basename(file_path)}")
                print(f"       - Status: {status_map.get(status, 'UNKNOWN')}")
                print(f"       - Record Count: {record['data_file']['record_count']}")

    print("\n🔗 How it's all connected:")
    print("   1. The `metadata.json` file is the entry point. It points to the current snapshot.")
    print("   2. The snapshot points to a `manifest-list.avro` file.")
    print("   3. The `manifest-list.avro` file lists one or more `manifest-file.avro` files.")
    print("   4. Each `manifest-file.avro` tracks the state of individual data files (`.parquet`).")
    print("   This chain allows Iceberg to provide atomic snapshots (ACID) of the entire table.")

def demonstrate_time_travel(table, snapshot_ids):
    """Demonstrate time travel capabilities"""
    interactive_prompt(
        5, # Updated step number
        "TIME TRAVEL DEMONSTRATION",
        "Iceberg's time travel lets us query historical versions of our data.",
        "• Query V1: Original 4 users\n• Query V2: After upsert (6 users)\n• Query V3: After delete (3 users)\n• Show how each snapshot represents a point in time"
    )
    print("🕰️  Time Travel Through Snapshots:")
    for i, snapshot_id in enumerate(snapshot_ids, 1):
        print(f"\n📸 Version {i} (Snapshot {snapshot_id}):")
        historical_df = table.scan(snapshot_id=snapshot_id).to_pandas()
        print(historical_df)
        print(f"   Records: {len(historical_df)}")

def main():
    """Main demonstration function"""
    os.system('clear')
    print("🚀 APACHE ICEBERG COMPREHENSIVE DEMONSTRATION")
    print("=" * 50)
    print("This demo shows ACID operations, file structure, and internals")
    print("You'll see INSERT, UPSERT, DELETE, and time travel capabilities")
    
    try:
        catalog, table = setup_iceberg_environment()
        analyze_iceberg_state(table, "Initial Environment Setup")
        snapshot_ids = []
        
        snapshot_ids.append(insert_initial_data(table))
        analyze_iceberg_state(table, "Initial Insert")

        snapshot_ids.append(perform_upsert_operation(table))
        analyze_iceberg_state(table, "Upsert Operation")

        snapshot_ids.append(perform_delete_operation(table))
        analyze_iceberg_state(table, "Delete Operation")
        
        demonstrate_time_travel(table, snapshot_ids)
        
        print("\n" + "=" * 80)
        print("🎉 DEMONSTRATION COMPLETE!")
        print("=" * 80)
        print("✅ You've seen Iceberg's ACID operations in action")
        print("✅ Explored the internal file structure and metadata linkage") 
        print("✅ Demonstrated time travel capabilities")
        print("\n💡 Key takeaway: Iceberg provides ACID guarantees while maintaining")
        print("   complete data lineage and the ability to query any historical version!")
        
    except Exception as e:
        print(f"\n❌ Error during demonstration: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()



