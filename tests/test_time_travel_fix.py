#!/usr/bin/env python3
"""
Test time travel functionality to ensure the snapshot reference fix works
"""

import os
import shutil
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, MapType
from pyiceberg import expressions

def test_time_travel():
    """Test that time travel with snapshot IDs works correctly"""
    warehouse_path = "test_warehouse"
    
    # Clean up
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)
    
    try:
        # Setup
        os.makedirs(warehouse_path, exist_ok=True)
        warehouse_abs_path = os.path.abspath(warehouse_path)
        catalog_db_path = os.path.join(warehouse_abs_path, "catalog.db")
        
        catalog_config = {
            "uri": f"sqlite:///{catalog_db_path}",
            "warehouse": f"file://{warehouse_abs_path}"
        }
        
        catalog = load_catalog("default", **catalog_config)
        
        # Create namespace and table
        try:
            catalog.create_namespace("test_db")
        except Exception:
            pass
        
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType(), required=True),
            NestedField(field_id=3, name="metadata", field_type=MapType(
                key_id=4, key_type=StringType(),
                value_id=5, value_type=StringType(),
                value_required=False
            ), required=False)
        )
        
        table = catalog.create_table("test_db.users", schema)
        
        # Insert V1 data
        data_v1 = {
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "metadata": [
                {"city": "New York", "dept": "Engineering"},
                {"city": "San Francisco", "dept": "Marketing"}
            ]
        }
        
        df_v1 = pd.DataFrame(data_v1)
        pa_table_v1 = pa.Table.from_pandas(df_v1, schema=table.schema().as_arrow(), preserve_index=False)
        table.append(pa_table_v1)
        
        snapshot_v1 = table.current_snapshot().snapshot_id
        print(f"V1 Snapshot: {snapshot_v1}")
        
        # Insert V2 data
        data_v2 = {
            "id": [3, 4],
            "name": ["Charlie", "Diana"],
            "metadata": [
                {"city": "London", "dept": "Sales"},
                {"city": "Toronto", "dept": "Engineering"}
            ]
        }
        
        df_v2 = pd.DataFrame(data_v2)
        pa_table_v2 = pa.Table.from_pandas(df_v2, schema=table.schema().as_arrow(), preserve_index=False)
        table.append(pa_table_v2)
        
        snapshot_v2 = table.current_snapshot().snapshot_id
        print(f"V2 Snapshot: {snapshot_v2}")
        
        # Test time travel with snapshot_id parameter
        print("\nTesting time travel...")
        
        # Current data (should have 4 records)
        current_df = table.scan().to_pandas()
        print(f"Current data: {len(current_df)} records")
        
        # V1 data (should have 2 records)
        v1_df = table.scan(snapshot_id=snapshot_v1).to_pandas()
        print(f"V1 data: {len(v1_df)} records")
        
        # V2 data (should have 4 records)
        v2_df = table.scan(snapshot_id=snapshot_v2).to_pandas()
        print(f"V2 data: {len(v2_df)} records")
        
        # Verify the fix worked
        assert len(current_df) == 4, f"Expected 4 current records, got {len(current_df)}"
        assert len(v1_df) == 2, f"Expected 2 V1 records, got {len(v1_df)}"
        assert len(v2_df) == 4, f"Expected 4 V2 records, got {len(v2_df)}"
        
        print("\n✅ Time travel test PASSED!")
        print("The snapshot_id parameter fix works correctly.")
        
    except Exception as e:
        print(f"\n❌ Time travel test FAILED: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Clean up
        if os.path.exists(warehouse_path):
            shutil.rmtree(warehouse_path)

if __name__ == "__main__":
    test_time_travel()
