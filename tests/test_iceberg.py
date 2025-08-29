#!/usr/bin/env python3
"""
Test suite for Apache Iceberg Demo
==================================
This file contains automated tests that validate all demo functionality
without requiring user interaction.
"""

import unittest
import tempfile
import os
import shutil
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, MapType
from pyiceberg import expressions


class TestIcebergDemo(unittest.TestCase):
    """Test cases for Iceberg demo functionality"""
    
    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.warehouse_path = os.path.join(self.temp_dir, "warehouse")
        os.makedirs(self.warehouse_path, exist_ok=True)
        
    def tearDown(self):
        """Clean up test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_maptype_creation(self):
        """Test that MapType can be created with field IDs"""
        try:
            map_type = MapType(key_id=4, key=StringType(), value_id=5, value=StringType())
            self.assertIsNotNone(map_type)
        except Exception as e:
            self.fail(f"MapType creation failed: {e}")
    
    def test_schema_creation(self):
        """Test schema creation with MapType"""
        try:
            schema = Schema(
                NestedField(1, "id", LongType(), required=True),
                NestedField(2, "name", StringType(), required=True),
                NestedField(3, "metadata", MapType(key_id=4, key=StringType(), value_id=5, value=StringType()), required=False)
            )
            self.assertEqual(len(schema.fields), 3)
        except Exception as e:
            self.fail(f"Schema creation failed: {e}")
    
    def test_catalog_creation(self):
        """Test catalog creation"""
        try:
            catalog_db_path = os.path.join(self.warehouse_path, "catalog.db")
            catalog_config = {
                "uri": f"sqlite:///{catalog_db_path}",
                "warehouse": f"file://{self.warehouse_path}"
            }
            
            catalog = load_catalog("default", **catalog_config)
            self.assertIsNotNone(catalog)
        except Exception as e:
            self.fail(f"Catalog creation failed: {e}")
    
    def test_table_creation_and_operations(self):
        """Test complete table lifecycle"""
        try:
            # Setup
            catalog_db_path = os.path.join(self.warehouse_path, "catalog.db")
            catalog_config = {
                "uri": f"sqlite:///{catalog_db_path}",
                "warehouse": f"file://{self.warehouse_path}"
            }
            
            catalog = load_catalog("default", **catalog_config)
            
            # Create namespace first
            try:
                catalog.create_namespace("test_db")
            except Exception:
                # Namespace might already exist, that's ok
                pass
            
            schema = Schema(
                NestedField(1, "id", LongType(), required=True),
                NestedField(2, "name", StringType(), required=True),
                NestedField(3, "metadata", MapType(key_id=4, key=StringType(), value_id=5, value=StringType()), required=False)
            )
            
            table = catalog.create_table("test_db.users", schema)
            self.assertIsNotNone(table)
            
            # Test insert
            data = {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "metadata": [
                    {"city": "New York", "dept": "Engineering"}, 
                    {"city": "San Francisco", "dept": "Marketing"}
                ]
            }
            
            df = pd.DataFrame(data)
            pa_table = pa.Table.from_pandas(df, schema=table.schema().as_arrow(), preserve_index=False)
            table.append(pa_table)
            
            # Verify data
            result_df = table.scan().to_pandas()
            self.assertEqual(len(result_df), 2)
            self.assertEqual(list(result_df['name']), ['Alice', 'Bob'])
            
            # Test delete
            table.delete(expressions.GreaterThan("id", 1))
            result_df = table.scan().to_pandas()
            self.assertEqual(len(result_df), 1)
            self.assertEqual(result_df.iloc[0]['name'], 'Alice')
            
        except Exception as e:
            self.fail(f"Table operations failed: {e}")
    
    def test_time_travel(self):
        """Test time travel functionality"""
        try:
            # Setup table with multiple snapshots
            catalog_db_path = os.path.join(self.warehouse_path, "catalog.db")
            catalog_config = {
                "uri": f"sqlite:///{catalog_db_path}",
                "warehouse": f"file://{self.warehouse_path}"
            }
            
            catalog = load_catalog("default", **catalog_config)
            
            # Create namespace first
            try:
                catalog.create_namespace("test_db")
            except Exception:
                # Namespace might already exist, that's ok
                pass
                
            schema = Schema(
                NestedField(1, "id", LongType(), required=True),
                NestedField(2, "name", StringType(), required=True),
                NestedField(3, "metadata", MapType(key_id=4, key=StringType(), value_id=5, value=StringType()), required=False)
            )
            
            table = catalog.create_table("test_db.time_travel", schema)
            
            # First snapshot
            data1 = {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "metadata": [{"city": "NYC", "dept": "Eng"}, {"city": "SF", "dept": "Sales"}]
            }
            df1 = pd.DataFrame(data1)
            pa_table1 = pa.Table.from_pandas(df1, schema=table.schema().as_arrow(), preserve_index=False)
            table.append(pa_table1)
            snapshot1 = table.current_snapshot().snapshot_id
            
            # Second snapshot
            data2 = {
                "id": [3],
                "name": ["Charlie"],
                "metadata": [{"city": "LA", "dept": "Marketing"}]
            }
            df2 = pd.DataFrame(data2)
            pa_table2 = pa.Table.from_pandas(df2, schema=table.schema().as_arrow(), preserve_index=False)
            table.append(pa_table2)
            snapshot2 = table.current_snapshot().snapshot_id
            
            # Test time travel
            historical_df = table.scan().use_ref(str(snapshot1)).to_pandas()
            self.assertEqual(len(historical_df), 2)
            
            current_df = table.scan().to_pandas()
            self.assertEqual(len(current_df), 3)
            
        except Exception as e:
            self.fail(f"Time travel test failed: {e}")


def run_tests():
    """Run all tests and return success status"""
    suite = unittest.TestLoader().loadTestsFromTestCase(TestIcebergDemo)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


if __name__ == "__main__":
    print("🧪 Running Apache Iceberg Tests...")
    print("=" * 50)
    
    success = run_tests()
    
    if success:
        print("\n🎉 All tests passed!")
        exit(0)
    else:
        print("\n❌ Some tests failed!")
        exit(1)
