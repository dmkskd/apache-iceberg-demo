#!/usr/bin/env python3
"""
Compatibility test for PyArrow 18.1.0 with PyIceberg 0.7.1
"""

def test_pyarrow_compatibility():
    """Test if PyArrow 18.1.0 works with our setup"""
    print("🧪 Testing PyArrow compatibility...")
    
    try:
        # Test PyArrow import and basic functionality
        import pyarrow as pa
        print(f"✅ PyArrow version: {pa.__version__}")
        
        # Test basic Arrow operations
        data = [1, 2, 3, 4]
        arrow_array = pa.array(data)
        print(f"✅ Arrow array creation: {len(arrow_array)} items")
        
        # Test Table creation
        import pandas as pd
        df = pd.DataFrame({'id': [1, 2], 'name': ['test1', 'test2']})
        table = pa.Table.from_pandas(df)
        print(f"✅ Arrow table from pandas: {table.shape}")
        
        # Test PyIceberg imports with new PyArrow
        from pyiceberg.types import NestedField, LongType, StringType, MapType
        from pyiceberg.schema import Schema
        
        # Test MapType creation with field IDs
        map_type = MapType(key_id=4, key=StringType(), value_id=5, value=StringType())
        print("✅ MapType creation with field IDs works")
        
        # Test schema creation
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "name", StringType(), required=True),
            NestedField(3, "metadata", map_type, required=False)
        )
        print(f"✅ Schema creation: {len(schema.fields)} fields")
        
        # Test Arrow schema conversion
        arrow_schema = schema.as_arrow()
        print(f"✅ Arrow schema conversion: {len(arrow_schema)} fields")
        
        print("\n🎉 PyArrow 18.1.0 is compatible with PyIceberg 0.7.1!")
        return True
        
    except Exception as e:
        print(f"\n❌ Compatibility test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_pyarrow_compatibility()
    exit(0 if success else 1)
