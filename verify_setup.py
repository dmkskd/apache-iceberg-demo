#!/usr/bin/env python3
"""
Quick verification script to test that our setup is working correctly.
"""

import os
import sys
from pathlib import Path

def check_project_structure():
    """Check that all expected files are in place"""
    project_root = Path(__file__).parent
    
    expected_files = [
        "iceberg_demo_basic.py",
        "iceberg_demo_interactive.py", 
        "README.md",
        "pyproject.toml",
        "run_tests.py",
        "tests/__init__.py",
        "tests/test_basic_demo.py",
        "tests/test_interactive_demo.py",
        "tests/test_integration.py"
    ]
    
    print("🔍 Checking project structure...")
    missing_files = []
    
    for file_path in expected_files:
        full_path = project_root / file_path
        if full_path.exists():
            print(f"✅ {file_path}")
        else:
            print(f"❌ {file_path} - MISSING")
            missing_files.append(file_path)
    
    if missing_files:
        print(f"\n❌ {len(missing_files)} files are missing!")
        return False
    else:
        print(f"\n✅ All {len(expected_files)} files are present!")
        return True

def check_imports():
    """Check that our demo modules can be imported"""
    print("\n🔍 Checking imports...")
    
    # Add project root to path
    project_root = Path(__file__).parent
    sys.path.insert(0, str(project_root))
    
    try:
        import iceberg_demo_basic
        print("✅ iceberg_demo_basic.py imports successfully")
    except Exception as e:
        print(f"❌ iceberg_demo_basic.py import failed: {e}")
        return False
    
    try:
        import iceberg_demo_interactive
        print("✅ iceberg_demo_interactive.py imports successfully")
    except Exception as e:
        print(f"❌ iceberg_demo_interactive.py import failed: {e}")
        return False
    
    # Check test imports
    try:
        from tests import test_basic_demo
        print("✅ test_basic_demo.py imports successfully")
    except Exception as e:
        print(f"❌ test_basic_demo.py import failed: {e}")
        return False
    
    try:
        from tests import test_interactive_demo
        print("✅ test_interactive_demo.py imports successfully")
    except Exception as e:
        print(f"❌ test_interactive_demo.py import failed: {e}")
        return False
    
    try:
        from tests import test_integration
        print("✅ test_integration.py imports successfully")
    except Exception as e:
        print(f"❌ test_integration.py import failed: {e}")
        return False
    
    print("\n✅ All imports successful!")
    return True

def check_dependencies():
    """Check that required dependencies are available"""
    print("\n🔍 Checking dependencies...")
    
    required_packages = [
        'pandas',
        'pyarrow', 
        'pyiceberg',
        'rich',
        'fastavro'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package} - NOT AVAILABLE")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n❌ {len(missing_packages)} packages are missing!")
        print("Run: uv install  (or pip install -r requirements.txt)")
        return False
    else:
        print(f"\n✅ All {len(required_packages)} dependencies are available!")
        return True

def main():
    """Main verification function"""
    print("🚀 Apache Iceberg Demo Project - Setup Verification")
    print("=" * 60)
    
    structure_ok = check_project_structure()
    imports_ok = check_imports()
    deps_ok = check_dependencies()
    
    print("\n" + "=" * 60)
    print("📋 VERIFICATION SUMMARY")
    print("=" * 60)
    
    if structure_ok and imports_ok and deps_ok:
        print("🎉 ALL CHECKS PASSED!")
        print("\nYour project is ready to use:")
        print("• Run basic demo: python iceberg_demo_basic.py")
        print("• Run interactive demo: python iceberg_demo_interactive.py")
        print("• Run tests: python run_tests.py")
        return True
    else:
        print("❌ SOME CHECKS FAILED!")
        print("\nPlease fix the issues above before proceeding.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
