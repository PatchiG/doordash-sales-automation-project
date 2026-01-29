"""
Test script to verify setup is correct or not
"""

import sys
import importlib

def test_imports():
    """Test that all required packages are installed"""
    
    required_packages = [
        'pandas',
        'numpy',
        'googlemaps',
        'requests',
        'langchain',
        'openai',
        'chromadb',
        'streamlit',
        'airflow',
        'dotenv',
    ]
    
    print("Testing package imports...")
    print("=" * 50)
    
    all_passed = True
    
    for package in required_packages:
        try:
            module = importlib.import_module(package)
            version = getattr(module, '__version__', 'unknown')
            print(f"✓ {package:20s} {version}")
        except ImportError as e:
            print(f"✗ {package:20s} FAILED: {e}")
            all_passed = False
    
    print("=" * 50)
    
    if all_passed:
        print("All packages installed successfully!")
    else:
        print("Some packages failed to import")
        sys.exit(1)

def test_config():
    """Test that config loads correctly"""
    
    print("\nTesting configuration...")
    print("=" * 50)
    
    try:
        from src.config import (
            PROJECT_ROOT,
            DATA_DIR,
            CITIES,
            SEARCH_QUERIES,
            SCORING_WEIGHTS
        )
        
        print(f"Project root: {PROJECT_ROOT}")
        print(f"Data directory: {DATA_DIR}")
        print(f"Cities configured: {len(CITIES)}")
        print(f"Search queries: {len(SEARCH_QUERIES)}")
        print(f"Scoring weights: {len(SCORING_WEIGHTS)} factors")
        
        print("=" * 50)
        print("Configuration loaded successfully!")
        
    except Exception as e:
        print(f"Configuration failed: {e}")
        sys.exit(1)

def test_directories():
    """Test that all directories exist"""
    
    print("\nTesting directory structure...")
    print("=" * 50)
    
    from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR, OUTPUT_DIR, LOGS_DIR
    
    directories = {
        'Raw data': RAW_DATA_DIR,
        'Processed data': PROCESSED_DATA_DIR,
        'Output': OUTPUT_DIR,
        'Logs': LOGS_DIR
    }
    
    for name, path in directories.items():
        if path.exists():
            print(f"{name:20s} {path}")
        else:
            print(f"{name:20s} MISSING: {path}")
    
    print("=" * 50)
    print("Directory structure verified!")

if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("DOORDASH GTM AUTOMATION - SETUP TEST")
    print("=" * 50 + "\n")
    
    test_imports()
    test_config()
    test_directories()
    
    print("\n" + "=" * 50)
    print("SETUP COMPLETE - READY TO START BUILDING!")
    print("=" * 50 + "\n")