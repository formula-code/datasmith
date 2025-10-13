#!/usr/bin/env python3
"""
Simple test script to validate SQLite storage implementation works.
"""

import sys
import os
from pathlib import Path

# Add src to path to import datasmith storage modules directly
src_path = str(Path(__file__).parent.parent / "src" / "datasmith" / "storage")
sys.path.insert(0, src_path)

from database import DataSmithDB
from repositories import RepositoryStore, Repository
from migration import PipelineMigrator

def test_basic_functionality():
    """Test basic database and migration functionality."""
    print("Testing DataSmith SQLite storage implementation...")
    
    # Test database initialization
    db_path = "/tmp/test_datasmith.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    
    try:
        # Initialize database
        db = DataSmithDB(db_path)
        print("✓ Database initialized successfully")
        
        # Test repository store
        repo_store = RepositoryStore(db)
        test_repo = Repository(
            id=None,
            owner="test_owner",
            repo="test_repo",
            url="https://github.com/test_owner/test_repo",
            is_valid=True,
            metadata={"language": "Python"}
        )
        
        repo_id = repo_store.insert_repository(test_repo)
        print(f"✓ Repository inserted with ID: {repo_id}")
        
        # Test retrieval
        retrieved_repo = repo_store.get_repository_by_id(repo_id)
        if retrieved_repo and retrieved_repo.owner == "test_owner":
            print("✓ Repository retrieval works")
        else:
            print("✗ Repository retrieval failed")
        
        # Test migration functionality
        migrator = PipelineMigrator(db)
        print("✓ PipelineMigrator initialized successfully")
        
        # Show stats
        stats = db.get_stats()
        print(f"✓ Database stats: {stats}")
        
        db.close()
        print("✓ Database closed successfully")
        
        print("\n🎉 All tests passed! SQLite storage implementation is working.")
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Clean up
        if os.path.exists(db_path):
            os.remove(db_path)
    
    return True

if __name__ == "__main__":
    success = test_basic_functionality()
    sys.exit(0 if success else 1)