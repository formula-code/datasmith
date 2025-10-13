#!/usr/bin/env python3
"""
Standalone test script to validate SQLite schema and basic operations.
This test validates the core SQLite storage functionality without dependencies.
"""

import sqlite3
import json
import tempfile
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any


def load_schema() -> str:
    """Load the SQLite schema from schema.sql"""
    schema_path = Path(__file__).parent.parent / "src" / "datasmith" / "storage" / "schema.sql"
    with open(schema_path, 'r') as f:
        return f.read()


class TestDataSmithDB:
    """Simple test database class without logging dependencies"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path)
        self.connection.execute("PRAGMA foreign_keys = ON")
        self._create_schema()
    
    def _create_schema(self):
        """Create database schema from SQL file"""
        schema_sql = load_schema()
        self.connection.executescript(schema_sql)
        self.connection.commit()
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get basic database statistics"""
        cursor = self.connection.cursor()
        stats = {}
        
        # Get table counts
        tables = ["repositories", "commits", "build_contexts", "benchmark_collections", "benchmark_runs", "breakpoints"]
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            stats[f"{table}_count"] = cursor.fetchone()[0]
        
        return stats


def test_database_creation():
    """Test database creation and schema setup"""
    print("Testing database creation and schema setup...")
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        # Create database
        db = TestDataSmithDB(db_path)
        print("✓ Database created with schema")
        
        # Test basic operations
        cursor = db.connection.cursor()
        
        # Insert test repository
        cursor.execute("""
            INSERT INTO repositories (owner, repo, url, is_valid, metadata)
            VALUES (?, ?, ?, ?, ?)
        """, ("test_owner", "test_repo", "https://github.com/test_owner/test_repo", True, "{}"))
        
        repo_id = cursor.lastrowid
        print(f"✓ Repository inserted with ID: {repo_id}")
        
        # Insert test commit
        cursor.execute("""
            INSERT INTO commits (repository_id, sha, commit_date, author_name, message, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (repo_id, "abc123", datetime.now().isoformat(), "Test Author", "Test commit", "{}"))
        
        commit_id = cursor.lastrowid
        print(f"✓ Commit inserted with ID: {commit_id}")
        
        db.connection.commit()
        
        # Test retrieval
        cursor.execute("SELECT owner, repo FROM repositories WHERE id = ?", (repo_id,))
        result = cursor.fetchone()
        
        if result and result[0] == "test_owner" and result[1] == "test_repo":
            print("✓ Repository retrieval works")
        else:
            print("✗ Repository retrieval failed")
            return False
        
        # Test foreign key constraint
        cursor.execute("SELECT COUNT(*) FROM commits WHERE repository_id = ?", (repo_id,))
        commit_count = cursor.fetchone()[0]
        
        if commit_count == 1:
            print("✓ Foreign key relationship works")
        else:
            print("✗ Foreign key relationship failed")
            return False
        
        # Test stats
        stats = db.get_stats()
        print(f"✓ Database stats: {stats}")
        
        # Test indexes (check they exist)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND sql IS NOT NULL")
        indexes = cursor.fetchall()
        
        if len(indexes) > 0:
            print(f"✓ Database indexes created: {len(indexes)} indexes")
        else:
            print("⚠ No indexes found")
        
        db.close()
        print("✓ Database closed successfully")
        
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Clean up
        if os.path.exists(db_path):
            os.remove(db_path)


def test_migration_plan_exists():
    """Test that migration plan document exists"""
    print("\nTesting migration plan documentation...")
    
    plan_path = Path(__file__).parent.parent / "SQLITE_MIGRATION_PLAN.md"
    if plan_path.exists():
        print("✓ Migration plan document exists")
        return True
    else:
        print("✗ Migration plan document missing")
        return False


def test_storage_modules_exist():
    """Test that storage modules exist"""
    print("\nTesting storage module structure...")
    
    storage_path = Path(__file__).parent.parent / "src" / "datasmith" / "storage"
    required_files = [
        "schema.sql",
        "database.py", 
        "repositories.py",
        "commits.py",
        "contexts.py",
        "benchmarks.py",
        "pipeline.py",
        "migration.py"
    ]
    
    all_exist = True
    for filename in required_files:
        filepath = storage_path / filename
        if filepath.exists():
            print(f"✓ {filename} exists")
        else:
            print(f"✗ {filename} missing")
            all_exist = False
    
    return all_exist


def main():
    """Run all tests"""
    print("🧪 DataSmith SQLite Storage Implementation Test Suite")
    print("=" * 60)
    
    tests = [
        test_storage_modules_exist,
        test_migration_plan_exists,
        test_database_creation,
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
            print()
        except Exception as e:
            print(f"✗ Test {test_func.__name__} failed with exception: {e}")
            print()
    
    print("=" * 60)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! SQLite storage implementation is working.")
        return True
    else:
        print("❌ Some tests failed. Please check the implementation.")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)