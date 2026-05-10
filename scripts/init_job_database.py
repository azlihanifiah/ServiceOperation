"""
Database Initialization and Setup Script
Run this to initialize the new database schema for job tracking
"""

import sqlite3
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from database_schema import JOB_TASK_SCHEMA, init_database
from gcp_storage import upload_database, get_bucket
import pandas as pd

def init_local_database(db_path: Path = None) -> bool:
    """Initialize local SQLite database with new schema"""
    if db_path is None:
        db_path = Path(__file__).parent / "data" / "job_tasks.db"
    
    # Create data directory if it doesn't exist
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"📊 Initializing database at: {db_path}")
    
    try:
        success = init_database(db_path)
        if success:
            print("✅ Database initialized successfully!")
            print(f"   - job_tasks table created")
            print(f"   - spare_parts table created")
            print(f"   - Indexes created")
            return True
        else:
            print("❌ Failed to initialize database")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def verify_database_schema(db_path: Path) -> bool:
    """Verify database schema is correctly created"""
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Check job_tasks table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='job_tasks'")
        if not cursor.fetchone():
            print("❌ job_tasks table not found")
            return False
        
        # Check spare_parts table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='spare_parts'")
        if not cursor.fetchone():
            print("❌ spare_parts table not found")
            return False
        
        # Check columns
        cursor.execute("PRAGMA table_info(job_tasks)")
        columns = [col[1] for col in cursor.fetchall()]
        
        expected_columns = list(JOB_TASK_SCHEMA["job_tasks"]["columns"].keys())
        for col in expected_columns:
            if col not in columns:
                print(f"❌ Missing column: {col}")
                return False
        
        conn.close()
        print("✅ Database schema verified successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Verification error: {e}")
        return False


def backup_existing_data(db_path: Path) -> bool:
    """Create a backup of existing database before migration"""
    try:
        if not db_path.exists():
            print("ℹ️  No existing database to backup")
            return True
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = db_path.parent / f"backup_job_tasks_{timestamp}.db"
        
        # Copy file
        import shutil
        shutil.copy2(db_path, backup_path)
        print(f"✅ Backup created: {backup_path}")
        return True
        
    except Exception as e:
        print(f"❌ Backup error: {e}")
        return False


def push_to_gcs():
    """Push initialized database to Google Cloud Storage"""
    print("\n📤 Pushing database to Google Cloud Storage...")
    
    try:
        # Create empty dataframe with schema
        data_dict = {col: [] for col in JOB_TASK_SCHEMA["job_tasks"]["columns"].keys()}
        df = pd.DataFrame(data_dict)
        
        # Upload
        from gcp_storage import upload_database
        success = upload_database(df)
        
        if success:
            print("✅ Database successfully pushed to Google Cloud Storage!")
            return True
        else:
            print("❌ Failed to push to GCS")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def print_schema():
    """Print the database schema"""
    print("\n📋 DATABASE SCHEMA")
    print("=" * 60)
    
    for table_name, table_info in JOB_TASK_SCHEMA.items():
        print(f"\n📊 Table: {table_name}")
        print("-" * 60)
        
        for col_name, col_info in table_info["columns"].items():
            col_type = col_info["type"]
            description = col_info["description"]
            print(f"  {col_name:<25} {col_type:<30} {description}")


def main():
    """Main setup wizard"""
    print("\n" + "=" * 60)
    print("🛠️  JOB TASK REPORT DATABASE SETUP WIZARD")
    print("=" * 60)
    
    print("\nThis wizard will help you initialize the new database schema")
    print("for the enhanced job tracking system.\n")
    
    # Show schema
    show_schema = input("📋 Display database schema? (y/n): ").strip().lower() == 'y'
    if show_schema:
        print_schema()
    
    # Initialize local database
    print("\n📊 Step 1: Initialize Local Database")
    print("-" * 60)
    
    db_path = Path(__file__).parent / "data" / "job_tasks.db"
    print(f"Database will be created at: {db_path}")
    
    # Backup if exists
    if db_path.exists():
        print("\n⚠️  Existing database found!")
        backup = input("Create backup before proceeding? (y/n): ").strip().lower() == 'y'
        if backup:
            if not backup_existing_data(db_path):
                print("Failed to create backup. Aborting...")
                return False
    
    # Initialize
    if not init_local_database(db_path):
        print("\n❌ Failed to initialize local database. Please try again.")
        return False
    
    # Verify
    print("\n✔️  Verifying schema...")
    if not verify_database_schema(db_path):
        print("\n❌ Schema verification failed. Please check your database.")
        return False
    
    # GCS push
    print("\n📤 Step 2: Push to Google Cloud Storage")
    print("-" * 60)
    
    push_gcs = input("Push database to Google Cloud Storage? (y/n): ").strip().lower() == 'y'
    if push_gcs:
        if not push_to_gcs():
            print("Note: You can push later using the Streamlit app.")
    
    print("\n" + "=" * 60)
    print("✅ SETUP COMPLETE!")
    print("=" * 60)
    print("\n📝 Next steps:")
    print("1. Run: streamlit run Home.py")
    print("2. Navigate to the 'Job Entry' page")
    print("3. Start creating job task reports!")
    print("\n💾 Your data will be automatically synced with Google Cloud Storage\n")
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
