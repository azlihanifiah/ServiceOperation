"""
Google Cloud Storage helper functions
Handles database and image uploads/downloads
"""

import os
import io
import sqlite3
import pandas as pd
from pathlib import Path
from google.cloud import storage
from google.oauth2 import service_account
import streamlit as st


# ======================================
# Configuration
# ======================================
PROJECT_ROOT = Path(__file__).resolve().parent
CONFIG_DIR = PROJECT_ROOT / "config"
GCP_KEY_PATH = CONFIG_DIR / "gcp-key.json"

# Google Cloud Storage bucket name
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME", "ammar-builders-maintenance")

# Remote paths in GCS
REMOTE_DB_PATH = "databases/task_reports.db"
REMOTE_IMAGES_PREFIX = "images"


# ======================================
# Initialize GCS Client
# ======================================
@st.cache_resource
def get_gcs_client():
    """Initialize and cache Google Cloud Storage client"""
    try:
        if not GCP_KEY_PATH.exists():
            st.error(f"❌ GCP key not found at {GCP_KEY_PATH}")
            st.stop()
        
        credentials = service_account.Credentials.from_service_account_file(
            str(GCP_KEY_PATH)
        )
        client = storage.Client(credentials=credentials)
        return client
    except Exception as e:
        st.error(f"❌ Failed to initialize GCS client: {e}")
        st.stop()


def get_bucket():
    """Get the GCS bucket"""
    client = get_gcs_client()
    return client.bucket(BUCKET_NAME)


# ======================================
# Database Operations
# ======================================
def download_database():
    """Download database from Google Cloud Storage to memory"""
    try:
        bucket = get_bucket()
        blob = bucket.blob(REMOTE_DB_PATH)
        
        # Check if file exists
        if not blob.exists():
            # Return empty dataframe if DB doesn't exist yet
            return pd.DataFrame()
        
        # Download to bytes
        db_bytes = blob.download_as_bytes()
        
        # Load into SQLite in memory
        conn = sqlite3.connect(":memory:")
        conn.executescript(db_bytes.decode() if isinstance(db_bytes, bytes) else db_bytes)
        
        # Read data into DataFrame
        try:
            df = pd.read_sql_query('SELECT * FROM task_reports', conn)
            conn.close()
            return df
        except Exception:
            # Table might not exist yet
            conn.close()
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"❌ Failed to download database: {e}")
        return pd.DataFrame()


def upload_database(df: pd.DataFrame) -> bool:
    """Upload database to Google Cloud Storage"""
    try:
        bucket = get_bucket()
        blob = bucket.blob(REMOTE_DB_PATH)
        
        # Create temporary SQLite database file
        temp_db_path = Path("/tmp/task_reports_temp.db")
        
        # Write dataframe to SQLite
        conn = sqlite3.connect(str(temp_db_path))
        df.to_sql('task_reports', conn, if_exists='replace', index=False)
        conn.close()
        
        # Upload to GCS
        blob.upload_from_filename(str(temp_db_path))
        
        # Clean up temp file
        temp_db_path.unlink(missing_ok=True)
        
        st.success("✅ Database saved to Google Cloud!")
        return True
        
    except Exception as e:
        st.error(f"❌ Failed to upload database: {e}")
        return False


# ======================================
# Image Operations
# ======================================
def upload_image(image_bytes, job_id: str, image_type: str, filename: str) -> str:
    """Upload image to Google Cloud Storage and return public URL"""
    try:
        bucket = get_bucket()
        
        # Create remote path
        remote_filename = f"{job_id}_{image_type}_{filename}"
        blob = bucket.blob(f"{REMOTE_IMAGES_PREFIX}/{remote_filename}")
        
        # Upload image
        blob.upload_from_string(image_bytes)
        
        # Return the storage path
        return f"{REMOTE_IMAGES_PREFIX}/{remote_filename}"
        
    except Exception as e:
        st.error(f"❌ Failed to upload image: {e}")
        return ""


def download_image(image_path: str) -> bytes:
    """Download image from Google Cloud Storage"""
    try:
        bucket = get_bucket()
        blob = bucket.blob(image_path)
        return blob.download_as_bytes()
    except Exception as e:
        st.error(f"❌ Failed to download image: {e}")
        return b""


def list_images_for_job(job_id: str) -> list:
    """List all images for a specific job"""
    try:
        bucket = get_bucket()
        blobs = bucket.list_blobs(prefix=f"{REMOTE_IMAGES_PREFIX}/{job_id}")
        return [blob.name for blob in blobs]
    except Exception as e:
        st.error(f"❌ Failed to list images: {e}")
        return []


# ======================================
# Backup Operations
# ======================================
def create_backup() -> bool:
    """Create a timestamped backup of the database"""
    try:
        from datetime import datetime
        
        bucket = get_bucket()
        blob = bucket.blob(REMOTE_DB_PATH)
        
        # Download current database
        current_db = blob.download_as_bytes()
        
        # Create backup with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_blob = bucket.blob(f"backups/task_reports_backup_{timestamp}.db")
        backup_blob.upload_from_string(current_db)
        
        st.success(f"✅ Backup created: task_reports_backup_{timestamp}.db")
        return True
        
    except Exception as e:
        st.error(f"❌ Failed to create backup: {e}")
        return False


def list_backups() -> list:
    """List all database backups"""
    try:
        bucket = get_bucket()
        blobs = bucket.list_blobs(prefix="backups/")
        return sorted([blob.name for blob in blobs], reverse=True)
    except Exception as e:
        st.error(f"❌ Failed to list backups: {e}")
        return []


def restore_from_backup(backup_name: str) -> bool:
    """Restore database from a backup"""
    try:
        bucket = get_bucket()
        backup_blob = bucket.blob(backup_name)
        
        # Download backup
        backup_db = backup_blob.download_as_bytes()
        
        # Overwrite current database
        current_blob = bucket.blob(REMOTE_DB_PATH)
        current_blob.upload_from_string(backup_db)
        
        st.success(f"✅ Database restored from {backup_name}")
        return True
        
    except Exception as e:
        st.error(f"❌ Failed to restore from backup: {e}")
        return False


# ======================================
# Status Check
# ======================================
def check_gcs_connection() -> bool:
    """Test GCS connection"""
    try:
        client = get_gcs_client()
        bucket = client.bucket(BUCKET_NAME)
        bucket.reload()
        return True
    except Exception as e:
        st.error(f"❌ GCS Connection Error: {e}")
        return False
