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
REMOTE_REGDATA_PATH = "databases/regdata.db"
REMOTE_IMAGES_PREFIX = "images"


# ======================================
# Initialize GCS Client
# ======================================
@st.cache_resource
def get_gcs_client():
    """Initialize and cache Google Cloud Storage client"""
    try:
        # Try to load from file first (local development)
        if GCP_KEY_PATH.exists():
            credentials = service_account.Credentials.from_service_account_file(
                str(GCP_KEY_PATH)
            )
            client = storage.Client(credentials=credentials)
            return client
        
        # Fall back to Streamlit secrets (Streamlit Cloud)
        try:
            import json
            secret_dict = st.secrets.get("gcp_service_account")
            if secret_dict:
                credentials = service_account.Credentials.from_service_account_info(secret_dict)
                client = storage.Client(credentials=credentials)
                return client
        except Exception:
            pass
        
        # If neither method works, show error
        st.error(f"❌ GCP credentials not found")
        st.error("Please add GCP service account to Streamlit secrets or config/gcp-key.json")
        st.stop()
        
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
        
        # Write bytes to temporary file and open with sqlite3
        temp_db_path = Path("/tmp/task_reports_temp.db")
        temp_db_path.write_bytes(db_bytes)
        
        # Open the database file
        conn = sqlite3.connect(str(temp_db_path))
        
        # Read data into DataFrame
        try:
            df = pd.read_sql_query('SELECT * FROM task_reports', conn)
            conn.close()
            temp_db_path.unlink(missing_ok=True)  # Clean up temp file
            return df
        except Exception:
            # Table might not exist yet
            conn.close()
            temp_db_path.unlink(missing_ok=True)  # Clean up temp file
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


def list_uploaded_data(prefix: str = "") -> list:
    """List uploaded objects in bucket for reporting/audit."""
    try:
        bucket = get_bucket()
        effective_prefix = prefix or ""
        blobs = bucket.list_blobs(prefix=effective_prefix)
        results = []
        for blob in blobs:
            if blob.name.endswith("/"):
                continue
            results.append(
                {
                    "Path": blob.name,
                    "Size (KB)": round((blob.size or 0) / 1024, 2),
                    "Updated": str(blob.updated) if blob.updated else "",
                    "Content Type": blob.content_type or "",
                }
            )
        return results
    except Exception as e:
        st.error(f"❌ Failed to list uploaded data: {e}")
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


# ======================================
# RegData Database Operations
# ======================================
def download_regdata():
    """Download regdata.db from Google Cloud Storage"""
    try:
        bucket = get_bucket()
        blob = bucket.blob(REMOTE_REGDATA_PATH)
        
        # Check if file exists
        if not blob.exists():
            return None
        
        # Download to bytes
        db_bytes = blob.download_as_bytes()
        
        # Write bytes to temporary file
        temp_regdata_path = Path("/tmp/regdata_temp.db")
        temp_regdata_path.write_bytes(db_bytes)
        
        return temp_regdata_path
        
    except Exception as e:
        st.error(f"❌ Failed to download regdata: {e}")
        return None


def upload_regdata(local_path: Path) -> bool:
    """Upload regdata.db to Google Cloud Storage"""
    try:
        if not local_path.exists():
            st.error(f"❌ regdata.db not found at {local_path}")
            return False
        
        bucket = get_bucket()
        blob = bucket.blob(REMOTE_REGDATA_PATH)
        
        # Upload file
        blob.upload_from_filename(str(local_path))
        st.success("✅ regdata.db uploaded to Google Cloud!")
        return True
        
    except Exception as e:
        st.error(f"❌ Failed to upload regdata: {e}")
        return False


def sync_regdata_to_gcs(local_path: Path) -> bool:
    """Sync local regdata.db to Google Cloud Storage"""
    try:
        if not local_path.exists():
            return False
        
        # Upload to GCS
        bucket = get_bucket()
        blob = bucket.blob(REMOTE_REGDATA_PATH)
        blob.upload_from_filename(str(local_path))
        
        return True
    except Exception:
        return False


def sync_regdata_from_gcs(local_path: Path) -> bool:
    """Sync regdata.db from Google Cloud Storage to local"""
    try:
        bucket = get_bucket()
        blob = bucket.blob(REMOTE_REGDATA_PATH)
        
        if not blob.exists():
            return False
        
        # Download and save
        db_bytes = blob.download_as_bytes()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(db_bytes)
        
        return True
    except Exception:
        return False


# ======================================
# Job Tasks Database Operations (New Schema)
# ======================================
def download_job_tasks_database() -> pd.DataFrame:
    """Download job tasks from database"""
    try:
        bucket = get_bucket()
        blob = bucket.blob(REMOTE_DB_PATH)
        
        if not blob.exists():
            return pd.DataFrame()
        
        # Download and read
        db_bytes = blob.download_as_bytes()
        temp_db_path = Path("/tmp/job_tasks_temp.db")
        temp_db_path.write_bytes(db_bytes)
        
        conn = sqlite3.connect(str(temp_db_path))
        
        try:
            df = pd.read_sql_query('SELECT * FROM job_tasks', conn)
            conn.close()
            temp_db_path.unlink(missing_ok=True)
            return df
        except Exception:
            conn.close()
            temp_db_path.unlink(missing_ok=True)
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"❌ Failed to download job tasks: {e}")
        return pd.DataFrame()


def save_job_task(job_data: dict, spare_parts: list = None) -> bool:
    """
    Save a new job task and its spare parts to database
    
    Args:
        job_data: Dictionary with job information
        spare_parts: List of spare parts (each as dict with item_name and quantity)
    
    Returns:
        bool: True if successful
    """
    try:
        # Download existing data
        df_jobs = download_job_tasks_database()
        
        # Convert job_data to DataFrame
        new_job_df = pd.DataFrame([job_data])
        
        # Append to existing
        if df_jobs.empty:
            df_jobs = new_job_df
        else:
            df_jobs = pd.concat([df_jobs, new_job_df], ignore_index=True)
        
        # Upload to GCS
        bucket = get_bucket()
        blob = bucket.blob(REMOTE_DB_PATH)
        
        temp_db_path = Path("/tmp/job_tasks_temp.db")
        conn = sqlite3.connect(str(temp_db_path))
        
        # Create job_tasks table
        df_jobs.to_sql('job_tasks', conn, if_exists='replace', index=False)
        
        # Save spare parts if provided
        if spare_parts:
            spare_parts_data = []
            for spare in spare_parts:
                spare_record = {
                    "job_id": job_data.get("job_id"),
                    "item_name": spare.get("item_name"),
                    "quantity": spare.get("quantity"),
                    "created_at": job_data.get("created_at")
                }
                spare_parts_data.append(spare_record)
            
            if spare_parts_data:
                df_spare = pd.DataFrame(spare_parts_data)
                df_spare.to_sql('spare_parts', conn, if_exists='append', index=False)
        
        conn.commit()
        conn.close()
        
        # Upload to GCS
        blob.upload_from_filename(str(temp_db_path))
        temp_db_path.unlink(missing_ok=True)
        
        return True
        
    except Exception as e:
        st.error(f"❌ Failed to save job task: {e}")
        return False


def get_spare_parts_for_job(job_id: str) -> list:
    """Get all spare parts for a specific job"""
    try:
        bucket = get_bucket()
        blob = bucket.blob(REMOTE_DB_PATH)
        
        if not blob.exists():
            return []
        
        db_bytes = blob.download_as_bytes()
        temp_db_path = Path("/tmp/spare_parts_temp.db")
        temp_db_path.write_bytes(db_bytes)
        
        conn = sqlite3.connect(str(temp_db_path))
        
        try:
            query = f"SELECT * FROM spare_parts WHERE job_id = '{job_id}'"
            df = pd.read_sql_query(query, conn)
            conn.close()
            temp_db_path.unlink(missing_ok=True)
            return df.to_dict('records') if not df.empty else []
        except Exception:
            conn.close()
            temp_db_path.unlink(missing_ok=True)
            return []
            
    except Exception as e:
        st.error(f"❌ Failed to get spare parts: {e}")
        return []


def get_job_task_by_id(job_id: str) -> dict:
    """Get a specific job task by ID"""
    try:
        df = download_job_tasks_database()
        if df.empty:
            return {}
        
        job = df[df['job_id'] == job_id]
        if job.empty:
            return {}
        
        return job.iloc[0].to_dict()
        
    except Exception as e:
        st.error(f"❌ Failed to get job task: {e}")
        return {}


def update_job_task_status(job_id: str, new_status: str, verify_by: str = "") -> bool:
    """Update the status of a job task"""
    try:
        df = download_job_tasks_database()
        if df.empty:
            return False
        
        # Update status
        df.loc[df['job_id'] == job_id, 'job_status'] = new_status
        
        if verify_by:
            df.loc[df['job_id'] == job_id, 'verify_by'] = verify_by
        
        # Update modification timestamp
        from utils import now_sg
        df.loc[df['job_id'] == job_id, 'last_modified'] = now_sg()
        
        # Save back to GCS
        bucket = get_bucket()
        blob = bucket.blob(REMOTE_DB_PATH)
        
        temp_db_path = Path("/tmp/job_tasks_temp.db")
        conn = sqlite3.connect(str(temp_db_path))
        
        df.to_sql('job_tasks', conn, if_exists='replace', index=False)
        conn.close()
        
        blob.upload_from_filename(str(temp_db_path))
        temp_db_path.unlink(missing_ok=True)
        
        return True
        
    except Exception as e:
        st.error(f"❌ Failed to update job task: {e}")
        return False


def get_jobs_by_status(status: str) -> pd.DataFrame:
    """Get all jobs with a specific status"""
    try:
        df = download_job_tasks_database()
        if df.empty:
            return pd.DataFrame()
        
        return df[df['job_status'] == status]
        
    except Exception as e:
        st.error(f"❌ Failed to get jobs by status: {e}")
        return pd.DataFrame()


def get_jobs_by_technician(technician: str) -> pd.DataFrame:
    """Get all jobs assigned to a specific technician"""
    try:
        df = download_job_tasks_database()
        if df.empty:
            return pd.DataFrame()
        
        return df[df['technician'] == technician]
        
    except Exception as e:
        st.error(f"❌ Failed to get jobs by technician: {e}")
        return pd.DataFrame()
