"""
Database Schema for Job Task Report System
Defines the structure for enhanced job tracking with new fields
"""

import sqlite3
from pathlib import Path
from typing import Optional

# Database schema definition
JOB_TASK_SCHEMA = {
    "job_tasks": {
        "columns": {
            "job_id": {"type": "TEXT PRIMARY KEY", "description": "Auto-generated Job ID (YYMMDD_M_NNN)"},
            "created_by": {"type": "TEXT NOT NULL", "description": "User ID of creator"},
            "created_at": {"type": "TIMESTAMP NOT NULL", "description": "Creation timestamp"},
            "job_type": {"type": "TEXT NOT NULL", "description": "Maintenance/Repair/Inspection"},
            "job_class": {"type": "TEXT NOT NULL", "description": "Electrical/Mechanical/Civil/General"},
            "date_start": {"type": "DATE NOT NULL", "description": "Job start date"},
            "time_start": {"type": "TIME NOT NULL", "description": "Job start time"},
            "date_end": {"type": "DATE", "description": "Job end date"},
            "time_end": {"type": "TIME", "description": "Job end time"},
            "technician": {"type": "TEXT NOT NULL", "description": "Assigned technician from regdata"},
            "job_title": {"type": "TEXT NOT NULL", "description": "Job title (max 40 words)"},
            "job_details": {"type": "TEXT", "description": "Detailed job description (max 300 words)"},
            "remark": {"type": "TEXT", "description": "Additional remarks (max 100 words)"},
            "job_status": {"type": "TEXT NOT NULL", "description": "Pending/Inprogress/Completed"},
            "verify_by": {"type": "TEXT", "description": "Verified by user"},
            "images_before_paths": {"type": "TEXT", "description": "CSV of image paths (min 4)"},
            "images_after_paths": {"type": "TEXT", "description": "CSV of image paths (min 4)"},
            "last_modified": {"type": "TIMESTAMP", "description": "Last modification timestamp"},
            "last_modified_by": {"type": "TEXT", "description": "Last user to modify"},
        },
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_job_id ON job_tasks(job_id)",
            "CREATE INDEX IF NOT EXISTS idx_created_at ON job_tasks(created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_job_status ON job_tasks(job_status)",
            "CREATE INDEX IF NOT EXISTS idx_technician ON job_tasks(technician)",
        ]
    },
    "spare_parts": {
        "columns": {
            "spare_id": {"type": "INTEGER PRIMARY KEY AUTOINCREMENT", "description": "Unique ID"},
            "job_id": {"type": "TEXT NOT NULL", "description": "Reference to job_tasks"},
            "item_name": {"type": "TEXT NOT NULL", "description": "Name of spare part"},
            "quantity": {"type": "INTEGER NOT NULL", "description": "Quantity used"},
            "created_at": {"type": "TIMESTAMP NOT NULL", "description": "Creation timestamp"},
        },
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_spare_job_id ON spare_parts(job_id)",
        ]
    }
}

def init_database(db_path: Path) -> bool:
    """
    Initialize or update the database with the new schema
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Create job_tasks table
        job_tasks_cols = ", ".join([
            f"{col_name} {col_info['type']}"
            for col_name, col_info in JOB_TASK_SCHEMA["job_tasks"]["columns"].items()
        ])
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS job_tasks (
                {job_tasks_cols}
            )
        """)
        
        # Create spare_parts table
        spare_parts_cols = ", ".join([
            f"{col_name} {col_info['type']}"
            for col_name, col_info in JOB_TASK_SCHEMA["spare_parts"]["columns"].items()
        ])
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS spare_parts (
                {spare_parts_cols},
                FOREIGN KEY (job_id) REFERENCES job_tasks(job_id) ON DELETE CASCADE
            )
        """)
        
        # Create indexes
        for index_sql in JOB_TASK_SCHEMA["job_tasks"]["indexes"]:
            cursor.execute(index_sql)
        for index_sql in JOB_TASK_SCHEMA["spare_parts"]["indexes"]:
            cursor.execute(index_sql)
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error initializing database: {e}")
        return False


def get_schema_columns(table_name: str) -> dict:
    """Get column definitions for a specific table"""
    return JOB_TASK_SCHEMA.get(table_name, {}).get("columns", {})


def get_all_columns() -> list:
    """Get all column names from job_tasks table"""
    return list(JOB_TASK_SCHEMA["job_tasks"]["columns"].keys())


def validate_job_data(data: dict) -> tuple[bool, str]:
    """
    Validate job data before saving
    
    Args:
        data: Dictionary containing job data
        
    Returns:
        tuple: (is_valid, error_message)
    """
    # Required fields validation
    required_fields = ["job_type", "job_class", "date_start", "time_start", "technician", "job_title", "job_status"]
    for field in required_fields:
        if not data.get(field):
            return False, f"Missing required field: {field}"
    
    # Word count validations
    job_title_words = len(str(data.get("job_title", "")).split())
    if job_title_words > 40:
        return False, f"Job Title exceeds 40 words ({job_title_words})"
    
    job_details_words = len(str(data.get("job_details", "")).split())
    if job_details_words > 300:
        return False, f"Job Details exceeds 300 words ({job_details_words})"
    
    remark_words = len(str(data.get("remark", "")).split())
    if remark_words > 100:
        return False, f"Remark exceeds 100 words ({remark_words})"
    
    # Image validations
    images_before = str(data.get("images_before_paths", "")).split(",")
    images_before = [img.strip() for img in images_before if img.strip()]
    if len(images_before) < 4:
        return False, f"Minimum 4 'Before' images required ({len(images_before)} provided)"
    
    images_after = str(data.get("images_after_paths", "")).split(",")
    images_after = [img.strip() for img in images_after if img.strip()]
    if len(images_after) < 4:
        return False, f"Minimum 4 'After' images required ({len(images_after)} provided)"
    
    # Job status validation
    valid_statuses = ["Pending", "Inprogress", "Completed"]
    if data.get("job_status") not in valid_statuses:
        return False, f"Invalid Job Status: {data.get('job_status')}"
    
    # Job type and class validation
    valid_job_types = ["Maintenance", "Repair", "Inspection"]
    if data.get("job_type") not in valid_job_types:
        return False, f"Invalid Job Type: {data.get('job_type')}"
    
    valid_job_classes = ["Electrical", "Mechanical", "Civil", "General"]
    if data.get("job_class") not in valid_job_classes:
        return False, f"Invalid Job Class: {data.get('job_class')}"
    
    return True, "Validation passed"
