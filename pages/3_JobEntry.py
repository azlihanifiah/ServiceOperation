import streamlit as st
import pandas as pd
from datetime import datetime, date, time
from pathlib import Path
from utils import (
    hide_default_sidebar_navigation,
    require_login,
    render_role_navigation,
    now_sg,
    today_sg,
    format_ts_sg,
    lookup_user_in_regdata,
)
from gcp_storage import (
    download_job_tasks_database,
    save_job_task,
    upload_image,
)
from database_schema import validate_job_data, init_database
import uuid

st.set_page_config(page_title="Job Entry", page_icon="📝", layout="wide")
hide_default_sidebar_navigation()

# Require Technician level access (rank 2+)
auth = require_login(min_level_rank=2)
render_role_navigation(auth)

st.title("📝 Job Task Entry Form")
st.markdown("#### Create new job task report with complete details")
st.markdown("---")

# ======================================
# HELPER FUNCTIONS
# ======================================

def _current_user() -> str:
    """Get current logged-in user"""
    name = str(auth.get("name", "") or "").strip()
    user_id = str(auth.get("user_id", "") or "").strip()
    return name or user_id or "System"

def _get_technician_list() -> list:
    """Get list of technicians from regdata"""
    try:
        # Try to load from local regdata.db
        import sqlite3
        db_path = Path(__file__).parent / "data" / "regdata.db"
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            cursor.execute("SELECT display_name, user_id FROM users WHERE level_rank >= 1")
            results = cursor.fetchall()
            conn.close()
            return [f"{name} ({uid})" for name, uid in results] if results else ["No technicians found"]
    except:
        pass
    return ["Technician 1", "Technician 2", "Technician 3"]

def _generate_job_id() -> str:
    """Generate unique Job ID"""
    date_str = today_sg().strftime("%y%m%d")
    prefix = f"{date_str}_J_"
    unique_suffix = str(uuid.uuid4())[:8].upper()
    return f"{prefix}{unique_suffix}"

def _count_words(text: str) -> int:
    """Count words in text"""
    return len(str(text).strip().split()) if text else 0

def _upload_images(uploaded_files, job_id: str, image_type: str) -> tuple[list, int]:
    """Upload images to GCS and return paths and count"""
    if not uploaded_files:
        return [], 0
    
    saved_paths = []
    for i, uploaded_file in enumerate(uploaded_files):
        try:
            image_bytes = uploaded_file.getbuffer().tobytes()
            ext = Path(uploaded_file.name).suffix.lower()
            filename = f"{image_type}_{i+1}{ext}"
            path = upload_image(image_bytes, job_id, image_type, filename)
            if path:
                saved_paths.append(path)
        except Exception as e:
            st.error(f"❌ Failed to upload image: {uploaded_file.name}")
            continue
    
    return saved_paths, len(saved_paths)

# ======================================
# SESSION STATE INITIALIZATION
# ======================================
if 'spare_parts' not in st.session_state:
    st.session_state.spare_parts = []

if 'images_before' not in st.session_state:
    st.session_state.images_before = []

if 'images_after' not in st.session_state:
    st.session_state.images_after = []

# ======================================
# FORM START
# ======================================
form = st.form("job_entry_form")

# ======================================
# SECTION 1: AUTO-GENERATED FIELDS
# ======================================
form.markdown("### 🆔 Auto-Generated Information")
col1, col2, col3 = form.columns(3)

with col1:
    job_id = _generate_job_id()
    form.text_input("Job ID", value=job_id, disabled=True, help="Auto-generated unique identifier")

with col2:
    created_by = _current_user()
    form.text_input("Created By", value=created_by, disabled=True, help="Auto-filled from login")

with col3:
    created_at = format_ts_sg()
    form.text_input("Created At", value=created_at, disabled=True, help="Auto-generated timestamp")

form.markdown("---")

# ======================================
# SECTION 2: JOB TYPE & CLASS
# ======================================
form.markdown("### 📋 Job Classification")
col1, col2, col3, col4 = form.columns(4)

with col1:
    job_type = form.selectbox(
        "Job Type *",
        options=["", "Maintenance", "Repair", "Inspection"],
        help="Type of job to be performed"
    )

with col2:
    job_class = form.selectbox(
        "Job Class *",
        options=["", "Electrical", "Mechanical", "Civil", "General"],
        help="Classification of job"
    )

with col3:
    date_start = form.date_input(
        "Date Start *",
        value=today_sg(),
        help="Job start date"
    )

with col4:
    time_start = form.time_input(
        "Time Start *",
        value=datetime.now().time(),
        help="Job start time"
    )

form.markdown("---")

# ======================================
# SECTION 3: JOB DURATION
# ======================================
form.markdown("### ⏰ Job Duration")
col1, col2, col3, col4 = form.columns(4)

with col1:
    date_end = form.date_input(
        "Date End",
        value=date_start,
        help="Job end date (optional)"
    )

with col2:
    time_end = form.time_input(
        "Time End",
        value=datetime.now().time(),
        help="Job end time (optional)"
    )

with col3:
    form.markdown("")
    form.markdown("")
    form.info("⏱️ Leave empty if job not yet completed")

form.markdown("---")

# ======================================
# SECTION 4: PERSONNEL
# ======================================
form.markdown("### 👤 Personnel Assignment")
col1, col2 = form.columns(2)

with col1:
    technician_list = _get_technician_list()
    technician = form.selectbox(
        "Assigned Technician *",
        options=[""] + technician_list,
        help="Select technician from registered users"
    )

with col2:
    verify_by = form.text_input(
        "Verify By",
        placeholder="Name of verifier",
        help="Optional: Person who will verify the job"
    )

form.markdown("---")

# ======================================
# SECTION 5: JOB DESCRIPTION
# ======================================
form.markdown("### 📝 Job Description")

job_title = form.text_area(
    "Job Title *",
    max_chars=None,
    placeholder="Enter job title (max 40 words)",
    height=60,
    help="Brief title of the job"
)
title_word_count = _count_words(job_title)
form.caption(f"Word count: {title_word_count}/40")
if title_word_count > 40:
    form.error("❌ Job Title exceeds 40 words")

job_details = form.text_area(
    "Job Details *",
    max_chars=None,
    placeholder="Enter detailed job description (max 300 words)",
    height=120,
    help="Detailed description of work performed"
)
details_word_count = _count_words(job_details)
form.caption(f"Word count: {details_word_count}/300")
if details_word_count > 300:
    form.error("❌ Job Details exceeds 300 words")

remark = form.text_area(
    "Remarks",
    max_chars=None,
    placeholder="Additional remarks (max 100 words)",
    height=80,
    help="Any additional notes or observations"
)
remark_word_count = _count_words(remark)
form.caption(f"Word count: {remark_word_count}/100")
if remark_word_count > 100:
    form.error("❌ Remarks exceeds 100 words")

form.markdown("---")

# ======================================
# SECTION 6: JOB STATUS
# ======================================
form.markdown("### ✅ Job Status")
col1, col2 = form.columns(2)

with col1:
    job_status = form.selectbox(
        "Job Status *",
        options=["", "Pending", "Inprogress", "Completed"],
        help="Current status of the job"
    )

form.markdown("---")

# ======================================
# SECTION 7: SPARE PARTS
# ======================================
form.markdown("### 🔧 Spare Parts Used")
form.info("Add spare parts used during the job")

spare_col1, spare_col2, spare_col3 = form.columns([2, 1, 1])

with spare_col1:
    spare_item = form.text_input("Item Name", key=f"spare_item_{len(st.session_state.spare_parts)}")

with spare_col2:
    spare_qty = form.number_input(
        "Quantity",
        min_value=1,
        value=1,
        key=f"spare_qty_{len(st.session_state.spare_parts)}"
    )

with spare_col3:
    form.markdown("")
    form.markdown("")
    if form.button("➕ Add Item", key=f"add_spare_{len(st.session_state.spare_parts)}"):
        if spare_item.strip():
            st.session_state.spare_parts.append({
                "item_name": spare_item,
                "quantity": spare_qty
            })

# Display current spare parts
if st.session_state.spare_parts:
    form.markdown("**Current Spare Parts:**")
    spare_parts_df = pd.DataFrame(st.session_state.spare_parts)
    form.dataframe(
        spare_parts_df,
        use_container_width=True,
        hide_index=True
    )
    
    if form.button("🗑️ Clear All Spare Parts"):
        st.session_state.spare_parts = []
        st.rerun()

form.markdown("---")

# ======================================
# SECTION 8: IMAGES - BEFORE
# ======================================
form.markdown("### 📸 Before Images (Minimum 4)")
before_images = form.file_uploader(
    "Upload 'Before' images",
    type=['jpg', 'jpeg', 'png', 'gif', 'webp'],
    accept_multiple_files=True,
    key="before_images",
    help="Upload at least 4 'before' images (JPEG, PNG, GIF, WebP)"
)
form.caption(f"Images uploaded: {len(before_images) if before_images else 0}/4 (minimum)")

form.markdown("---")

# ======================================
# SECTION 9: IMAGES - AFTER
# ======================================
form.markdown("### 📸 After Images (Minimum 4)")
after_images = form.file_uploader(
    "Upload 'After' images",
    type=['jpg', 'jpeg', 'png', 'gif', 'webp'],
    accept_multiple_files=True,
    key="after_images",
    help="Upload at least 4 'after' images (JPEG, PNG, GIF, WebP)"
)
form.caption(f"Images uploaded: {len(after_images) if after_images else 0}/4 (minimum)")

form.markdown("---")

# ======================================
# SUBMIT & SAVE BUTTONS
# ======================================
st.markdown("### 🎯 Submit Your Report")
col1, col2, col3 = form.columns(3)

with col1:
    submit_button = form.form_submit_button(
        label="✅ Submit",
        use_container_width=True,
        help="Submit and save the job report to database"
    )

with col2:
    save_button = form.form_submit_button(
        label="💾 Save (Draft)",
        use_container_width=True,
        help="Save as draft (validation not enforced)"
    )

with col3:
    form.markdown("")

# ======================================
# FORM PROCESSING
# ======================================
if submit_button or save_button:
    # Prepare data dictionary
    job_data = {
        "job_id": job_id,
        "created_by": auth.get("user_id", "system"),
        "created_at": now_sg(),
        "job_type": job_type,
        "job_class": job_class,
        "date_start": date_start,
        "time_start": time_start,
        "date_end": date_end if date_end else None,
        "time_end": time_end if time_end else None,
        "technician": technician,
        "job_title": job_title,
        "job_details": job_details,
        "remark": remark,
        "job_status": job_status,
        "verify_by": verify_by,
        "images_before_paths": [],
        "images_after_paths": [],
        "last_modified": now_sg(),
        "last_modified_by": auth.get("user_id", "system"),
    }

    # Validation for Submit button
    is_valid = True
    error_messages = []
    
    if submit_button:
        is_valid, val_msg = validate_job_data(job_data)
        if not is_valid:
            error_messages.append(val_msg)
    else:
        # Draft mode - only check required fields
        if not job_type:
            error_messages.append("Job Type is required")
        if not job_class:
            error_messages.append("Job Class is required")
        if not technician:
            error_messages.append("Technician is required")
        if not job_title:
            error_messages.append("Job Title is required")
        if not job_status:
            error_messages.append("Job Status is required")

    # Handle errors
    if error_messages:
        for error_msg in error_messages:
            st.error(f"❌ {error_msg}")
    else:
        # Upload images
        with st.spinner("⏳ Uploading images..."):
            if before_images:
                before_paths, count_before = _upload_images(before_images, job_id, "before")
                job_data["images_before_paths"] = ",".join(before_paths)
            
            if after_images:
                after_paths, count_after = _upload_images(after_images, job_id, "after")
                job_data["images_after_paths"] = ",".join(after_paths)
        
        # Save job task
        try:
            with st.spinner("💾 Saving to database..."):
                success = save_job_task(job_data, st.session_state.spare_parts if st.session_state.spare_parts else None)
            
            if success:
                if submit_button:
                    st.success(f"✅ Job Report Submitted Successfully!\n\nJob ID: **{job_id}**")
                else:
                    st.success(f"✅ Job Report Saved as Draft!\n\nJob ID: **{job_id}**")
                
                st.info("📊 The report has been uploaded to Google Cloud Storage and is now accessible.")
                
                # Show summary
                st.markdown("### 📋 Report Summary")
                summary_data = {
                    "Job ID": job_id,
                    "Job Type": job_type,
                    "Job Class": job_class,
                    "Technician": technician,
                    "Status": job_status,
                    "Images Before": len(before_images) if before_images else 0,
                    "Images After": len(after_images) if after_images else 0,
                    "Spare Parts": len(st.session_state.spare_parts),
                }
                st.dataframe(
                    pd.DataFrame([summary_data]),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.error("❌ Failed to save report. Please try again.")
        
        except Exception as e:
            st.error(f"❌ Error saving report: {e}")
            st.info("Please check your internet connection and Google Cloud Storage configuration.")
