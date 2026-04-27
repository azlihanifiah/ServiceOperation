import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime, date, time
from utils import (
    now_sg,
    today_sg,
    format_ts_sg,
    require_login,
    show_user_error,
    render_role_navigation,
)
from gcp_storage import (
    download_database,
    upload_database,
    upload_image,
)

st.set_page_config(page_title="TaskUpdate page", page_icon="🔧", layout="wide")
auth = require_login(min_level_rank=2)
render_role_navigation(auth)

# Database and images now stored in Google Cloud Storage
# Local paths kept for backwards compatibility (deprecated)
# APP_ROOT = Path(__file__).resolve().parents[1]
# DATA_DIR = APP_ROOT / "data"
# IMAGES_DIR = DATA_DIR / "task_images"
# DB_PATH = DATA_DIR / "task_reports.db"

# Define database columns
DB_COLUMNS = [
    "Job Type",
    "Create By",
    "Create at",
    "Date",
    "Job ID",
    "Severity",
    "Priority",
    "Maintenance Frequency",
    "Shift",
    "Location",
    "Job Status",
    "Assign by",
    "Date Start",
    "Time Start",
    "Machine ID",
    "Date End",
    "Time End",
    "Machine/Equipment",
    "Task Description",
    "Action",
    "Remark",
    "Verify by",
    "Spare Parts Used",
    "Before Images",
    "After Images"
]

def _performed_by_label() -> str:
    name = str(auth.get("name", "") or "").strip()
    user_id = str(auth.get("user_id", "") or "").strip()
    return name or user_id or "System"

def _current_user_display_name() -> str:
    return _performed_by_label()

def save_images(images, job_id: str, image_type: str) -> str:
    """Upload images to Google Cloud Storage and return comma-separated list of paths"""
    if not images:
        return ""

    saved_paths = []
    for i, img in enumerate(images):
        # Get file extension
        ext = Path(img.name).suffix.lower()
        filename = f"{i+1}{ext}"
        
        # Upload to GCS
        image_bytes = img.getbuffer()
        path = upload_image(image_bytes, job_id, image_type, filename)
        
        if path:
            saved_paths.append(path)

    return ",".join(saved_paths)

def generate_job_id(entry_date: date) -> str:
    date_str = entry_date.strftime("%y%m%d")
    prefix = f"{date_str}_M_"
    
    # Download current database from cloud
    try:
        df = download_database()
        if df is not None and not df.empty and "Job ID" in df.columns:
            existing_ids = [str(x).strip() for x in df["Job ID"] if x]
            
            max_n = 0
            for jid in existing_ids:
                if jid.startswith(prefix):
                    tail = jid[len(prefix):].strip()
                    try:
                        max_n = max(max_n, int(tail))
                    except Exception:
                        continue
            return f"{prefix}{(max_n + 1):03d}"
    except Exception:
        pass
    
    return f"{prefix}001"

def load_task_data() -> pd.DataFrame:
    try:
        df = download_database()
        if df is not None and not df.empty:
            return df
    except Exception as e:
        show_user_error(f"Failed to load data: {e}")
    
    return pd.DataFrame(columns=DB_COLUMNS)

def save_task_data(df: pd.DataFrame) -> bool:
    try:
        return upload_database(df)
    except Exception as e:
        show_user_error(f"Failed to save data: {e}")
        return False

st.title("TaskUpdate page")
st.markdown("### New Task")

if "task_form" not in st.session_state:
    st.session_state.task_form = {
        "date": today_sg(),
        "shift": "",
        "location": "",
        "job_status": "",
        "assign_by": "",
        "start_date": today_sg(),
        "time_start": now_sg().time().replace(microsecond=0),
        "end_date": today_sg(),
        "time_end": now_sg().time().replace(microsecond=0),
        "machine_id": "",
        "machine_equipment": "",
        "severity": "",
        "priority": "",
        "maintenance_frequency": "",
        "task_description": "",
        "action": "",
        "remark": "",
        "verify_by": "",
        "spare_parts_used": "",
        "before_images": [],
        "after_images": []
    }

# Job Type row (fixed to Maintenance)
col_jt, col_cb, col_ca = st.columns(3)
with col_jt:
    st.write("**Job Type:** Maintenance")
with col_cb:
    st.write(f"**Create By:** {_current_user_display_name()}")
with col_ca:
    st.write(f"**Create at:** {format_ts_sg()}")

# Date, Job ID, Severity, Priority, Maintenance Frequency row
c_date, c_jid, c_sev, c_pri, c_mf = st.columns(5)
with c_date:
    entry_date = st.date_input("Date *", value=st.session_state.task_form["date"], key="date")
    st.session_state.task_form["date"] = entry_date
with c_jid:
    job_id = generate_job_id(entry_date)
    st.text_input("Job ID (auto)", value=job_id, disabled=True, key="job_id_display")
with c_sev:
    severity = st.selectbox("Severity *", options=[""] + ["Low", "Medium", "High", "Critical"], key="severity")
    st.session_state.task_form["severity"] = severity
with c_pri:
    priority = st.selectbox("Priority *", options=[""] + ["Low", "High"], key="priority")
    st.session_state.task_form["priority"] = priority
with c_mf:
    maintenance_frequency = st.selectbox(
        "Maintenance Frequency *",
        options=[""] + ["NONE", "Twice a Day", "Daily", "Every Two Days", "Weekly", "BiWeekly", "Fortnight", "Monthly", "Quaterly", "Biannual", "Yearly"],
        key="maintenance_frequency"
    )
    st.session_state.task_form["maintenance_frequency"] = maintenance_frequency

# Shift, Location, Job Status, Assign by row
c_shift, c_loc, c_status, c_assign = st.columns(4)
with c_shift:
    shift = st.selectbox("Shift *", options=[""] + ["Day", "Night"], key="shift")
    st.session_state.task_form["shift"] = shift
with c_loc:
    location = st.text_input("Location *", key="location")
    st.session_state.task_form["location"] = location
with c_status:
    job_status = st.selectbox("Job Status *", options=[""] + ["Pending", "In Progress", "Completed"], key="job_status")
    st.session_state.task_form["job_status"] = job_status
with c_assign:
    assign_by = st.text_input("Assign by", key="assign_by")
    st.session_state.task_form["assign_by"] = assign_by

# Date Start, Time Start, Machine ID row
c_sd, c_st, c_mid = st.columns(3)
with c_sd:
    start_date = st.date_input("Date Start *", value=st.session_state.task_form["start_date"], key="start_date")
    st.session_state.task_form["start_date"] = start_date
with c_st:
    time_start = st.time_input("Time Start *", value=st.session_state.task_form["time_start"], key="time_start")
    st.session_state.task_form["time_start"] = time_start
with c_mid:
    machine_id = st.text_input("Machine ID *", key="machine_id")
    st.session_state.task_form["machine_id"] = machine_id

# Date End, Time End, Machine/Equipment row
c_ed, c_et, c_meq = st.columns(3)
with c_ed:
    end_date = st.date_input("Date End *", value=st.session_state.task_form["end_date"], key="end_date")
    st.session_state.task_form["end_date"] = end_date
with c_et:
    time_end = st.time_input("Time End *", value=st.session_state.task_form["time_end"], key="time_end")
    st.session_state.task_form["time_end"] = time_end
with c_meq:
    machine_equipment = st.text_input("Machine/Equipment *", key="machine_equipment")
    st.session_state.task_form["machine_equipment"] = machine_equipment

# Task Description
task_description = st.text_area("Task Description *", height=120, key="task_description")
st.session_state.task_form["task_description"] = task_description

# Action
action = st.text_area("Action *", height=120, key="action")
st.session_state.task_form["action"] = action

# Remark
remark = st.text_area("Remark", height=90, key="remark")
st.session_state.task_form["remark"] = remark

# Verify by
verify_by = st.text_input("Verify by *", key="verify_by")
st.session_state.task_form["verify_by"] = verify_by

# Spare Parts Used
spare_parts_used = st.text_area("Spare Parts Used", height=90, key="spare_parts_used")
st.session_state.task_form["spare_parts_used"] = spare_parts_used

# Upload images before (minimum 4)
st.markdown("### 📸 Upload Images Before Maintenance (Minimum 4 images)")
before_images = st.file_uploader(
    "Upload images before maintenance",
    type=["png", "jpg", "jpeg", "gif"],
    accept_multiple_files=True,
    key="before_images"
)
if before_images:
    st.session_state.task_form["before_images"] = before_images
    if len(before_images) < 4:
        st.warning("⚠️ Please upload at least 4 images before maintenance")
    else:
        st.success(f"✅ {len(before_images)} images uploaded before maintenance")

    # Display uploaded images
    cols = st.columns(4)
    for i, img in enumerate(before_images):
        with cols[i % 4]:
            st.image(img, caption=f"Before {i+1}", use_column_width=True)

# Upload images after (minimum 4)
st.markdown("### 📸 Upload Images After Maintenance (Minimum 4 images)")
after_images = st.file_uploader(
    "Upload images after maintenance",
    type=["png", "jpg", "jpeg", "gif"],
    accept_multiple_files=True,
    key="after_images"
)
if after_images:
    st.session_state.task_form["after_images"] = after_images
    if len(after_images) < 4:
        st.warning("⚠️ Please upload at least 4 images after maintenance")
    else:
        st.success(f"✅ {len(after_images)} images uploaded after maintenance")

    # Display uploaded images
    cols = st.columns(4)
    for i, img in enumerate(after_images):
        with cols[i % 4]:
            st.image(img, caption=f"After {i+1}", use_column_width=True)

# Validation and duration check
start_dt = datetime.combine(start_date, time_start)
end_dt = datetime.combine(end_date, time_end)

duration_err = None
if end_dt < start_dt:
    duration_err = "Date/Time End must be after Date/Time Start."

# Submit button
if st.button("✅ Submit Task", type="primary"):
    if duration_err:
        show_user_error(duration_err)
    elif not str(shift).strip():
        show_user_error("Shift is required.")
    elif not str(location).strip():
        show_user_error("Location is required.")
    elif not str(severity).strip():
        show_user_error("Severity is required.")
    elif not str(priority).strip():
        show_user_error("Priority is required.")
    elif not str(maintenance_frequency).strip():
        show_user_error("Maintenance Frequency is required.")
    elif not str(job_status).strip():
        show_user_error("Job Status is required.")
    elif not str(machine_id).strip():
        show_user_error("Machine ID is required.")
    elif not str(machine_equipment).strip():
        show_user_error("Machine/Equipment is required.")
    elif not str(task_description).strip():
        show_user_error("Task Description is required.")
    elif not str(action).strip():
        show_user_error("Action is required.")
    elif not str(verify_by).strip():
        show_user_error("Verify by is required.")
    elif len(st.session_state.task_form.get("before_images", [])) < 4:
        show_user_error("At least 4 images before maintenance are required.")
    elif len(st.session_state.task_form.get("after_images", [])) < 4:
        show_user_error("At least 4 images after maintenance are required.")
    else:
        # Save images and get paths
        before_image_paths = save_images(st.session_state.task_form.get("before_images", []), job_id, "before")
        after_image_paths = save_images(st.session_state.task_form.get("after_images", []), job_id, "after")

        new_entry = {
            "Job Type": "Maintenance",
            "Create By": _current_user_display_name(),
            "Create at": format_ts_sg(),
            "Date": entry_date.strftime("%Y-%m-%d"),
            "Job ID": job_id,
            "Severity": str(severity).strip(),
            "Priority": str(priority).strip(),
            "Maintenance Frequency": str(maintenance_frequency).strip(),
            "Shift": str(shift).strip(),
            "Location": str(location).strip(),
            "Job Status": str(job_status).strip(),
            "Assign by": str(assign_by).strip(),
            "Date Start": start_date.strftime("%Y-%m-%d"),
            "Time Start": time_start.strftime("%H:%M:%S"),
            "Machine ID": str(machine_id).strip(),
            "Date End": end_date.strftime("%Y-%m-%d"),
            "Time End": time_end.strftime("%H:%M:%S"),
            "Machine/Equipment": str(machine_equipment).strip(),
            "Task Description": str(task_description).strip(),
            "Action": str(action).strip(),
            "Remark": str(remark).strip(),
            "Verify by": str(verify_by).strip(),
            "Spare Parts Used": str(spare_parts_used).strip(),
            "Before Images": before_image_paths,
            "After Images": after_image_paths
        }

        df = load_task_data()
        df = pd.concat([df, pd.DataFrame([new_entry])], ignore_index=True)

        for col in DB_COLUMNS:
            if col not in df.columns:
                df[col] = ""

        df = df[DB_COLUMNS]

        if save_task_data(df):
            st.success("Task submitted successfully!")

            st.session_state.task_form = {
                "date": today_sg(),
                "shift": "",
                "location": "",
                "job_status": "",
                "assign_by": "",
                "start_date": today_sg(),
                "time_start": now_sg().time().replace(microsecond=0),
                "end_date": today_sg(),
                "time_end": now_sg().time().replace(microsecond=0),
                "machine_id": "",
                "machine_equipment": "",
                "severity": "",
                "priority": "",
                "maintenance_frequency": "",
                "task_description": "",
                "action": "",
                "remark": "",
                "verify_by": "",
                "spare_parts_used": "",
                "before_images": [],
                "after_images": []
            }
            st.rerun()
