# Google Cloud Storage Integration Guide

## Quick Integration Checklist

Your project now has Google Cloud Storage support! Here's what you need to do next:

### 1. **Download Your GCP Service Account Key**
   - From Google Cloud Console → APIs & Services → Credentials
   - Click on "ammar-builders-app" service account
   - Go to KEYS tab → Create Key → JSON
   - Save the file as: `config/gcp-key.json`
   - ⚠️ DO NOT commit this file to Git (it's in .gitignore)

### 2. **Verify File Structure**
```
AB_taskreport_v1.0/
├── config/
│   └── gcp-key.json          ← Place your key here (NEVER SHARE!)
├── .streamlit/
│   └── secrets.toml          ✅ Created
├── .gitignore                ✅ Created
├── gcp_storage.py            ✅ Created
├── requirements.txt          ✅ Updated
├── Home.py
├── utils.py
├── pages/
│   └── 3_TaskUpdate.py
└── data/
    ├── task_images/
    └── task_reports.db
```

### 3. **Test the Connection**
Once you have the `config/gcp-key.json` file in place, run this test:

```python
import streamlit as st
from gcp_storage import check_gcs_connection

if st.button("Test GCS Connection"):
    if check_gcs_connection():
        st.success("✅ Connected to Google Cloud Storage!")
    else:
        st.error("❌ Failed to connect")
```

---

## Code Integration Examples

### **For 3_TaskUpdate.py:**

**BEFORE (Local Storage):**
```python
import sqlite3
from pathlib import Path

DB_PATH = Path("data/task_reports.db")

def load_task_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query('SELECT * FROM task_reports', conn)
    conn.close()
    return df

def save_task_data(df):
    conn = sqlite3.connect(DB_PATH)
    df.to_sql('task_reports', conn, if_exists='replace', index=False)
    conn.close()

def save_images(images, job_id: str, image_type: str):
    for i, img in enumerate(images):
        filepath = IMAGES_DIR / f"{job_id}_{image_type}_{i+1}.jpg"
        with open(filepath, "wb") as f:
            f.write(img.getbuffer())
```

**AFTER (Google Cloud Storage):**
```python
from gcp_storage import download_database, upload_database, upload_image

def load_task_data():
    return download_database()

def save_task_data(df):
    return upload_database(df)

def save_images(images, job_id: str, image_type: str):
    saved_paths = []
    for i, img in enumerate(images):
        path = upload_image(
            img.getbuffer(),
            job_id,
            image_type,
            f"image_{i+1}.jpg"
        )
        saved_paths.append(path)
    return ",".join(saved_paths)
```

---

## Available Functions in gcp_storage.py

### **Database Functions:**
```python
# Download database from cloud
df = download_database()

# Upload database to cloud
success = upload_database(df)
```

### **Image Functions:**
```python
# Upload image
storage_path = upload_image(image_bytes, job_id, image_type, filename)

# Download image
image_bytes = download_image(storage_path)

# List all images for a job
images = list_images_for_job(job_id)
```

### **Backup Functions:**
```python
# Create timestamped backup
success = create_backup()

# List all backups
backups = list_backups()

# Restore from backup
success = restore_from_backup("backups/task_reports_backup_20260426_120000.db")
```

### **Status Check:**
```python
# Test connection
is_connected = check_gcs_connection()
```

---

## Next Steps

1. **Complete Google Cloud Setup (Steps 1-6 from guide)**
   - Create Google Cloud Project
   - Enable APIs
   - Create Storage bucket
   - Create Service Account
   - Download JSON key → Save as `config/gcp-key.json`

2. **Test the Connection**
   - Run a simple test to verify your setup

3. **Update Your Code Files**
   - Gradually migrate your local storage to cloud storage
   - Start with one file (e.g., 3_TaskUpdate.py)
   - Test thoroughly before moving to other files

4. **Migrate Existing Data** (Optional)
   - Download your current `data/task_reports.db`
   - Load it with pandas
   - Upload using `upload_database(df)`

---

## Troubleshooting

### **Error: "GCP key not found"**
- Make sure `config/gcp-key.json` exists
- Check the file path is correct

### **Error: "Failed to initialize GCS client"**
- Verify the JSON key is valid (not corrupted)
- Check that the service account has Storage Admin role

### **Error: "Bucket not found"**
- Verify bucket name in `.streamlit/secrets.toml`
- Ensure bucket exists in Google Cloud Console

### **Images not uploading**
- Check that image bytes are not empty
- Verify bucket write permissions

---

## Security Notes

⚠️ **IMPORTANT:**
- Never commit `config/gcp-key.json` to Git
- It's already in `.gitignore` ✅
- If you accidentally commit it, rotate the key in Google Cloud Console
- The key grants full access to your cloud storage

---

## Cost Estimate

**Monthly Cost for Typical Usage:**
- Storage: ~$0.02/GB/month
- Operations: ~$0.0004 per 10K operations
- Total: $1-10/month depending on usage

**Example:**
- 100 task records = ~10MB
- 500 images @ 1MB each = 500MB
- Total: ~510MB storage = ~$0.01/month
- Plus operation costs = ~$2-5/month total

---

## Support

If you encounter issues:
1. Check `.streamlit/secrets.toml` has correct bucket name
2. Verify `config/gcp-key.json` exists and is valid
3. Test connection with `check_gcs_connection()`
4. Check Google Cloud Console for any errors
