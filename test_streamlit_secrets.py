"""
Test Streamlit Secrets Configuration
Run: streamlit run test_streamlit_secrets.py
"""

import streamlit as st
import json
from pathlib import Path

st.set_page_config(page_title="Streamlit Secrets Test", page_icon="🔐")

st.title("🔐 Streamlit Secrets Configuration Test")

st.info("This app helps debug Streamlit secrets configuration for Google Cloud Storage")

# Test 1: Check local GCP key file
st.subheader("1️⃣ Local GCP Key File")
local_key_path = Path(__file__).parent / "config" / "gcp-key.json"

if local_key_path.exists():
    st.success(f"✅ Found local key at: {local_key_path}")
    try:
        with open(local_key_path) as f:
            key_data = json.load(f)
        st.success(f"✅ Key is valid JSON")
        st.json({
            "project_id": key_data.get("project_id"),
            "type": key_data.get("type"),
            "client_email": key_data.get("client_email")
        })
    except json.JSONDecodeError:
        st.error("❌ Key file is not valid JSON")
else:
    st.warning(f"⚠️ Local key not found at: {local_key_path}")

# Test 2: Check Streamlit Secrets
st.subheader("2️⃣ Streamlit Cloud Secrets")

try:
    secrets = st.secrets
    st.write("Available secrets:")
    st.write(list(secrets.keys()))
    
    # Check for gcp_service_account
    if "gcp_service_account" in secrets:
        st.success("✅ Found gcp_service_account in secrets")
        gcp_secret = secrets["gcp_service_account"]
        
        if isinstance(gcp_secret, dict):
            st.success("✅ gcp_service_account is a dictionary")
            st.json({
                "project_id": gcp_secret.get("project_id"),
                "type": gcp_secret.get("type"),
                "client_email": gcp_secret.get("client_email")
            })
        else:
            st.warning("⚠️ gcp_service_account is not a dictionary")
            st.write(f"Type: {type(gcp_secret)}")
    else:
        st.error("❌ gcp_service_account NOT found in secrets")
        st.error("Please add GCP secret to Streamlit Cloud")
        st.info("""
        **How to add GCP secret to Streamlit Cloud:**
        
        1. Go to https://share.streamlit.io/
        2. Find your app
        3. Click Settings (⋮ menu)
        4. Click Secrets
        5. Add this format:
        
        ```toml
        [gcp_service_account]
        type = "service_account"
        project_id = "your-project-id"
        private_key_id = "..."
        private_key = "..."
        client_email = "..."
        client_id = "..."
        auth_uri = "https://accounts.google.com/o/oauth2/auth"
        token_uri = "https://oauth2.googleapis.com/token"
        auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
        client_x509_cert_url = "..."
        ```
        """)
        
except Exception as e:
    st.error(f"❌ Error accessing secrets: {e}")

# Test 3: Test GCS connection
st.subheader("3️⃣ Google Cloud Storage Connection")

if st.button("🔗 Test GCS Connection"):
    try:
        from google.oauth2 import service_account
        from google.cloud import storage
        
        # Try local file first
        try:
            with open(local_key_path) as f:
                key_data = json.load(f)
            credentials = service_account.Credentials.from_service_account_info(key_data)
            st.success("✅ Loaded credentials from local file")
        except Exception as e:
            st.info(f"Local file not available: {e}")
            # Try secrets
            gcp_secret = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(gcp_secret)
            st.success("✅ Loaded credentials from Streamlit secrets")
        
        # Test connection
        client = storage.Client(credentials=credentials)
        bucket = client.bucket("ammar-builders-maintenance")
        bucket.reload()
        
        st.success("✅ Connected to Google Cloud Storage!")
        st.success(f"✅ Bucket exists: {bucket.name}")
        
        # List files
        blobs = list(bucket.list_blobs(max_results=10))
        if blobs:
            st.success(f"✅ Found {len(blobs)} files in bucket")
            for blob in blobs:
                st.text(f"  📄 {blob.name}")
        else:
            st.info("ℹ️ Bucket is empty")
            
    except Exception as e:
        st.error(f"❌ Connection failed: {e}")
        st.error("Make sure you've added the GCP secret to Streamlit Cloud")

st.divider()

# Instructions
st.subheader("📋 Setup Instructions")

st.markdown("""
### Local Development (Your Computer)
✅ Keep `config/gcp-key.json` file - it's in `.gitignore`

### Streamlit Cloud (https://ammarbuilders-v1.streamlit.app/)
1. Go to https://share.streamlit.io/
2. Click on **ammarbuilders-v1**
3. Click the **⋮ (Settings)** menu
4. Click **Secrets**
5. Paste your GCP service account JSON in this format:

```toml
[gcp_service_account]
type = "service_account"
project_id = "service-report-494512"
private_key_id = "xxxx"
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "ammar-builders-app@service-report-494512.iam.gserviceaccount.com"
client_id = "123456789"
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/ammar-builders-app%40service-report-494512.iam.gserviceaccount.com"
```

6. Click **Save**
7. Wait for app to reload

### Important Notes
- Don't commit `config/gcp-key.json` to GitHub (it's in `.gitignore` ✅)
- Only add secrets to Streamlit Cloud (Settings → Secrets)
- The private_key value must have `\\n` for newlines in TOML format
""")
