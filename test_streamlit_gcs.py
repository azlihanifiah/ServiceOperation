"""
Streamlit app to test Google Cloud Storage connection
Run: streamlit run test_streamlit_gcs.py
"""

import streamlit as st
from pathlib import Path
from gcp_storage import (
    check_gcs_connection,
    get_bucket,
    upload_image,
    download_image,
    list_images_for_job,
    GCP_KEY_PATH,
)

st.set_page_config(page_title="GCS Connection Test", page_icon="☁️", layout="wide")

st.title("☁️ Google Cloud Storage Connection Test")

# Check key file
st.subheader("1️⃣ GCP Key File Status")

if GCP_KEY_PATH.exists():
    st.success(f"✅ GCP key found at: `{GCP_KEY_PATH}`")
    file_size = GCP_KEY_PATH.stat().st_size
    st.info(f"File size: {file_size} bytes")
else:
    st.error(f"❌ GCP key NOT found at: `{GCP_KEY_PATH}`")
    st.warning("Please download your service account JSON key and save it to: `config/gcp-key.json`")

# Test connection
st.subheader("2️⃣ Connection Test")

if st.button("🔗 Test Connection", use_container_width=True):
    with st.spinner("Testing connection..."):
        try:
            if check_gcs_connection():
                st.success("✅ Connected to Google Cloud Storage!")
                
                # Get bucket info
                try:
                    bucket = get_bucket()
                    bucket.reload()
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Bucket Name", bucket.name)
                    with col2:
                        st.metric("Location", bucket.location or "N/A")
                    with col3:
                        st.metric("Storage Class", bucket.storage_class or "N/A")
                    
                    # Count objects
                    blobs = list(bucket.list_blobs(max_results=100))
                    st.info(f"Objects in bucket: {len(blobs)}")
                    
                    if blobs:
                        st.write("**Recent files:**")
                        for blob in blobs[:10]:
                            st.text(f"  📄 {blob.name} ({blob.size} bytes)")
                except Exception as e:
                    st.error(f"Failed to get bucket info: {e}")
            else:
                st.error("❌ Connection failed")
        except Exception as e:
            st.error(f"Error: {e}")

# Test image upload/download
st.subheader("3️⃣ Image Upload/Download Test")

col1, col2 = st.columns(2)

with col1:
    st.write("**Upload a test image:**")
    uploaded_file = st.file_uploader("Choose an image", type=["jpg", "jpeg", "png", "gif"])
    
    if uploaded_file and st.button("📤 Upload to Cloud"):
        with st.spinner("Uploading..."):
            try:
                image_bytes = uploaded_file.getvalue()
                job_id = "TEST_JOB"
                image_type = "test"
                filename = uploaded_file.name
                
                path = upload_image(image_bytes, job_id, image_type, filename)
                
                if path:
                    st.success(f"✅ Uploaded to: `{path}`")
                    st.session_state.uploaded_image_path = path
                else:
                    st.error("❌ Upload failed")
            except Exception as e:
                st.error(f"Error: {e}")

with col2:
    st.write("**Download test image:**")
    
    if "uploaded_image_path" in st.session_state:
        image_path = st.session_state.uploaded_image_path
        
        if st.button("📥 Download from Cloud"):
            with st.spinner("Downloading..."):
                try:
                    image_bytes = download_image(image_path)
                    
                    if image_bytes:
                        st.success("✅ Downloaded successfully")
                        st.image(image_bytes)
                    else:
                        st.error("❌ Download failed")
                except Exception as e:
                    st.error(f"Error: {e}")
    else:
        st.info("Upload an image first to test download")

# List test images
st.subheader("4️⃣ List Test Images")

if st.button("📋 List Test Images"):
    with st.spinner("Loading..."):
        try:
            images = list_images_for_job("TEST_JOB")
            
            if images:
                st.success(f"Found {len(images)} test images:")
                for img in images:
                    st.text(f"  📄 {img}")
            else:
                st.info("No test images found yet")
        except Exception as e:
            st.error(f"Error: {e}")

# Summary
st.divider()
st.subheader("✅ Checklist")

checklist = {
    "GCP key file exists": GCP_KEY_PATH.exists(),
    "Connection works": False,  # Would need to test
}

for item, status in checklist.items():
    status_emoji = "✅" if status else "❌"
    st.write(f"{status_emoji} {item}")

st.info("✅ If all tests pass, you can start integrating GCS into your app!")
