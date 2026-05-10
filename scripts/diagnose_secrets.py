import streamlit as st
import json

st.title("🔍 Streamlit Secrets Diagnostic")

# Check 1: Is gcp_service_account in secrets?
st.subheader("1️⃣ Check if gcp_service_account exists")
try:
    secret_account = st.secrets.get("gcp_service_account")
    if secret_account:
        st.success("✅ gcp_service_account found in secrets!")
        # Show which fields are present
        st.write("**Fields present:**")
        if isinstance(secret_account, dict):
            for key in secret_account.keys():
                if key == "private_key":
                    st.write(f"  - {key}: {secret_account[key][:50]}... (truncated)")
                else:
                    st.write(f"  - {key}: {secret_account[key]}")
    else:
        st.error("❌ gcp_service_account NOT found in secrets")
except Exception as e:
    st.error(f"❌ Error reading secrets: {e}")

# Check 2: Verify individual fields
st.subheader("2️⃣ Verify Required Fields")
if st.secrets:
    try:
        secret_dict = st.secrets.get("gcp_service_account", {})
        required_fields = [
            "type",
            "project_id", 
            "private_key_id",
            "private_key",
            "client_email",
            "client_id",
            "auth_uri",
            "token_uri",
            "auth_provider_x509_cert_url",
            "client_x509_cert_url"
        ]
        
        for field in required_fields:
            if field in secret_dict:
                st.write(f"✅ {field}")
            else:
                st.error(f"❌ {field} MISSING")
    except Exception as e:
        st.error(f"Error checking fields: {e}")

# Check 3: Test GCS Connection
st.subheader("3️⃣ Test GCS Connection")
if st.button("🔗 Test GCS Connection with Secrets"):
    try:
        from google.cloud import storage
        from google.oauth2 import service_account
        
        secret_dict = st.secrets.get("gcp_service_account")
        if not secret_dict:
            st.error("❌ No gcp_service_account in secrets")
        else:
            # Try to create credentials from the secret
            credentials = service_account.Credentials.from_service_account_info(secret_dict)
            st.success(f"✅ Credentials created: {credentials.service_account_email}")
            
            # Try to create storage client
            client = storage.Client(credentials=credentials)
            st.success(f"✅ Storage Client created: Project {client.project}")
            
            # Try to access the bucket
            bucket = client.bucket("ammar-builders-maintenance")
            exists = bucket.exists()
            if exists:
                st.success("✅ Bucket 'ammar-builders-maintenance' accessible!")
            else:
                st.error("❌ Bucket not found or not accessible")
    except Exception as e:
        st.error(f"❌ GCS Connection Error: {str(e)}")
        st.write("**Full Error:**")
        st.code(str(e))

# Check 4: Compare with local file
st.subheader("4️⃣ Compare with Local GCP Key")
try:
    from pathlib import Path
    key_path = Path("config/gcp-key.json")
    if key_path.exists():
        with open(key_path) as f:
            local_key = json.load(f)
        
        st.write("**Local GCP Key Fields:**")
        for key in ["type", "project_id", "client_email", "client_id"]:
            st.write(f"  - {key}: {local_key.get(key)}")
        
        # Compare
        secret_dict = st.secrets.get("gcp_service_account", {})
        st.write("\n**Secret Fields:**")
        for key in ["type", "project_id", "client_email", "client_id"]:
            st.write(f"  - {key}: {secret_dict.get(key)}")
        
        # Check differences
        st.write("\n**Differences:**")
        if local_key.get("client_email") != secret_dict.get("client_email"):
            st.warning(f"⚠️ client_email mismatch!")
            st.write(f"  Local: {local_key.get('client_email')}")
            st.write(f"  Secret: {secret_dict.get('client_email')}")
        if local_key.get("client_id") != secret_dict.get("client_id"):
            st.warning(f"⚠️ client_id mismatch!")
            st.write(f"  Local: {local_key.get('client_id')}")
            st.write(f"  Secret: {secret_dict.get('client_id')}")
    else:
        st.info("No local gcp-key.json found")
except Exception as e:
    st.error(f"Error comparing: {e}")
