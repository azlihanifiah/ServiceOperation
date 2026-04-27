#!/usr/bin/env python
"""
Test Google Cloud Storage connection
Run: python test_gcs.py
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from gcp_storage import (
    get_gcs_client, 
    get_bucket, 
    check_gcs_connection,
    GCP_KEY_PATH,
    BUCKET_NAME
)


def test_key_exists():
    """Check if GCP key file exists"""
    print("\n" + "="*60)
    print("TEST 1: GCP Key File")
    print("="*60)
    
    if GCP_KEY_PATH.exists():
        print(f"✅ GCP key found at: {GCP_KEY_PATH}")
        print(f"   File size: {GCP_KEY_PATH.stat().st_size} bytes")
        return True
    else:
        print(f"❌ GCP key NOT found at: {GCP_KEY_PATH}")
        print("   Please download your service account JSON key from Google Cloud Console")
        print("   and save it to: config/gcp-key.json")
        return False


def test_client_initialization():
    """Test initializing GCS client"""
    print("\n" + "="*60)
    print("TEST 2: GCS Client Initialization")
    print("="*60)
    
    try:
        client = get_gcs_client()
        print(f"✅ GCS Client initialized successfully")
        print(f"   Project ID: {client.project}")
        return True
    except Exception as e:
        print(f"❌ Failed to initialize GCS client")
        print(f"   Error: {e}")
        return False


def test_bucket_access():
    """Test accessing the bucket"""
    print("\n" + "="*60)
    print("TEST 3: Bucket Access")
    print("="*60)
    
    try:
        bucket = get_bucket()
        print(f"✅ Bucket accessed: {BUCKET_NAME}")
        
        # Try to get bucket metadata
        bucket.reload()
        print(f"   Bucket exists: ✅")
        print(f"   Location: {bucket.location}")
        print(f"   Storage class: {bucket.storage_class}")
        return True
    except Exception as e:
        print(f"❌ Failed to access bucket")
        print(f"   Bucket name: {BUCKET_NAME}")
        print(f"   Error: {e}")
        return False


def test_list_files():
    """Test listing files in bucket"""
    print("\n" + "="*60)
    print("TEST 4: List Files in Bucket")
    print("="*60)
    
    try:
        bucket = get_bucket()
        blobs = list(bucket.list_blobs())
        
        if blobs:
            print(f"✅ Found {len(blobs)} objects in bucket:")
            for blob in blobs[:10]:  # Show first 10
                print(f"   - {blob.name} ({blob.size} bytes)")
            if len(blobs) > 10:
                print(f"   ... and {len(blobs) - 10} more")
        else:
            print(f"✅ Bucket is empty (ready for data)")
        return True
    except Exception as e:
        print(f"❌ Failed to list files")
        print(f"   Error: {e}")
        return False


def test_overall_connection():
    """Test overall connection"""
    print("\n" + "="*60)
    print("TEST 5: Overall Connection Test")
    print("="*60)
    
    try:
        if check_gcs_connection():
            print(f"✅ Google Cloud Storage connection is working!")
            return True
        else:
            print(f"❌ Connection test failed")
            return False
    except Exception as e:
        print(f"❌ Connection test error: {e}")
        return False


def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("GOOGLE CLOUD STORAGE CONNECTION TEST")
    print("="*60)
    
    tests = [
        ("Key File", test_key_exists),
        ("Client Init", test_client_initialization),
        ("Bucket Access", test_bucket_access),
        ("List Files", test_list_files),
        ("Overall", test_overall_connection),
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"\n❌ Test '{test_name}' crashed: {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, passed_bool in results.items():
        status = "✅ PASS" if passed_bool else "❌ FAIL"
        print(f"{test_name:20} {status}")
    
    print("="*60)
    print(f"\nResult: {passed}/{total} tests passed")
    
    if passed == total:
        print("✅ All tests passed! Your Google Cloud Storage is ready to use.")
        return 0
    else:
        print("❌ Some tests failed. Check the errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
