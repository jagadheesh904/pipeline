#!/usr/bin/env python3
import requests
import json
from config.databricks_config import db_config

def test_databricks_connection():
    print("Testing Databricks connection...")
    
    if not db_config.is_configured():
        print("❌ Databricks not configured. Check your .env file")
        return False
    
    print(f"✅ Server: {db_config.server_hostname}")
    print(f"✅ HTTP Path: {db_config.http_path}")
    print(f"✅ Access Token: {'*' * len(db_config.access_token)}")
    
    # Test Jobs API
    headers = {
        "Authorization": f"Bearer {db_config.access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(
            f"https://{db_config.server_hostname}/api/2.0/jobs/list",
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            print("✅ Jobs API connection successful!")
            return True
        else:
            print(f"❌ Jobs API failed: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_databricks_connection()
