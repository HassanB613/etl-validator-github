#!/usr/bin/env python3
"""
Trigger Jenkins Build for ETL Test Pipeline
"""
import requests

JENKINS_URL = "https://jenkins-east.cloud.cms.gov/mtf-pm/job/etl-test/job/main"
JENKINS_USER = "BOSF"
JENKINS_TOKEN = "11d6dbd08d2e55327737e0e8bd9f19c8e9"

print("🚀 Triggering Jenkins Build #161...")
print(f"   URL: {JENKINS_URL}")

response = requests.post(
    f"{JENKINS_URL}/build",
    auth=(JENKINS_USER, JENKINS_TOKEN),
    verify=True  # Verify SSL certificate
)

print(f"\n📊 Response Status: {response.status_code}")

if response.status_code in [200, 201]:
    print("✅ Build triggered successfully!")
    print(f"🔗 Monitor at: {JENKINS_URL}")
elif response.status_code == 401:
    print("❌ Authentication failed - check username/token")
elif response.status_code == 403:
    print("❌ Forbidden - insufficient permissions")
elif response.status_code == 404:
    print("❌ Job not found - check URL")
else:
    print(f"❌ Unexpected status code: {response.status_code}")
    print(f"Response: {response.text}")
