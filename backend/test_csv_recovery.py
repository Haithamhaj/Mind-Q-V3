#!/usr/bin/env python3
"""
Test CSV recovery with the problematic file
"""

import requests
from pathlib import Path

def test_csv_recovery():
    """Test the new CSV recovery system"""
    
    # Test with the problematic file
    csv_file = Path("../Bulk Shipment Report_LM_2025.08.23.csv") 
    
    if not csv_file.exists():
        print("❌ Test CSV file not found")
        return
    
    print(f"🧪 Testing CSV recovery with: {csv_file.name}")
    
    # Send to Quality Control endpoint
    url = "http://localhost:8000/api/v1/phases/quality-control"
    
    with open(csv_file, 'rb') as f:
        files = {'file': (csv_file.name, f, 'text/csv')}
        
        try:
            response = requests.post(url, files=files, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                print("✅ CSV Recovery Success!")
                print(f"📊 Status: {result.get('status', 'Unknown')}")
                print(f"📋 Fixes Applied: {len(result.get('fixes_applied', []))}")
                
                for fix in result.get('fixes_applied', []):
                    print(f"  🔧 {fix}")
                    
                print(f"⚠️  Warnings: {len(result.get('warnings', []))}")
                print(f"❌ Errors: {len(result.get('errors', []))}")
                
                return True
            else:
                print(f"❌ HTTP Error: {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Request failed: {e}")
            return False

if __name__ == "__main__":
    test_csv_recovery()



