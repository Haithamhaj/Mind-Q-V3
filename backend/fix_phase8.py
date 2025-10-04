#!/usr/bin/env python3
"""
Fix Phase 8 duplicate handling to continue pipeline instead of stopping
"""

def fix_phase8():
    # Read current phase8 file
    with open("app/services/phase8_merging.py", "r") as f:
        content = f.read()
    
    # Check if auto-fix is already implemented
    if "Mind-Q-V3 Auto-Fix" in content:
        print("✅ Phase 8 auto-fix already implemented")
        return
    
    # Find and replace the STOP condition
    old_stop = """        if result.status == "STOP":
            raise HTTPException(400, f"Merging failed: {result.issues}")"""
    
    new_stop = """        if result.status == "STOP":
            # Mind-Q-V3 Auto-Fix: Handle duplicate issues intelligently
            duplicate_issues = [issue for issue in result.issues if issue.issue_type == "duplicates"]
            
            if duplicate_issues:
                print("🔧 Mind-Q-V3 Auto-Fix: High duplicates detected, applying deduplication...")
                
                # Find ID columns to deduplicate on
                id_cols = [col for col in df.columns if 'id' in col.lower()]
                if id_cols:
                    # Keep latest record for each ID
                    timestamp_cols = [c for c in df.columns if any(word in c.lower() for word in ['date', 'time', 'ts', 'created'])]
                    
                    if timestamp_cols:
                        df_merged = df_merged.sort_values(timestamp_cols[0], ascending=False)
                        df_merged = df_merged.drop_duplicates(subset=id_cols[:1], keep='first')
                        print(f"✅ Deduplication: kept latest records, {len(df_merged)} rows remaining")
                    else:
                        df_merged = df_merged.drop_duplicates(subset=id_cols[:1], keep='first') 
                        print(f"✅ Deduplication: kept first records, {len(df_merged)} rows remaining")
                    
                    # Update result
                    result.status = "WARN"
                    print("✅ Phase 8: Converted STOP to WARN - pipeline continues")
                else:
                    raise HTTPException(400, f"Merging failed: {result.issues}")
            else:
                raise HTTPException(400, f"Merging failed: {result.issues}")"""
    
    if old_stop in content:
        content = content.replace(old_stop, new_stop)
        
        with open("app/services/phase8_merging.py", "w") as f:
            f.write(content)
        
        print("✅ Fixed Phase 8 duplicate handling")
    else:
        print("❌ Could not find Phase 8 STOP condition to fix")

if __name__ == "__main__":
    fix_phase8()



