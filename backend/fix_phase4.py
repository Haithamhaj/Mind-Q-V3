#!/usr/bin/env python3
"""
Fix Phase 4 timeout issues by adding sampling for large datasets
"""

def fix_phase4():
    # Read current phase4 file
    with open("app/services/phase4_profiling.py", "r") as f:
        content = f.read()
    
    # Add sampling logic for large datasets
    if "sample_size = 50000" not in content:
        # Find the run method and add sampling
        old_run_start = """    def run(self) -> ProfilingResult:
        \"\"\"Execute Phase 4: Profiling\"\"\"
        # Basic stats
        memory_mb = self.df.memory_usage(deep=True).sum() / (1024**2)"""
        
        new_run_start = """    def run(self) -> ProfilingResult:
        \"\"\"Execute Phase 4: Profiling with large dataset handling\"\"\"
        # Sample large datasets to prevent timeout
        original_size = len(self.df)
        if original_size > 50000:  # Sample if > 50K rows
            sample_size = 50000
            df_sample = self.df.sample(n=sample_size, random_state=42)
            print(f"🔧 Phase 4: Sampling {sample_size} rows from {original_size} total")
        else:
            df_sample = self.df
        
        # Basic stats
        memory_mb = df_sample.memory_usage(deep=True).sum() / (1024**2)"""
        
        content = content.replace(old_run_start, new_run_start)
        
        # Write back
        with open("app/services/phase4_profiling.py", "w") as f:
            f.write(content)
        
        print("✅ Fixed Phase 4 timeout issue with sampling")
    else:
        print("✅ Phase 4 already has sampling logic")

if __name__ == "__main__":
    fix_phase4()



