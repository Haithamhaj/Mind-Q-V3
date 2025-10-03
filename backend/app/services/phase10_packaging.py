from typing import List
from pathlib import Path
import json
import hashlib
from datetime import datetime
from pydantic import BaseModel
import zipfile


class PackagingResult(BaseModel):
    artifacts_packaged: List[str]
    total_size_mb: float
    zip_path: str
    provenance_hash: str


class PackagingService:
    def __init__(self, artifacts_dir: Path):
        self.artifacts_dir = artifacts_dir
        self.artifacts = []
    
    def run(self) -> PackagingResult:
        """Execute Phase 10: Packaging (Pre-Split)"""
        
        # Collect all artifacts
        self._collect_artifacts()
        
        # Generate provenance
        provenance = self._generate_provenance()
        provenance_path = self.artifacts_dir / "provenance.json"
        with open(provenance_path, "w") as f:
            json.dump(provenance, f, indent=2)
        
        # Generate changelog
        changelog = self._generate_changelog()
        changelog_path = self.artifacts_dir / "changelog.md"
        with open(changelog_path, "w") as f:
            f.write(changelog)
        
        # Create ZIP bundle
        zip_path = self._create_zip_bundle()
        
        # Calculate total size
        total_size = sum(f.stat().st_size for f in self.artifacts_dir.glob("*") if f.is_file())
        total_size_mb = total_size / (1024 * 1024)
        
        # Calculate hash
        prov_hash = hashlib.sha256(json.dumps(provenance).encode()).hexdigest()[:16]
        
        result = PackagingResult(
            artifacts_packaged=[f.name for f in self.artifacts],
            total_size_mb=round(total_size_mb, 2),
            zip_path=str(zip_path),
            provenance_hash=prov_hash
        )
        
        return result
    
    def _collect_artifacts(self):
        """Collect all artifacts in directory"""
        expected_files = [
            "dq_report.json",
            "profile_summary.json",
            "imputation_policy.json",
            "mapping_config.json",
            "feature_spec.json",
            "correlation_matrix.json",
            "business_veto_report.json",
            "merged_data.parquet"
        ]
        
        for filename in expected_files:
            path = self.artifacts_dir / filename
            if path.exists():
                self.artifacts.append(path)
    
    def _generate_provenance(self) -> dict:
        """Generate provenance metadata"""
        return {
            "pipeline_version": "1.2.2",
            "generated_at": datetime.utcnow().isoformat(),
            "artifacts": [
                {
                    "name": f.name,
                    "size_bytes": f.stat().st_size,
                    "sha256": self._hash_file(f)
                }
                for f in self.artifacts
            ]
        }
    
    def _hash_file(self, path: Path) -> str:
        """Calculate SHA256 hash of file"""
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()[:16]
    
    def _generate_changelog(self) -> str:
        """Generate human-readable changelog"""
        return f"""# EDA Pipeline Changelog

Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}
Pipeline Version: 1.2.2

## Phases Completed

- ✅ Phase 0: Quality Control
- ✅ Phase 1: Goal & KPIs
- ✅ Phase 2: Ingestion & Landing
- ✅ Phase 3: Schema & Dtypes
- ✅ Phase 4: Profiling
- ✅ Phase 5: Missing Data Handling
- ✅ Phase 6: Standardization
- ✅ Phase 7: Feature Draft
- ✅ Phase 8: Merging & Keys
- ✅ Phase 9: Correlations
- ✅ Phase 9.5: Business Validation

## Artifacts

{chr(10).join(f'- {f.name} ({f.stat().st_size / 1024:.1f} KB)' for f in self.artifacts)}

## Next Steps

1. Run Phase 10.5: Train/Validation/Test Split
2. Run Phase 7.5: Encoding & Scaling (on split data)
3. Run Phase 11: Advanced Exploration
4. Run Phase 11.5: Feature Selection
"""
    
    def _create_zip_bundle(self) -> Path:
        """Create ZIP bundle of all artifacts"""
        zip_path = self.artifacts_dir / "eda_bundle.zip"
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for artifact in self.artifacts:
                zipf.write(artifact, artifact.name)
            
            # Add provenance and changelog
            if (self.artifacts_dir / "provenance.json").exists():
                zipf.write(self.artifacts_dir / "provenance.json", "provenance.json")
            if (self.artifacts_dir / "changelog.md").exists():
                zipf.write(self.artifacts_dir / "changelog.md", "changelog.md")
        
        return zip_path


