from typing import Dict, List, Tuple
import pandas as pd
import numpy as np
from pydantic import BaseModel


class FeatureSpec(BaseModel):
    name: str
    description: str
    dtype: str
    derivation: str
    capped: bool = False
    cap_value: float | None = None


class FeatureDraftResult(BaseModel):
    features_created: List[FeatureSpec]
    outliers_capped: Dict[str, int]


class FeatureDraftService:
    def __init__(self, df: pd.DataFrame, domain: str = "logistics"):
        self.df = df.copy()
        self.domain = domain
        self.feature_specs: List[FeatureSpec] = []
        self.outliers_capped: Dict[str, int] = {}
    
    def run(self) -> Tuple[pd.DataFrame, FeatureDraftResult]:
        """Execute Phase 7: Feature Draft"""
        
        # Derive domain-specific features
        if self.domain == "logistics":
            self._logistics_features()
        elif self.domain == "healthcare":
            self._healthcare_features()
        elif self.domain == "retail":
            self._retail_features()
        elif self.domain == "emarketing":
            self._emarketing_features()
        elif self.domain == "finance":
            self._finance_features()
        
        # Cap outliers on derived features
        self._cap_outliers()
        
        result = FeatureDraftResult(
            features_created=self.feature_specs,
            outliers_capped=self.outliers_capped
        )
        
        return self.df, result
    
    def _logistics_features(self):
        """Derive logistics-specific features"""
        
        # Transit time (if dates available)
        if 'pickup_date' in self.df.columns and 'delivery_date' in self.df.columns:
            self.df['pickup_date'] = pd.to_datetime(self.df['pickup_date'], errors='coerce')
            self.df['delivery_date'] = pd.to_datetime(self.df['delivery_date'], errors='coerce')
            
            self.df['transit_time'] = (
                self.df['delivery_date'] - self.df['pickup_date']
            ).dt.total_seconds() / 3600  # hours
            
            self.feature_specs.append(FeatureSpec(
                name="transit_time",
                description="Hours between pickup and delivery",
                dtype="float64",
                derivation="(delivery_date - pickup_date).total_seconds() / 3600"
            ))
        
        # SLA flag (if transit_time exists)
        if 'transit_time' in self.df.columns:
            self.df['sla_flag'] = (self.df['transit_time'] <= 48).astype(int)
            
            self.feature_specs.append(FeatureSpec(
                name="sla_flag",
                description="1 if delivered within 48h SLA",
                dtype="int64",
                derivation="transit_time <= 48"
            ))
        
        # RTO flag
        if 'status' in self.df.columns:
            self.df['rto_flag'] = (
                self.df['status'].astype(str).str.lower().str.contains('return', na=False)
            ).astype(int)
            
            self.feature_specs.append(FeatureSpec(
                name="rto_flag",
                description="1 if shipment returned to origin",
                dtype="int64",
                derivation="status contains 'return'"
            ))
    
    def _healthcare_features(self):
        """Derive healthcare-specific features"""
        
        # Length of Stay (LOS)
        if 'admission_ts' in self.df.columns and 'discharge_ts' in self.df.columns:
            self.df['admission_ts'] = pd.to_datetime(self.df['admission_ts'], errors='coerce')
            self.df['discharge_ts'] = pd.to_datetime(self.df['discharge_ts'], errors='coerce')
            
            self.df['los_days'] = (
                self.df['discharge_ts'] - self.df['admission_ts']
            ).dt.total_seconds() / 86400  # days
            
            self.feature_specs.append(FeatureSpec(
                name="los_days",
                description="Length of stay in days",
                dtype="float64",
                derivation="(discharge_ts - admission_ts).days"
            ))
        
        # Age group
        if 'age' in self.df.columns:
            self.df['age_group'] = pd.cut(
                self.df['age'],
                bins=[0, 18, 40, 60, 100],
                labels=['Child', 'Adult', 'Middle', 'Senior']
            )
            
            self.feature_specs.append(FeatureSpec(
                name="age_group",
                description="Age category",
                dtype="category",
                derivation="cut(age, [0,18,40,60,100])"
            ))
    
    def _retail_features(self):
        """Derive retail-specific features"""
        
        # Order value
        if 'quantity' in self.df.columns and 'price' in self.df.columns:
            self.df['order_value'] = self.df['quantity'] * self.df['price']
            
            self.feature_specs.append(FeatureSpec(
                name="order_value",
                description="Total order amount",
                dtype="float64",
                derivation="quantity * price"
            ))
        
        # Return flag
        if 'return_flag' not in self.df.columns and 'status' in self.df.columns:
            self.df['return_flag'] = (
                self.df['status'].astype(str).str.lower().str.contains('return', na=False)
            ).astype(int)
            
            self.feature_specs.append(FeatureSpec(
                name="return_flag",
                description="1 if order returned",
                dtype="int64",
                derivation="status contains 'return'"
            ))
    
    def _emarketing_features(self):
        """Derive e-marketing features"""
        
        # CTR (Click-Through Rate)
        if 'clicks' in self.df.columns and 'impressions' in self.df.columns:
            self.df['ctr'] = self.df['clicks'] / self.df['impressions'].replace(0, np.nan)
            
            self.feature_specs.append(FeatureSpec(
                name="ctr",
                description="Click-through rate",
                dtype="float64",
                derivation="clicks / impressions"
            ))
        
        # Conversion flag
        if 'conversions' in self.df.columns:
            self.df['conversion_flag'] = (self.df['conversions'] > 0).astype(int)
            
            self.feature_specs.append(FeatureSpec(
                name="conversion_flag",
                description="1 if at least one conversion",
                dtype="int64",
                derivation="conversions > 0"
            ))
    
    def _finance_features(self):
        """Derive finance features"""
        
        # Loan duration
        if 'open_date' in self.df.columns:
            self.df['open_date'] = pd.to_datetime(self.df['open_date'], errors='coerce')
            today = pd.Timestamp.now()
            
            self.df['loan_duration_days'] = (today - self.df['open_date']).dt.days
            
            self.feature_specs.append(FeatureSpec(
                name="loan_duration_days",
                description="Days since loan opened",
                dtype="float64",
                derivation="(today - open_date).days"
            ))
        
        # Overdue flag
        if 'default_flag' in self.df.columns:
            self.df['overdue_flag'] = self.df['default_flag']
    
    def _cap_outliers(self):
        """Cap extreme outliers based on domain rules"""
        
        if self.domain == "logistics":
            caps = {"transit_time": 240, "dwell_time": 72}
        elif self.domain == "healthcare":
            caps = {"los_days": 365}
        elif self.domain == "retail":
            caps = {}
        else:
            caps = {}
        
        for col, cap_value in caps.items():
            if col in self.df.columns:
                original_count = (self.df[col] > cap_value).sum()
                self.df[col] = self.df[col].clip(upper=cap_value)
                
                if original_count > 0:
                    self.outliers_capped[col] = int(original_count)
                    
                    # Update spec
                    for spec in self.feature_specs:
                        if spec.name == col:
                            spec.capped = True
                            spec.cap_value = cap_value


