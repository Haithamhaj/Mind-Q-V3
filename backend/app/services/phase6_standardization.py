from typing import Dict, List, Tuple
import pandas as pd
import re
from unicodedata import normalize
from pydantic import BaseModel


class StandardizationResult(BaseModel):
    text_normalized: List[str]
    categories_collapsed: Dict[str, int]  # column -> num_collapsed
    mappings_applied: Dict[str, Dict[str, str]]
    rare_threshold: float


class StandardizationService:
    def __init__(self, df: pd.DataFrame, domain: str = "logistics", rare_threshold: float = 0.01):
        self.df = df.copy()
        self.domain = domain
        self.rare_threshold = rare_threshold
        self.text_columns: List[str] = []
        self.category_mappings: Dict[str, Dict[str, str]] = {}
    
    def run(self) -> Tuple[pd.DataFrame, StandardizationResult]:
        """Execute Phase 6: Standardization"""
        
        # 1. Unicode normalization on text columns
        text_cols = self._normalize_text()
        
        # 2. Apply domain-specific mappings
        mappings = self._apply_domain_mappings()
        
        # 3. Collapse rare categories
        collapsed = self._collapse_rare_categories()
        
        result = StandardizationResult(
            text_normalized=text_cols,
            categories_collapsed=collapsed,
            mappings_applied=mappings,
            rare_threshold=self.rare_threshold
        )
        
        return self.df, result
    
    def _normalize_text(self) -> List[str]:
        """Apply Unicode NFC normalization + Arabic-specific fixes"""
        text_cols = self.df.select_dtypes(include=['object', 'category']).columns
        normalized: List[str] = []
        
        for col in text_cols:
            # Convert to string and apply NFC normalization
            self.df[col] = self.df[col].astype(str).apply(
                lambda x: normalize('NFC', x) if pd.notna(x) else x
            )
            
            # Arabic-specific normalization
            self.df[col] = self.df[col].apply(self._normalize_arabic)
            
            normalized.append(col)
        
        return normalized
    
    def _normalize_arabic(self, text: str) -> str:
        """Normalize Arabic characters (Alef, Ya, Ta-Marbuta)"""
        if not isinstance(text, str):
            return text
        
        # Normalize different forms of Alef to simple Alef
        text = re.sub('[إأآٱ]', 'ا', text)
        
        # Normalize Ya variations
        text = re.sub('ى', 'ي', text)
        
        # Normalize Ta-Marbuta
        text = re.sub('ة', 'ه', text)
        
        return text
    
    def _apply_domain_mappings(self) -> Dict[str, Dict[str, str]]:
        """Apply domain-specific standardization mappings"""
        mappings: Dict[str, Dict[str, str]] = {}
        
        if self.domain == "logistics":
            mappings = self._logistics_mappings()
        elif self.domain == "healthcare":
            mappings = self._healthcare_mappings()
        elif self.domain == "retail":
            mappings = self._retail_mappings()
        
        # Apply mappings
        for col, mapping in mappings.items():
            if col in self.df.columns:
                # Lowercase for matching
                self.df[col] = self.df[col].apply(lambda v: mapping.get(str(v), mapping.get(str(v).lower(), v)))
        
        return mappings
    
    def _logistics_mappings(self) -> Dict[str, Dict[str, str]]:
        """Logistics-specific mappings"""
        return {
            "carrier": {
                "dhl": "DHL",
                "DHL Express": "DHL",
                "aramex": "Aramex",
                "ARAMEX": "Aramex",
                "smsa": "SMSA",
                "SMSA Express": "SMSA"
            },
            "status": {
                "delivered": "Delivered",
                "DELIVERED": "Delivered",
                "in_transit": "In Transit",
                "IN_TRANSIT": "In Transit",
                "pending": "Pending",
                "returned": "Returned"
            }
        }
    
    def _healthcare_mappings(self) -> Dict[str, Dict[str, str]]:
        """Healthcare-specific mappings"""
        return {
            "department": {
                "emergency": "Emergency",
                "ER": "Emergency",
                "icu": "ICU",
                "intensive care": "ICU",
                "surgery": "Surgery",
                "surgical": "Surgery"
            }
        }
    
    def _retail_mappings(self) -> Dict[str, Dict[str, str]]:
        """Retail-specific mappings"""
        return {
            "payment_method": {
                "credit_card": "Credit Card",
                "cc": "Credit Card",
                "debit": "Debit Card",
                "cash_on_delivery": "COD",
                "cod": "COD"
            }
        }
    
    def _collapse_rare_categories(self) -> Dict[str, int]:
        """Collapse categories with frequency < threshold to 'Other'"""
        collapsed: Dict[str, int] = {}
        
        cat_cols = self.df.select_dtypes(include=['object', 'category']).columns
        
        for col in cat_cols:
            value_counts = self.df[col].value_counts(normalize=True, dropna=False)
            # For small datasets, enforce a practical minimum threshold to avoid tiny classes
            n_rows = len(self.df)
            effective_threshold = self.rare_threshold
            if n_rows <= 1000 and effective_threshold < 0.03:
                effective_threshold = 0.03
            # Use <= to include edge cases exactly on the threshold (e.g., 3%)
            rare_categories = value_counts[value_counts <= effective_threshold].index.tolist()
            
            if len(rare_categories) > 0:
                self.df[col] = self.df[col].apply(
                    lambda x: 'Other' if x in rare_categories else x
                )
                collapsed[col] = len(rare_categories)
        
        return collapsed


