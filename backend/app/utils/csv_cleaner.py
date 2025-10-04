"""
CSV Cleaner Utility - Mind-Q V3
Advanced CSV parsing and cleaning inspired by Mind-Q V2 solutions
"""

import pandas as pd
import io
import re
from typing import Tuple, List, Dict, Any

class CSVCleaner:
    """Advanced CSV cleaner for handling malformed CSV files"""
    
    def __init__(self):
        self.fixes_applied: List[str] = []
        self.warnings: List[str] = []
    
    def clean_and_parse(self, file_content, filename: str) -> Tuple[pd.DataFrame, List[str], List[str]]:
        """
        Advanced CSV parsing with multiple fallback strategies
        Returns: (DataFrame, fixes_applied, warnings)
        """
        self.fixes_applied = []
        self.warnings = []
        
        # Strategy 1: Normal parsing
        try:
            file_content.seek(0)
            df = pd.read_csv(file_content)
            return df, self.fixes_applied, self.warnings
        except Exception as e1:
            self.warnings.append(f"Normal CSV parsing failed: {str(e1)}")
        
        # Strategy 2: Skip bad lines  
        try:
            file_content.seek(0)
            df = pd.read_csv(file_content, on_bad_lines='skip', engine='python')
            if len(df) > 0:
                self.fixes_applied.append("Skipped malformed lines during parsing")
                return df, self.fixes_applied, self.warnings
        except Exception as e2:
            self.warnings.append(f"Skip bad lines strategy failed: {str(e2)}")
        
        # Strategy 3: Manual line-by-line parsing
        try:
            file_content.seek(0)
            df = self._manual_parse_csv(file_content)
            if len(df) > 0:
                self.fixes_applied.append("Applied manual line-by-line CSV parsing")
                return df, self.fixes_applied, self.warnings
        except Exception as e3:
            self.warnings.append(f"Manual parsing strategy failed: {str(e3)}")
        
        # Strategy 4: Quote handling
        try:
            file_content.seek(0)
            df = pd.read_csv(
                file_content, 
                quoting=1,  # QUOTE_ALL
                escapechar='\\',
                on_bad_lines='skip',
                engine='python'
            )
            if len(df) > 0:
                self.fixes_applied.append("Applied aggressive quote handling")
                return df, self.fixes_applied, self.warnings
        except Exception as e4:
            self.warnings.append(f"Quote handling strategy failed: {str(e4)}")
        
        # If all strategies fail
        raise Exception("All CSV parsing strategies failed. File is severely malformed.")
    
    def _manual_parse_csv(self, file_content) -> pd.DataFrame:
        """
        Manual CSV parsing - reads line by line and fixes common issues
        """
        file_content.seek(0)
        lines = file_content.read().decode('utf-8', errors='ignore').splitlines()
        
        if len(lines) < 2:
            raise Exception("File has less than 2 lines")
        
        # Get header
        header_line = lines[0]
        headers = self._parse_csv_line(header_line)
        expected_cols = len(headers)
        
        # Parse data lines
        data_rows = []
        skipped_lines = 0
        
        for i, line in enumerate(lines[1:], 2):  # Start from line 2
            try:
                values = self._parse_csv_line(line)
                
                # Fix column count mismatch
                if len(values) > expected_cols:
                    # Too many values - truncate or merge
                    values = values[:expected_cols]
                    if i <= 10:  # Log first few fixes
                        self.fixes_applied.append(f"Line {i}: truncated {len(values)} extra columns")
                elif len(values) < expected_cols:
                    # Too few values - pad with NaN
                    values.extend([None] * (expected_cols - len(values)))
                    if i <= 10:  # Log first few fixes
                        self.fixes_applied.append(f"Line {i}: padded {expected_cols - len(values)} missing columns")
                
                data_rows.append(values)
                
            except Exception:
                skipped_lines += 1
                if skipped_lines <= 10:  # Log first few skips
                    self.fixes_applied.append(f"Skipped malformed line {i}")
        
        if skipped_lines > 10:
            self.fixes_applied.append(f"Skipped {skipped_lines - 10} additional malformed lines")
        
        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=headers)
        
        # Clean up common issues
        df = self._post_process_dataframe(df)
        
        return df
    
    def _parse_csv_line(self, line: str) -> List[str]:
        """Parse a single CSV line handling quotes and commas"""
        # Simple CSV parser handling common quote issues
        values = []
        current_value = ""
        in_quotes = False
        i = 0
        
        while i < len(line):
            char = line[i]
            
            if char == '"':
                if in_quotes and i + 1 < len(line) and line[i + 1] == '"':
                    # Escaped quote
                    current_value += '"'
                    i += 2
                else:
                    in_quotes = not in_quotes
                    i += 1
            elif char == ',' and not in_quotes:
                values.append(current_value.strip())
                current_value = ""
                i += 1
            else:
                current_value += char
                i += 1
        
        # Add last value
        values.append(current_value.strip())
        
        return values
    
    def _post_process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Post-process DataFrame to fix common issues"""
        original_shape = df.shape
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Remove completely empty columns
        df = df.dropna(how='all', axis=1)
        
        # Clean column names
        df.columns = [str(col).strip().replace('\n', '').replace('\r', '') for col in df.columns]
        
        # Remove duplicate column names
        seen_cols = set()
        new_cols = []
        for col in df.columns:
            if col in seen_cols:
                counter = 1
                new_col = f"{col}_{counter}"
                while new_col in seen_cols:
                    counter += 1
                    new_col = f"{col}_{counter}"
                new_cols.append(new_col)
                seen_cols.add(new_col)
            else:
                new_cols.append(col)
                seen_cols.add(col)
        
        df.columns = new_cols
        
        if df.shape != original_shape:
            self.fixes_applied.append(f"Post-processing: {original_shape} → {df.shape}")
        
        return df



