from __future__ import annotations
import json
import pandas as pd
from typing import Dict, Callable, Optional, List
from pydantic import BaseModel


class ParsedQuery(BaseModel):
    intent: str  # "aggregate", "compare", "trend", "filter", "overview"
    entities: Dict[str, str]
    filters: Dict = {}
    aggregation: str = "mean"
    language: str = "en"
    original: str = ""


class QueryParser:
    """Parse natural language questions into structured queries"""
    
    def __init__(self, domain: str, dataframe: Optional[pd.DataFrame] = None):
        self.domain = domain
        self.dataframe = dataframe
        self.domain_entities = self._load_domain_entities()
        self.data_entities = self._analyze_dataframe() if dataframe is not None else {}
    
    def parse(self, user_question: str, llm_call: Callable[[str], str]) -> ParsedQuery:
        """Parse NL question using LLM"""
        
        # Detect language
        language = self._detect_language(user_question)
        
        # Build prompt
        prompt = self._build_parsing_prompt(user_question, language)
        
        # Call LLM
        try:
            llm_response = llm_call(prompt)
        except Exception as e:
            print(f" LLM Call Error: {e}")
            return ParsedQuery(
                intent="unknown",
                entities={},
                filters={},
                language="en",
                original=user_question,
                aggregation="mean"
            )
        
        # Debug: Print LLM response
        print(f" LLM Response for '{user_question}': {llm_response}")
        print(f" Response type: {type(llm_response)}")
        
        # Handle None or empty responses
        if llm_response is None:
            print(" LLM returned None")
            llm_response = ""
        elif not isinstance(llm_response, str):
            print(f" LLM returned non-string: {type(llm_response)}")
            llm_response = str(llm_response)
        
        # Parse response
        parsed = self._parse_llm_response(llm_response, user_question)
        
        return parsed
    
    def _detect_language(self, text: str) -> str:
        """Detect Arabic vs English"""
        arabic_chars = sum(1 for c in text if '\u0600' <= c <= '\u06FF')
        return "ar" if arabic_chars > len(text) * 0.3 else "en"
    
    def _load_domain_entities(self) -> Dict:
        """Load domain-specific metrics and dimensions"""
        entities = {
            "logistics": {
                "metrics": ["transit_time", "dwell_time", "sla_pct", "rto_pct", "fas_pct"],
                "dimensions": ["carrier", "origin", "destination", "service_type"]
            },
            "healthcare": {
                "metrics": ["showed_up", "age", "date_diff", "scholarship", "hipertension", "diabetes", "alcoholism", "handcap", "sms_received", "patientid", "appointmentid"],
                "dimensions": ["gender", "neighbourhood", "patientid", "appointmentid", "scheduledday", "appointmentday"]
            },
            "retail": {
                "metrics": ["order_value", "basket_size", "return_rate"],
                "dimensions": ["product_category", "payment_method", "region"]
            },
            "emarketing": {
                "metrics": ["ctr", "conversion_rate", "cac", "roas"],
                "dimensions": ["channel", "campaign", "objective"]
            },
            "finance": {
                "metrics": ["balance", "interest_yield", "default_rate"],
                "dimensions": ["account_type", "product_type", "region"]
            }
        }
        
        return entities.get(self.domain, entities["logistics"])
    
    def _analyze_dataframe(self) -> Dict:
        """Analyze the actual dataframe to extract real column information"""
        if self.dataframe is None:
            return {}
        
        df = self.dataframe
        
        # Get column information
        columns_info = {}
        for col in df.columns:
            col_lower = col.lower()
            dtype = str(df[col].dtype)
            
            # Determine if column is numeric, categorical, or datetime
            is_numeric = pd.api.types.is_numeric_dtype(df[col])
            is_datetime = pd.api.types.is_datetime64_any_dtype(df[col])
            is_categorical = df[col].dtype.name == 'category' or (not is_numeric and not is_datetime)
            
            # Get sample values for better understanding
            sample_values = []
            if is_categorical:
                sample_values = df[col].value_counts().head(5).index.tolist()
            elif is_numeric:
                sample_values = [
                    f"min: {df[col].min():.2f}",
                    f"max: {df[col].max():.2f}",
                    f"mean: {df[col].mean():.2f}"
                ]
            elif is_datetime:
                sample_values = df[col].dt.strftime('%Y-%m-%d').unique()[:3].tolist()
            
            columns_info[col] = {
                "type": "numeric" if is_numeric else ("datetime" if is_datetime else "categorical"),
                "dtype": dtype,
                "unique_count": df[col].nunique(),
                "sample_values": sample_values,
                "description": self._generate_column_description(col, is_numeric, is_datetime, is_categorical),
                "suggested_aggregations": self._get_suggested_aggregations(is_numeric, is_datetime, is_categorical)
            }
        
        return {
            "columns": columns_info,
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "column_names": list(df.columns),
            "data_types": {col: str(df[col].dtype) for col in df.columns},
            "numeric_columns": [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])],
            "categorical_columns": [col for col in df.columns if not pd.api.types.is_numeric_dtype(df[col]) and not pd.api.types.is_datetime64_any_dtype(df[col])],
            "datetime_columns": [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
        }
    
    def _get_suggested_aggregations(self, is_numeric: bool, is_datetime: bool, is_categorical: bool) -> List[str]:
        """Get suggested aggregation functions based on data type"""
        if is_numeric:
            return ["mean", "median", "sum", "count", "p90", "p95"]
        elif is_datetime:
            return ["count", "min", "max"]
        else:
            return ["count"] 
    
    def _generate_column_description(self, col_name: str, is_numeric: bool, is_datetime: bool, is_categorical: bool) -> str:
        """Generate human-readable description for columns"""
        col_lower = col_name.lower()
        
        # Healthcare-specific descriptions
        if self.domain == "healthcare":
            if "patient" in col_lower and "id" in col_lower:
                return "Unique patient identifier"
            elif "appointment" in col_lower and "id" in col_lower:
                return "Unique appointment identifier"
            elif "gender" in col_lower:
                return "Patient gender (F/M)"
            elif "age" in col_lower:
                return "Patient age in years"
            elif "neighbourhood" in col_lower or "neighborhood" in col_lower:
                return "Patient neighborhood/location"
            elif "scholarship" in col_lower:
                return "Whether patient has scholarship (True/False)"
            elif "hipertension" in col_lower or "hypertension" in col_lower:
                return "Patient has hypertension (True/False)"
            elif "diabetes" in col_lower:
                return "Patient has diabetes (True/False)"
            elif "alcoholism" in col_lower:
                return "Patient has alcoholism (True/False)"
            elif "handcap" in col_lower or "handicap" in col_lower:
                return "Patient has handicap (True/False)"
            elif "sms" in col_lower:
                return "SMS reminder sent (True/False)"
            elif "show" in col_lower and "up" in col_lower:
                return "Patient showed up for appointment (True/False)"
            elif "date" in col_lower and "diff" in col_lower:
                return "Days between scheduled and appointment date"
            elif "scheduled" in col_lower and "day" in col_lower:
                return "Date when appointment was scheduled"
            elif "appointment" in col_lower and "day" in col_lower:
                return "Date of the actual appointment"
        
        # Generic descriptions
        if is_numeric:
            return f"Numeric data: {col_name}"
        elif is_datetime:
            return f"Date/time data: {col_name}"
        else:
            return f"Categorical data: {col_name}"
    
    def _build_parsing_prompt(self, question: str, language: str) -> str:
        """Build LLM prompt for parsing"""
        
        if self.data_entities:
            data_str = json.dumps(self.data_entities, indent=2, ensure_ascii=False)
            entities_context = f"""
ACTUAL DATA STRUCTURE:
{data_str}

This is the real data you're analyzing. Use the actual column names and their descriptions.
IMPORTANT: Only use aggregation functions that are appropriate for each column type:
- For numeric columns: mean, median, sum, count, p90, p95
- For categorical/string columns: count only
- For datetime columns: count, min, max
"""
        else:
            entities_str = json.dumps(self.domain_entities, indent=2, ensure_ascii=False)
            entities_context = f"""
Available entities in {self.domain}:
{entities_str}
"""
        
        if language == "ar":
            prompt = f"""أنت محلل بيانات. حلل السؤال التالي وحوّله إلى structured query.

السؤال: {question}

{entities_context}

أعد JSON فقط (بدون نص آخر):
{{
  "intent": "aggregate|compare|trend|filter|overview",
  "entities": {{"metric": "...", "dimension": "..."}},
  "filters": {{}},
  "aggregation": "mean|median|sum|count|p90|p95",
  "language": "ar"
}}

مثال:
السؤال: "عن ايش بتحكي الداتا؟"
{{
  "intent": "overview",
  "entities": {{}},
  "filters": {{}},
  "aggregation": "mean",
  "language": "ar"
}}

    السؤال: "كم عدد المرضى؟"
    {{
      "intent": "aggregate",
      "entities": {{"metric": "PatientId"}},
      "filters": {{}},
      "aggregation": "count",
      "language": "ar"
    }}

    السؤال: "علاقة جنس المريض بعدد الاسرة؟"
    {{
      "intent": "compare",
      "entities": {{"metric": "PatientId", "dimension": "Gender"}},
      "filters": {{}},
      "aggregation": "count",
      "language": "ar"
    }}

    السؤال: "متوسط عمر المرضى؟"
    {{
      "intent": "aggregate",
      "entities": {{"metric": "Age"}},
      "filters": {{}},
      "aggregation": "mean",
      "language": "ar"
    }}
"""
        else:
            prompt = f"""You are a data analyst. Parse this question into a structured query.

Question: {question}

{entities_context}

Return ONLY JSON:
{{
  "intent": "aggregate|compare|trend|filter|overview",
  "entities": {{"metric": "...", "dimension": "..."}},
  "filters": {{}},
  "aggregation": "mean|median|sum|count|p90|p95",
  "language": "en"
}}

Example:
Question: "What are the data talking about?"
{{
  "intent": "overview",
  "entities": {{}},
  "filters": {{}},
  "aggregation": "mean",
  "language": "en"
}}

    Question: "How many patients are there?"
    {{
      "intent": "aggregate",
      "entities": {{"metric": "PatientId"}},
      "filters": {{}},
      "aggregation": "count",
      "language": "en"
    }}

    Question: "Relationship between patient gender and number of beds?"
    {{
      "intent": "compare",
      "entities": {{"metric": "PatientId", "dimension": "Gender"}},
      "filters": {{}},
      "aggregation": "count",
      "language": "en"
    }}

    Question: "What is the average age of patients?"
    {{
      "intent": "aggregate",
      "entities": {{"metric": "Age"}},
      "filters": {{}},
      "aggregation": "mean",
      "language": "en"
    }}
"""
        
        return prompt
    
    def _parse_llm_response(self, llm_response: str, original: str) -> ParsedQuery:
        """Parse LLM JSON response"""
        
        print(f" Raw LLM Response: {repr(llm_response)}")
        
        cleaned = llm_response.strip()
        
        # Try to extract JSON from various formats
        if cleaned.startswith("```"):
            parts = cleaned.split("```")
            if len(parts) >= 2:
                cleaned = parts[1].strip()
                if cleaned.startswith("json"):
                    cleaned = cleaned[4:].strip()
        
        # Try to find JSON object in the response
        import re
        json_match = re.search(r'\{.*\}', cleaned, re.DOTALL)
        if json_match:
            cleaned = json_match.group(0)
        
        print(f" Extracted JSON: {repr(cleaned)}")
        
        try:
            parsed_json = json.loads(cleaned)
            print(f" Parsed JSON: {parsed_json}")
            
            # Ensure required fields exist with defaults
            parsed_json["original"] = original
            
            # Ensure entities is a dict
            if "entities" not in parsed_json or not isinstance(parsed_json["entities"], dict):
                parsed_json["entities"] = {}
            
            # Ensure filters is a dict
            if "filters" not in parsed_json or not isinstance(parsed_json["filters"], dict):
                parsed_json["filters"] = {}
            
            # Ensure intent exists
            if "intent" not in parsed_json:
                parsed_json["intent"] = "unknown"
            
            # Ensure language exists
            if "language" not in parsed_json:
                parsed_json["language"] = "en"
            
            # Ensure aggregation exists
            if "aggregation" not in parsed_json:
                parsed_json["aggregation"] = "mean"
            
            print(f" Final parsed data: {parsed_json}")
            
            return ParsedQuery(**parsed_json)
            
        except json.JSONDecodeError as e:
            print(f" JSON Decode Error: {e}")
            print(f" LLM Response: {llm_response}")
            print(f" Cleaned: {cleaned}")
        except Exception as e:
            print(f" General Error: {e}")
            print(f" LLM Response: {llm_response}")
            print(f" Cleaned: {cleaned}")
        
        # Fallback - create a safe default response
        print(" Using fallback response")
        return ParsedQuery(
            intent="unknown",
            entities={},
            filters={},
            language="en",
            original=original,
            aggregation="mean"
        )
