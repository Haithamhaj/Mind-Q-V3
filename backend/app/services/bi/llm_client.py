from __future__ import annotations
import os
from typing import Optional


class LLMClient:
    """LLM API Client - supports GPT-4 (OpenAI) and Gemini (Google)"""
    
    def __init__(self, provider: str = "gemini"):
        self.provider = provider
        
        if provider == "openai":
            try:
                import openai
                openai.api_key = os.environ.get("OPENAI_API_KEY")
                self.client = openai
            except ImportError:
                print("Error: openai package not installed.")
                raise Exception("openai package is required for OpenAI provider")
        elif provider == "gemini":
            try:
                import google.generativeai as genai
                # Try to get API key from settings first, then environment
                try:
                    from app.config import settings
                    api_key = settings.gemini_api_key or os.environ.get("GEMINI_API_KEY")
                except ImportError:
                    api_key = os.environ.get("GEMINI_API_KEY")
                
                if api_key and api_key != "":
                    genai.configure(api_key=api_key)
                    self.client = genai.GenerativeModel('gemini-2.5-flash')
                else:
                    print("Error: GEMINI_API_KEY not set.")
                    raise Exception("GEMINI_API_KEY is required for Gemini provider")
            except ImportError:
                print("Error: google-generativeai package not installed.")
                raise Exception("google-generativeai package is required for Gemini provider")
        else:
            self.client = None
    
    def call(self, prompt: str, max_tokens: int = 1500) -> str:
        """Call LLM API with prompt"""
        
        if self.provider == "openai" and self.client:
            try:
                response = self.client.ChatCompletion.create(
                    model="gpt-4",
                    messages=[
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=max_tokens
                )
                return response.choices[0].message.content
            except Exception as e:
                print(f"OpenAI API error: {e}")
                return self._mock_response(prompt)
                
        elif self.provider == "gemini" and self.client:
            try:
                import google.generativeai as genai
                response = self.client.generate_content(
                    prompt,
                    generation_config=genai.types.GenerationConfig(
                        max_output_tokens=max_tokens,
                        temperature=0.1,
                        top_p=0.8,
                        top_k=40
                    )
                )
                
                if hasattr(response, 'text') and response.text:
                    return response.text
                elif hasattr(response, 'parts') and response.parts:
                    return ''.join([part.text for part in response.parts if hasattr(part, 'text')])
                else:
                    print(f"Gemini response has no text content: {response}")
                    return '{"intent": "unknown", "entities": {}, "filters": {}, "aggregation": "mean", "language": "en"}'
            except Exception as e:
                print(f"Gemini API error: {e}")
                raise Exception(f"Gemini API error: {e}")
        
        else:
            raise Exception(f"No LLM client available for provider: {self.provider}")
    


# Global instance - use provider from settings
try:
    from app.config import settings
    provider = settings.llm_provider
except ImportError:
    provider = os.environ.get("LLM_PROVIDER", "gemini")

llm_client = LLMClient(provider=provider)


def call_llm_api(prompt: str) -> str:
    """Wrapper function for LLM API calls"""
    return llm_client.call(prompt)
