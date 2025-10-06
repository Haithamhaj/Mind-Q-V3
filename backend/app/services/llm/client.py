import os
from typing import Optional

import anthropic
import openai
import google.generativeai as genai
from app.config import settings


class LLMClient:
    """
    Unified LLM client supporting Claude (Anthropic) and GPT-4 (OpenAI)
    """

    def __init__(self, provider: Optional[str] = None, api_key: Optional[str] = None):
        # Prefer explicit args, then env, then settings (shared with BI layer)
        self.provider = provider or os.getenv("LLM_PROVIDER") or settings.llm_provider

        if self.provider == "anthropic":
            key = api_key or os.getenv("LLM_API_KEY") or settings.anthropic_api_key
            if not key:
                raise ValueError("Anthropic API key is missing. Set settings.anthropic_api_key or LLM_API_KEY.")
            self.client = anthropic.Anthropic(api_key=key)
            self.model = "claude-sonnet-4-20250514"
        elif self.provider == "openai":
            key = api_key or os.getenv("LLM_API_KEY") or settings.openai_api_key
            if not key:
                raise ValueError("OpenAI API key is missing. Set settings.openai_api_key or LLM_API_KEY.")
            openai.api_key = key
            self.client = openai
            self.model = "gpt-4"
        elif self.provider == "gemini":
            # Reuse existing BI configuration style
            key = api_key or os.getenv("GEMINI_API_KEY") or settings.gemini_api_key
            if not key:
                raise ValueError("Gemini API key is missing. Set settings.gemini_api_key or GEMINI_API_KEY.")
            genai.configure(api_key=key)
            # Default to lightweight, configurable
            self.client = genai.GenerativeModel("gemini-2.5-flash")
            self.model = "gemini-2.5-flash"
        else:
            raise ValueError(f"Unsupported LLM provider: {self.provider}")

    def call(self, prompt: str, max_tokens: int = 4000, system: Optional[str] = None) -> str:
        """
        Send prompt to LLM and return response text
        """
        try:
            if self.provider == "anthropic":
                messages = [{"role": "user", "content": prompt}]
                kwargs = {"model": self.model, "max_tokens": max_tokens, "messages": messages}
                if system:
                    kwargs["system"] = system
                response = self.client.messages.create(**kwargs)
                return response.content[0].text
            elif self.provider == "openai":
                messages = []
                if system:
                    messages.append({"role": "system", "content": system})
                messages.append({"role": "user", "content": prompt})
                response = self.client.ChatCompletion.create(model=self.model, messages=messages, max_tokens=max_tokens)
                return response.choices[0].message.content
            elif self.provider == "gemini":
                # Gemini doesn't use system prompts the same way; prepend if provided
                full_prompt = f"System: {system}\n\n{prompt}" if system else prompt
                response = self.client.generate_content(full_prompt)
                if hasattr(response, "text") and response.text:
                    return response.text
                # Fallback assemble parts
                if hasattr(response, "parts") and response.parts:
                    return "".join(getattr(p, "text", "") for p in response.parts)
                return ""
        except Exception as e:
            raise Exception(f"LLM API call failed: {str(e)}")


# Global instance
llm_client = LLMClient()


