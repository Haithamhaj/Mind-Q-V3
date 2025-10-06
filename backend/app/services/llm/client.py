import os
from typing import Optional

from app.config import settings


class LLMConfigurationError(RuntimeError):
    """Raised when the LLM client cannot be configured (missing key, package, etc.)."""


class LLMClient:
    """
    Unified LLM client supporting Claude (Anthropic), GPT-4 (OpenAI), and Gemini.
    Lazy-loads vendor SDKs and credentials on demand so that importing this module
    never breaks the application if keys are missing.
    """

    def __init__(self, provider: Optional[str] = None, api_key: Optional[str] = None):
        self.provider = provider or os.getenv("LLM_PROVIDER") or settings.llm_provider
        self._explicit_api_key = api_key
        self._client = None
        self.model: Optional[str] = None

    def _resolve_api_key(self) -> Optional[str]:
        if self._explicit_api_key:
            return self._explicit_api_key

        if self.provider == "anthropic":
            return os.getenv("LLM_API_KEY") or settings.anthropic_api_key
        if self.provider == "openai":
            return os.getenv("LLM_API_KEY") or settings.openai_api_key
        if self.provider == "gemini":
            return os.getenv("GEMINI_API_KEY") or settings.gemini_api_key
        return None

    def _ensure_client(self) -> None:
        if self._client is not None:
            return

        key = self._resolve_api_key()
        if not key:
            raise LLMConfigurationError(
                f"{self.provider.capitalize() if self.provider else 'LLM'} API key is missing. "
                "Provide LLM_API_KEY or provider-specific key in environment or settings."
            )

        if self.provider == "anthropic":
            try:
                import anthropic
            except ImportError as exc:
                raise LLMConfigurationError("anthropic package is required for Anthropic provider") from exc

            self._client = anthropic.Anthropic(api_key=key)
            self.model = "claude-sonnet-4-20250514"

        elif self.provider == "openai":
            try:
                import openai  # type: ignore
            except ImportError as exc:
                raise LLMConfigurationError("openai package is required for OpenAI provider") from exc

            openai.api_key = key
            self._client = openai
            self.model = "gpt-4"

        elif self.provider == "gemini":
            try:
                import google.generativeai as genai  # type: ignore
            except ImportError as exc:
                raise LLMConfigurationError("google-generativeai package is required for Gemini provider") from exc

            genai.configure(api_key=key)
            self._client = genai.GenerativeModel("gemini-2.5-flash")
            self.model = "gemini-2.5-flash"
        else:
            raise LLMConfigurationError(f"Unsupported LLM provider: {self.provider}")

    def call(self, prompt: str, max_tokens: int = 4000, system: Optional[str] = None) -> str:
        """
        Send prompt to LLM and return response text
        """
        self._ensure_client()

        try:
            if self.provider == "anthropic":
                messages = [{"role": "user", "content": prompt}]
                kwargs = {"model": self.model, "max_tokens": max_tokens, "messages": messages}
                if system:
                    kwargs["system"] = system
                response = self._client.messages.create(**kwargs)  # type: ignore[attr-defined]
                return response.content[0].text

            if self.provider == "openai":
                messages = []
                if system:
                    messages.append({"role": "system", "content": system})
                messages.append({"role": "user", "content": prompt})
                response = self._client.ChatCompletion.create(  # type: ignore[attr-defined]
                    model=self.model,
                    messages=messages,
                    max_tokens=max_tokens,
                )
                return response.choices[0].message.content

            if self.provider == "gemini":
                # Gemini does not use system prompts natively, prepend when provided.
                full_prompt = f"System: {system}\n\n{prompt}" if system else prompt
                response = self._client.generate_content(full_prompt)  # type: ignore[attr-defined]
                if hasattr(response, "text") and response.text:
                    return response.text
                if hasattr(response, "parts") and response.parts:
                    return "".join(getattr(part, "text", "") for part in response.parts)
                return ""

            raise LLMConfigurationError(f"Unsupported LLM provider during call: {self.provider}")

        except LLMConfigurationError:
            raise
        except Exception as exc:
            raise Exception(f"LLM API call failed: {str(exc)}") from exc


_cached_client: Optional[LLMClient] = None


def get_llm_client(force_refresh: bool = False) -> LLMClient:
    """
    Return a cached LLMClient instance, instantiating lazily so imports never fail.
    """
    global _cached_client

    if force_refresh or _cached_client is None:
        _cached_client = LLMClient()

    return _cached_client


__all__ = ["LLMClient", "LLMConfigurationError", "get_llm_client"]


