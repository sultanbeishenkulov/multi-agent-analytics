"""
Shared LLM client — supports Ollama (local) and OpenAI.

Default provider is controlled by LLM_PROVIDER env var (ollama or openai).
The chat() function also accepts a runtime provider override so the
benchmark runner can compare both models in one process.
"""

from __future__ import annotations

import os
import re

_DEFAULT_PROVIDER = os.getenv("LLM_PROVIDER", "ollama").lower()
_OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
_OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")
_OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")

_openai_client = None
_ollama_client = None


def _get_openai():
    global _openai_client
    if _openai_client is None:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _openai_client


def _get_ollama():
    global _ollama_client
    if _ollama_client is None:
        import ollama
        _ollama_client = ollama.Client(host=_OLLAMA_HOST)
    return _ollama_client


def get_model_name(provider: str | None = None) -> str:
    p = (provider or _DEFAULT_PROVIDER).lower()
    return _OPENAI_MODEL if p == "openai" else _OLLAMA_MODEL


def chat(system: str, user: str, provider: str | None = None) -> str:
    """Send a system + user message and return the response text.

    provider overrides LLM_PROVIDER for this single call.
    """
    p = (provider or _DEFAULT_PROVIDER).lower()

    if p == "openai":
        client = _get_openai()
        response = client.chat.completions.create(
            model=_OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        content = response.choices[0].message.content.strip()
    else:
        client = _get_ollama()
        response = client.chat(
            model=_OLLAMA_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        content = response["message"]["content"].strip()

    content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
    content = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", content)
    return content
