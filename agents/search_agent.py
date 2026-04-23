"""
Web Search Agent — fetches external context using DuckDuckGo (no API key required).

Only triggers for questions that need real-world or current information
(benchmarks, industry trends, news, comparisons to external data).
"""

from __future__ import annotations

import re

from duckduckgo_search import DDGS

_SEARCH_KEYWORDS = re.compile(
    r"\b(benchmark|industry|market|trend|compare|news|current|recent|"
    r"average|standard|typical|competitor|global|worldwide)\b",
    re.IGNORECASE,
)


def _needs_search(question: str) -> bool:
    return bool(_SEARCH_KEYWORDS.search(question))


def run_search_agent(question: str, max_results: int = 3) -> str:
    """Search DuckDuckGo for external context relevant to the question.

    Returns empty string if the question doesn't need external data.
    """
    if not _needs_search(question):
        return ""

    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(question, max_results=max_results))

        if not results:
            return ""

        snippets = "\n\n".join(
            f"**{r['title']}**\n{r['body']}" for r in results
        )
        return f"## External Web Context\n{snippets}"

    except Exception as e:
        return f"## External Web Context\n(Search unavailable: {e})"
