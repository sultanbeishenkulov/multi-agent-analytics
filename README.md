# Multi-Agent Analytics

Ask a question in plain English, get back SQL, a chart, and a written summary — all from a team of AI agents working together.

## What it does

You type something like *"Which regions have the highest revenue from Enterprise customers?"* and the system:

1. Pulls relevant context from a knowledge base (RAG)
2. Writes and validates a PostgreSQL query
3. Executes it and interprets the results
4. Builds an interactive Plotly chart
5. Writes a plain-English summary with key findings and follow-up suggestions

It also remembers previous questions in a session, so follow-ups like *"Now break that down by product category"* work as expected.

## How it's built

The pipeline runs as a [LangGraph](https://github.com/langchain-ai/langgraph) state graph with four nodes wired together:

```
context → sql → interpret → viz
```

**Context node** — retrieves relevant docs from a FAISS vector store and optionally fetches live web results via DuckDuckGo (triggered for questions about benchmarks, trends, industry data, etc.)

**SQL node** — sends the question + schema + context to an LLM, parses the JSON response, validates the query with `EXPLAIN`, and retries up to twice if something's wrong

**Interpret node** — reads the query results and writes a summary, key findings, and follow-up question suggestions

**Viz node** — picks the right chart type and builds a Plotly figure

The whole thing is exposed as a REST API (FastAPI) with a streaming endpoint that sends each node's output as it finishes, so a frontend can show progress in real time.

## Stack

| Layer | Choice |
|---|---|
| Orchestration | LangGraph |
| LLM | Ollama (local) or OpenAI — switchable via env var |
| Database | PostgreSQL |
| Embeddings | sentence-transformers + FAISS |
| Vector search | FAISS |
| Web search | DuckDuckGo (no API key needed) |
| API | FastAPI + SSE streaming |
| Charts | Plotly |
| Memory | SQLite (session-based conversation history) |
| Deployment | Docker Compose |

## Benchmark

There's a 40-question benchmark that scores the SQL agent on execution accuracy — it runs the ground-truth query and the agent-generated query, then compares result sets.

Running it against both the local model and GPT-4o:

```
                   qwen2.5-coder:7b    gpt-4o
Easy   (10 qs)     10/10  (100%)       9/10   (90%)
Medium (15 qs)      9/15   (60%)       8/15   (53%)
Hard   (15 qs)      2/15   (13%)       3/15   (20%)
Total  (40 qs)     21/40  (52.5%)     20/40  (50.0%)
```

The local 7B model running on a laptop is roughly even with GPT-4o on this dataset. Most failures on both sides are complex multi-table joins and percentage calculations where column ordering or extra selected columns break the exact-match comparison.

## Running it

```bash
# Start the database
docker compose up db -d

# Install dependencies
pip install -r requirements.txt

# Run a question from the CLI
python -m agents.orchestrator "What is the total revenue per region?"

# Start the API
uvicorn api:app --reload

# Run the benchmark
python -m benchmark.run --provider both
```

The LLM backend defaults to Ollama (local). To use OpenAI, set `LLM_PROVIDER=openai` and `OPENAI_API_KEY` in a `.env` file.

## License

MIT
