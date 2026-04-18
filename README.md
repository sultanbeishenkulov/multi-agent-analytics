# Multi-Agent Analytics
 
Natural language → SQL → Insights → Charts → Summaries, powered by specialized LLM agents orchestrated with LangGraph.
 
## What this is
 
A multi-agent system where you ask questions in plain English and a team of AI agents collaborates to query a real PostgreSQL database, analyze results, generate visualizations, and write summaries — with memory for follow-up questions.
 
## Architecture (planned)
 
```
User Query → Orchestrator → SQL Agent → Interpreter → Viz Agent → Summary Agent → Response
```
 
## Tech Stack
 
- **Orchestration:** LangGraph
- **LLM:** Claude (Anthropic SDK)
- **Database:** PostgreSQL
- **Viz:** Plotly
- **API:** FastAPI
- **Deployment:** Docker Compose
 
## License
 
MIT
 
