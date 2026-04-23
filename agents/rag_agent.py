"""
RAG Agent — retrieves relevant business context from local documents using FAISS.

On first call, builds a FAISS index from markdown files in data/documents/.
The index is persisted to data/faiss_index/ so subsequent starts are instant.

Returns a plain-text context string to be injected into the SQL agent's prompt.
"""

from __future__ import annotations

from pathlib import Path

from langchain_community.document_loaders import DirectoryLoader, TextLoader
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter

DOCS_PATH = Path(__file__).parent.parent / "data" / "documents"
INDEX_PATH = Path(__file__).parent.parent / "data" / "faiss_index"
EMBED_MODEL = "all-MiniLM-L6-v2"

_index: FAISS | None = None


def _get_index() -> FAISS:
    global _index
    if _index is not None:
        return _index

    embeddings = HuggingFaceEmbeddings(model_name=EMBED_MODEL)

    if (INDEX_PATH / "index.faiss").exists():
        _index = FAISS.load_local(
            str(INDEX_PATH),
            embeddings,
            allow_dangerous_deserialization=True,
        )
        return _index

    # Build index from documents
    loader = DirectoryLoader(str(DOCS_PATH), glob="**/*.md", loader_cls=TextLoader)
    docs = loader.load()

    splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
    chunks = splitter.split_documents(docs)

    _index = FAISS.from_documents(chunks, embeddings)
    INDEX_PATH.mkdir(parents=True, exist_ok=True)
    _index.save_local(str(INDEX_PATH))

    return _index


def run_rag_agent(question: str, k: int = 3) -> str:
    """Retrieve the top-k most relevant document chunks for the question.

    Returns a formatted string ready to be injected into a prompt.
    """
    try:
        index = _get_index()
        docs = index.similarity_search(question, k=k)
        if not docs:
            return ""
        chunks = "\n\n".join(doc.page_content for doc in docs)
        return f"## Retrieved Business Context\n{chunks}"
    except Exception as e:
        return f"## Retrieved Business Context\n(RAG unavailable: {e})"
