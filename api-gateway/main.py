import os
import time
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field


try:
    from langsmith import traceable
except Exception:
    def traceable(*args, **kwargs):
        def decorator(func):
            return func

        return decorator


app = FastAPI(title="AI Platform API Gateway")
Instrumentator().instrument(app).expose(app)

VLLM_URL = os.getenv("VLLM_URL") or os.getenv("VLLM_NGROK_URL") or ""
EMBED_URL = os.getenv("EMBED_URL") or os.getenv("EMBED_NGROK_URL") or ""
QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
MODEL_NAME = os.getenv("MODEL_NAME", "Qwen/Qwen2.5-0.5B-Instruct")
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION", "documents")
VECTOR_SIZE = int(os.getenv("VECTOR_SIZE", "384"))
TIMEOUT_SECONDS = float(os.getenv("API_TIMEOUT_SECONDS", "30"))
ALLOW_LLM_FALLBACK = os.getenv("ALLOW_LLM_FALLBACK", "false").lower() == "true"


class ChatRequest(BaseModel):
    query: str = Field(..., min_length=1)
    embedding: list[float] | None = None


def _valid_embedding(embedding: list[float] | None) -> bool:
    return embedding is not None and len(embedding) == VECTOR_SIZE


async def build_embedding(query: str, embedding: list[float] | None) -> list[float]:
    if _valid_embedding(embedding):
        return embedding or []

    if EMBED_URL:
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS) as client:
                response = await client.post(
                    f"{EMBED_URL.rstrip('/')}/embed",
                    json={"texts": [query]},
                    headers={"ngrok-skip-browser-warning": "true"},
                )
                response.raise_for_status()
                vector = response.json()["embeddings"][0]
                if len(vector) == VECTOR_SIZE:
                    return vector
        except Exception:
            pass

    return [0.0] * VECTOR_SIZE


async def search_context(vector: list[float]) -> list[dict[str, Any]]:
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS) as client:
            response = await client.post(
                f"{QDRANT_URL.rstrip('/')}/collections/{COLLECTION_NAME}/points/search",
                json={"vector": vector, "limit": 3, "with_payload": True},
            )
            response.raise_for_status()
            return response.json().get("result", [])
    except Exception:
        return []


def fallback_answer(query: str, context: list[dict[str, Any]]) -> dict[str, Any]:
    context_note = (
        f"Retrieved {len(context)} context item(s) from Qdrant."
        if context
        else "No vector context was available."
    )
    return {
        "answer": (
            "Fallback response: the platform is reachable and handled the request. "
            f"{context_note} Query: {query}"
        ),
        "model": "local-fallback",
    }


@traceable(name="lab28_call_llm")
async def call_llm(query: str, context: list[dict[str, Any]]) -> dict[str, Any]:
    if not VLLM_URL:
        if ALLOW_LLM_FALLBACK:
            return fallback_answer(query, context)
        raise HTTPException(status_code=503, detail="VLLM_URL is not configured")

    prompt = f"Context: {context}\n\nQuery: {query}"
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS) as client:
            response = await client.post(
                f"{VLLM_URL.rstrip('/')}/v1/chat/completions",
                json={
                    "model": MODEL_NAME,
                    "messages": [{"role": "user", "content": prompt}],
                },
                headers={"ngrok-skip-browser-warning": "true"},
            )
            response.raise_for_status()
            result = response.json()
            return {
                "answer": result["choices"][0]["message"]["content"],
                "model": result.get("model", MODEL_NAME),
            }
    except HTTPException:
        raise
    except Exception as exc:
        if ALLOW_LLM_FALLBACK:
            return fallback_answer(query, context)
        raise HTTPException(status_code=502, detail=f"LLM request failed: {exc}") from exc


@app.post("/api/v1/chat")
async def chat(payload: ChatRequest):
    start = time.time()
    vector = await build_embedding(payload.query, payload.embedding)
    context = await search_context(vector)
    result = await call_llm(payload.query, context)
    latency = (time.time() - start) * 1000
    return {
        "answer": result["answer"],
        "latency_ms": round(latency, 2),
        "model": result["model"],
        "context_count": len(context),
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "vllm_configured": bool(VLLM_URL),
        "embedding_configured": bool(EMBED_URL),
        "qdrant_url": QDRANT_URL,
        "model_name": MODEL_NAME,
        "allow_llm_fallback": ALLOW_LLM_FALLBACK,
    }
