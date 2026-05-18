import os
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[1]


def load_env_file(path: Path = ROOT / ".env") -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, values[key])
    return values


def require(value: str | None, name: str) -> str:
    if not value:
        raise AssertionError(f"{name} is not configured")
    return value.rstrip("/")


def get_model_ids(payload: dict) -> list[str]:
    return [item.get("id", "") for item in payload.get("data", []) if item.get("id")]


def assert_real_vllm(payload: dict):
    owners = {str(item.get("owned_by", "")).lower() for item in payload.get("data", [])}
    assert "kaggle-compat" not in owners, "Endpoint is the compat server, not real vLLM"


def main():
    env = load_env_file()
    vllm_url = require(env.get("VLLM_NGROK_URL") or os.getenv("VLLM_NGROK_URL"), "VLLM_NGROK_URL")
    embed_url = require(env.get("EMBED_NGROK_URL") or os.getenv("EMBED_NGROK_URL"), "EMBED_NGROK_URL")
    model_name = env.get("MODEL_NAME") or os.getenv("MODEL_NAME") or "Qwen/Qwen2.5-0.5B-Instruct"
    headers = {"ngrok-skip-browser-warning": "true"}

    print("Checking Kaggle vLLM /v1/models ...")
    models_response = requests.get(f"{vllm_url}/v1/models", headers=headers, timeout=60)
    models_response.raise_for_status()
    models_payload = models_response.json()
    assert_real_vllm(models_payload)
    model_ids = get_model_ids(models_payload)
    assert model_ids, "Kaggle vLLM returned no models"
    assert model_name in model_ids, (
        f"MODEL_NAME={model_name} is not served by Kaggle vLLM. "
        f"Available models: {', '.join(model_ids)}"
    )
    print(f"  OK: {', '.join(model_ids)}")

    print("Checking Kaggle vLLM /v1/chat/completions ...")
    chat_response = requests.post(
        f"{vllm_url}/v1/chat/completions",
        headers=headers,
        json={
            "model": model_name,
            "messages": [{"role": "user", "content": "Reply with one short sentence from vLLM."}],
            "max_tokens": 64,
            "temperature": 0.1,
        },
        timeout=120,
    )
    chat_response.raise_for_status()
    answer = chat_response.json()["choices"][0]["message"]["content"].strip()
    assert answer, "Kaggle vLLM returned an empty answer"
    print(f"  OK: {answer[:120]}")

    print("Checking Kaggle embedding /embed ...")
    embed_response = requests.post(
        f"{embed_url}/embed",
        headers=headers,
        json={"texts": ["hello from lab 28"]},
        timeout=60,
    )
    embed_response.raise_for_status()
    vector = embed_response.json()["embeddings"][0]
    assert len(vector) == int(env.get("VECTOR_SIZE", os.getenv("VECTOR_SIZE", "384")))
    print(f"  OK: embedding size {len(vector)}")

    print("Checking local API Gateway is not using fallback ...")
    local_response = requests.post(
        "http://localhost:8000/api/v1/chat",
        json={"query": "What model is answering this request?"},
        timeout=120,
    )
    local_response.raise_for_status()
    local_payload = local_response.json()
    assert local_payload.get("model") != "local-fallback", local_payload
    assert local_payload.get("model") == model_name, local_payload
    print(f"  OK: local API is backed by {local_payload['model']}")

    print("Kaggle vLLM verification OK")


if __name__ == "__main__":
    main()
