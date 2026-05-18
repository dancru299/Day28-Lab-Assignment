import os
from pathlib import Path

import requests


def load_env_file(path: str = ".env"):
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key, value.strip().strip('"').strip("'"))


def check_prometheus():
    response = requests.get(
        "http://localhost:9090/api/v1/query",
        params={"query": "up{job='api-gateway'}"},
        timeout=5,
    )
    response.raise_for_status()
    data = response.json()
    assert data["status"] == "success"
    assert data["data"]["result"], "api-gateway target is not visible in Prometheus"
    print("Integration 9 OK: Prometheus metrics flowing")


def check_langsmith():
    api_key = os.environ.get("LANGCHAIN_API_KEY")
    if not api_key:
        print("Integration 10 SKIP: LANGCHAIN_API_KEY is not configured")
        return

    from langsmith import Client

    project = os.environ.get("LANGCHAIN_PROJECT", "lab28-platform")
    client = Client(api_key=api_key)
    runs = list(client.list_runs(project_name=project, limit=1))
    assert len(runs) > 0
    print("Integration 10 OK: LangSmith traces visible")


if __name__ == "__main__":
    load_env_file()
    check_prometheus()
    check_langsmith()
