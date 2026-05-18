import subprocess
from pathlib import Path

import redis
import requests


results = {}


def load_env_file(path: str = ".env"):
    env_path = Path(path)
    if not env_path.exists():
        return {}

    values = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value.strip().strip('"').strip("'")
    return values


def check(name, fn):
    try:
        fn()
        results[name] = "PASS"
        print(f"  [PASS] {name}")
    except Exception as exc:
        results[name] = f"FAIL: {exc}"
        print(f"  [FAIL] {name}: {exc}")


def assert_http_ok(url, **kwargs):
    response = requests.get(url, timeout=5, **kwargs)
    response.raise_for_status()
    return response


print("\n=== RELIABILITY ===")
check("Health check endpoint", lambda: assert_http_ok("http://localhost:8000/health"))
check("API Gateway responds", lambda: assert_http_ok("http://localhost:8000/docs"))


def check_vllm_backed_chat():
    response = requests.post(
        "http://localhost:8000/api/v1/chat",
        json={"query": "Say hello from the Lab 28 vLLM service."},
        timeout=35,
    )
    response.raise_for_status()
    payload = response.json()
    assert payload.get("model") != "local-fallback", "API is using local fallback instead of Kaggle vLLM"
    assert payload.get("answer"), "LLM response is empty"


check("Kaggle vLLM-backed chat", check_vllm_backed_chat)


def check_real_vllm_endpoint():
    env = load_env_file()
    base_url = env.get("VLLM_NGROK_URL", "").rstrip("/")
    assert base_url, "VLLM_NGROK_URL is not configured"
    response = requests.get(
        f"{base_url}/v1/models",
        headers={"ngrok-skip-browser-warning": "true"},
        timeout=30,
    )
    response.raise_for_status()
    owners = {str(item.get("owned_by", "")).lower() for item in response.json().get("data", [])}
    assert "kaggle-compat" not in owners, "Endpoint is the compat server, not real vLLM"


check("Real Kaggle vLLM endpoint", check_real_vllm_endpoint)

print("\n=== OBSERVABILITY ===")
check("Prometheus up", lambda: assert_http_ok("http://localhost:9090/-/healthy"))
check("Grafana up", lambda: assert_http_ok("http://localhost:3000/api/health"))
check("Metrics endpoint exposed", lambda: assert_http_ok("http://localhost:8000/metrics"))

print("\n=== SECURITY ===")


def check_unauthorized():
    response = requests.get("http://localhost:8000/admin", timeout=5)
    assert response.status_code in [401, 403, 404]


check("Unauthorized request rejected", check_unauthorized)

print("\n=== VECTOR STORE ===")
check("Qdrant healthy", lambda: assert_http_ok("http://localhost:6333/healthz"))


def check_collection_exists():
    response = assert_http_ok("http://localhost:6333/collections/documents")
    assert response.json()["result"]["status"] in ["green", "yellow"]


check("Collection exists", check_collection_exists)

print("\n=== FEATURE STORE ===")
check("Redis reachable", lambda: redis.Redis(host="localhost", port=6379).ping())

print("\n=== KAFKA ===")


def check_kafka_topics():
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "kafka-topics",
            "--list",
            "--bootstrap-server",
            "localhost:9092",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "data.raw" in result.stdout


check("Kafka topics exist", check_kafka_topics)

passed = sum(1 for value in results.values() if value == "PASS")
total = len(results)
score = (passed / total) * 100
print(f"\n{'=' * 40}")
print(f"Production Readiness Score: {passed}/{total} = {score:.0f}%")
print(f"Target: >80% -- Status: {'READY' if score >= 80 else 'NOT READY'}")
