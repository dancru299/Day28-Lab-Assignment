import sys
import time
from pathlib import Path

import pytest
import requests


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pipeline_utils import send_records_to_kafka, stable_point_id  # noqa: E402


BASE_URL = "http://localhost:8000"
QDRANT_URL = "http://localhost:6333"
PROMETHEUS_URL = "http://localhost:9090"
GRAFANA_URL = "http://localhost:3000"


def wait_until(fn, timeout: int = 75, interval: float = 3):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            result = fn()
            if result:
                return result
        except Exception as exc:
            last_error = exc
        time.sleep(interval)
    if last_error:
        raise AssertionError(f"Timed out waiting for condition: {last_error}")
    raise AssertionError("Timed out waiting for condition")


class TestHappyPath:
    def test_health_check_passes(self):
        resp = requests.get(f"{BASE_URL}/health", timeout=5)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_full_inference_returns_200(self):
        resp = requests.post(
            f"{BASE_URL}/api/v1/chat",
            json={
                "query": "What is platform engineering?",
                "embedding": [0.1] * 384,
            },
            timeout=35,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["answer"]) > 10
        assert data["latency_ms"] < 35000
        assert "model" in data
        assert data["model"] != "local-fallback"


class TestDataIngestion:
    def test_kafka_ingest_prefect_redis_and_qdrant(self):
        record_id = f"smoke_{int(time.time())}"
        send_records_to_kafka(
            [
                {
                    "id": record_id,
                    "text": "smoke test document for Lab 28 platform",
                    "timestamp": time.time(),
                }
            ]
        )

        def redis_has_feature():
            import redis

            client = redis.Redis(host="localhost", port=6379, decode_responses=True)
            return client.exists(f"feature:{record_id}") == 1

        assert wait_until(redis_has_feature)

        point_id = stable_point_id(record_id)

        def qdrant_has_point():
            resp = requests.post(
                f"{QDRANT_URL}/collections/documents/points",
                json={"ids": [point_id], "with_payload": True},
                timeout=5,
            )
            if resp.status_code != 200:
                return False
            return len(resp.json().get("result", [])) == 1

        assert wait_until(qdrant_has_point)


class TestObservability:
    def test_prometheus_scrapes_api_gateway(self):
        def prometheus_has_target():
            resp = requests.get(
                f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": "up{job='api-gateway'}"},
                timeout=5,
            )
            resp.raise_for_status()
            result = resp.json()["data"]["result"]
            return result and result[0]["value"][1] == "1"

        assert wait_until(prometheus_has_target, timeout=45)

    def test_grafana_dashboard_accessible(self):
        resp = requests.get(
            f"{GRAFANA_URL}/api/health",
            auth=("admin", "admin"),
            timeout=5,
        )
        assert resp.status_code == 200


class TestFailurePath:
    def test_invalid_request_returns_422(self):
        resp = requests.post(f"{BASE_URL}/api/v1/chat", json={}, timeout=5)
        assert resp.status_code == 422

    def test_timeout_does_not_crash_service(self):
        try:
            requests.post(
                f"{BASE_URL}/api/v1/chat",
                json={"query": "test", "embedding": [0.1] * 384},
                timeout=0.001,
            )
        except requests.exceptions.Timeout:
            pass

        health = requests.get(f"{BASE_URL}/health", timeout=5)
        assert health.status_code == 200


class TestFeatureStore:
    def test_feast_redis_has_features(self):
        import redis

        client = redis.Redis(host="localhost", port=6379, decode_responses=True)
        keys = client.keys("feature:*")
        assert len(keys) > 0, "No features found in Redis feature store"
