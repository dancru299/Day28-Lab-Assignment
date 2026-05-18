import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


TOPIC = os.getenv("KAFKA_TOPIC", "data.raw")
VECTOR_SIZE = int(os.getenv("VECTOR_SIZE", "384"))
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION", "documents")


def sample_records() -> list[dict]:
    now = time.time()
    return [
        {
            "id": "doc_001",
            "text": "AI platform integration test",
            "timestamp": now,
        },
        {
            "id": "doc_002",
            "text": "Kafka to Delta to vector store pipeline",
            "timestamp": now,
        },
    ]


def stable_record_id(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def stable_point_id(record_id: str) -> int:
    digest = hashlib.sha256(record_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)


def normalize_record(record: dict) -> dict:
    text = str(record.get("text", "")).strip() or "(empty document)"
    record_id = str(record.get("id") or stable_record_id(text))
    timestamp = record.get("timestamp") or time.time()
    return {**record, "id": record_id, "text": text, "timestamp": timestamp}


def ensure_kafka_topic(
    bootstrap_servers: str | None = None,
    topic: str = TOPIC,
    partitions: int = 1,
    replication_factor: int = 1,
) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    bootstrap_servers = bootstrap_servers or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="lab28-topic-admin",
    )
    try:
        admin.create_topics(
            [
                NewTopic(
                    name=topic,
                    num_partitions=partitions,
                    replication_factor=replication_factor,
                )
            ],
            validate_only=False,
        )
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def send_records_to_kafka(
    records: Iterable[dict],
    bootstrap_servers: str | None = None,
    topic: str = TOPIC,
) -> int:
    from kafka import KafkaProducer

    bootstrap_servers = bootstrap_servers or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    ensure_kafka_topic(bootstrap_servers=bootstrap_servers, topic=topic)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    count = 0
    try:
        for record in records:
            normalized = normalize_record(record)
            producer.send(topic, value=normalized)
            print(f"Sent to Kafka topic {topic}: {normalized['id']}")
            count += 1
        producer.flush()
    finally:
        producer.close()
    return count


def consume_kafka_records(
    bootstrap_servers: str | None = None,
    topic: str = TOPIC,
    group_id: str | None = None,
    timeout_ms: int = 5000,
    max_records: int = 100,
) -> list[dict]:
    from kafka import KafkaConsumer

    bootstrap_servers = bootstrap_servers or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    group_id = group_id or os.getenv("KAFKA_CONSUMER_GROUP", "lab28-prefect")
    ensure_kafka_topic(bootstrap_servers=bootstrap_servers, topic=topic)
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
    )

    records: list[dict] = []
    try:
        for message in consumer:
            records.append(normalize_record(message.value))
            if len(records) >= max_records:
                break
    finally:
        consumer.close()
    return records


def delta_path() -> Path:
    return Path(os.getenv("DELTA_PATH", "delta-lake/raw"))


def save_records_to_delta(records: list[dict], path: Path | None = None) -> Path | None:
    if not records:
        return None

    path = path or delta_path()
    path.mkdir(parents=True, exist_ok=True)
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    normalized_records = [normalize_record(record) for record in records]

    try:
        import pandas as pd

        batch_path = path / f"batch_{batch_id}.parquet"
        pd.DataFrame(normalized_records).to_parquet(batch_path)
        return batch_path
    except Exception as exc:
        batch_path = path / f"batch_{batch_id}.jsonl"
        with batch_path.open("w", encoding="utf-8") as handle:
            for record in normalized_records:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        print(f"Parquet unavailable ({exc}); wrote JSONL fallback: {batch_path}")
        return batch_path


def load_records_from_delta(path: Path | None = None) -> list[dict]:
    path = path or delta_path()
    records: list[dict] = []

    parquet_files = sorted(path.glob("*.parquet"))
    if parquet_files:
        try:
            import pandas as pd

            frames = [pd.read_parquet(file) for file in parquet_files]
            df = pd.concat(frames, ignore_index=True)
            records.extend(df.to_dict(orient="records"))
        except Exception as exc:
            print(f"Skipping parquet read because dependencies are unavailable: {exc}")

    for jsonl_file in sorted(path.glob("*.jsonl")):
        with jsonl_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    records.append(json.loads(line))

    return [normalize_record(record) for record in records]


def push_records_to_redis(records: Iterable[dict], redis_url: str | None = None) -> int:
    import redis

    redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
    client = redis.from_url(redis_url, decode_responses=True)
    count = 0
    for record in records:
        normalized = normalize_record(record)
        client.set(
            f"feature:{normalized['id']}",
            json.dumps(
                {
                    "text": normalized["text"],
                    "timestamp": normalized["timestamp"],
                    "processed": True,
                }
            ),
        )
        count += 1
    return count


def deterministic_embedding(text: str, size: int = VECTOR_SIZE) -> list[float]:
    values: list[float] = []
    seed = text.encode("utf-8")
    counter = 0
    while len(values) < size:
        digest = hashlib.sha256(seed + counter.to_bytes(4, "big")).digest()
        values.extend(((byte / 127.5) - 1.0) for byte in digest)
        counter += 1
    return values[:size]


def embed_texts(texts: list[str], embed_url: str | None = None) -> list[list[float]]:
    import requests

    embed_url = (
        embed_url or os.getenv("EMBED_URL") or os.getenv("EMBED_NGROK_URL") or ""
    ).rstrip("/")
    allow_fallback = os.getenv("ALLOW_LOCAL_EMBED_FALLBACK", "true").lower() == "true"

    if embed_url:
        try:
            response = requests.post(
                f"{embed_url}/embed",
                json={"texts": texts},
                headers={"ngrok-skip-browser-warning": "true"},
                timeout=float(os.getenv("API_TIMEOUT_SECONDS", "30")),
            )
            response.raise_for_status()
            embeddings = response.json()["embeddings"]
            if all(len(vector) == VECTOR_SIZE for vector in embeddings):
                return embeddings
            raise ValueError(f"embedding size mismatch, expected {VECTOR_SIZE}")
        except Exception:
            if not allow_fallback:
                raise

    if not allow_fallback:
        raise RuntimeError("EMBED_URL is not configured and fallback is disabled")
    return [deterministic_embedding(text) for text in texts]


def qdrant_base_url() -> str:
    qdrant_url = os.getenv("QDRANT_URL", "").strip()
    if qdrant_url:
        return qdrant_url.rstrip("/")
    host = os.getenv("QDRANT_HOST", "localhost")
    port = os.getenv("QDRANT_PORT", "6333")
    return f"http://{host}:{port}"


def ensure_qdrant_collection(collection_name: str = COLLECTION_NAME) -> None:
    import requests

    base_url = qdrant_base_url()
    response = requests.get(f"{base_url}/collections/{collection_name}", timeout=10)
    if response.status_code == 200:
        return

    response = requests.put(
        f"{base_url}/collections/{collection_name}",
        json={
            "vectors": {
                "size": VECTOR_SIZE,
                "distance": "Cosine",
            }
        },
        timeout=10,
    )
    if response.status_code not in {200, 201}:
        response.raise_for_status()


def upsert_records_to_qdrant(
    records: Iterable[dict],
    collection_name: str = COLLECTION_NAME,
) -> int:
    import requests

    normalized_records = [normalize_record(record) for record in records]
    if not normalized_records:
        return 0

    ensure_qdrant_collection(collection_name)
    embeddings = embed_texts([record["text"] for record in normalized_records])
    points = []
    for record, embedding in zip(normalized_records, embeddings):
        points.append(
            {
                "id": stable_point_id(record["id"]),
                "vector": embedding,
                "payload": record,
            }
        )

    response = requests.put(
        f"{qdrant_base_url()}/collections/{collection_name}/points",
        params={"wait": "true"},
        json={"points": points},
        timeout=30,
    )
    response.raise_for_status()
    return len(points)
