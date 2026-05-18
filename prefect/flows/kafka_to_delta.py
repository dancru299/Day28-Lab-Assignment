import sys
from pathlib import Path

from prefect import flow, task


LOCAL_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
CONTAINER_SCRIPTS = Path("/opt/scripts")
for scripts_path in (LOCAL_SCRIPTS, CONTAINER_SCRIPTS):
    if scripts_path.exists():
        sys.path.insert(0, str(scripts_path))

from pipeline_utils import (  # noqa: E402
    consume_kafka_records,
    push_records_to_redis,
    save_records_to_delta,
    upsert_records_to_qdrant,
)


@task(retries=2, retry_delay_seconds=5)
def consume_and_process() -> list[dict]:
    records = consume_kafka_records()
    print(f"Consumed {len(records)} records from Kafka")
    return records


@task
def persist_batch(records: list[dict]) -> str | None:
    batch_path = save_records_to_delta(records)
    if not batch_path:
        print("No records to save")
        return None
    print(f"Saved {len(records)} records to Delta Lake: {batch_path}")
    return str(batch_path)


@task(retries=2, retry_delay_seconds=5)
def publish_online_features(records: list[dict]) -> int:
    count = push_records_to_redis(records)
    print(f"Stored {count} features in Redis")
    return count


@task(retries=2, retry_delay_seconds=5)
def publish_vectors(records: list[dict]) -> int:
    count = upsert_records_to_qdrant(records)
    print(f"Stored {count} vectors in Qdrant")
    return count


@flow(name="Kafka to Delta Pipeline")
def kafka_to_delta_flow() -> dict:
    records = consume_and_process()
    batch_path = persist_batch(records)
    feature_count = publish_online_features(records) if records else 0
    vector_count = publish_vectors(records) if records else 0
    return {
        "records": len(records),
        "batch_path": batch_path,
        "features": feature_count,
        "vectors": vector_count,
    }


if __name__ == "__main__":
    kafka_to_delta_flow()
