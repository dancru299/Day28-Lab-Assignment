from pipeline_utils import load_records_from_delta, sample_records, upsert_records_to_qdrant


def embed_and_store(records: list[dict]) -> int:
    count = upsert_records_to_qdrant(records)
    print(f"Integration 5 OK: {count} vectors stored in Qdrant")
    return count


if __name__ == "__main__":
    records = load_records_from_delta() or sample_records()
    embed_and_store(records)
