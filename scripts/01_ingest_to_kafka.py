from pipeline_utils import sample_records, send_records_to_kafka


def ingest_data(records: list[dict]) -> int:
    return send_records_to_kafka(records)


if __name__ == "__main__":
    count = ingest_data(sample_records())
    print(f"Integration 1 OK: {count} records sent to Kafka")
