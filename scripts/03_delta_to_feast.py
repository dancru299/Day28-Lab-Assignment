from pipeline_utils import load_records_from_delta, push_records_to_redis


def load_from_delta_and_push_feast() -> int:
    records = load_records_from_delta()
    if not records:
        print("No data in Delta Lake yet")
        return 0

    count = push_records_to_redis(records)
    print(f"Integration 3+4 OK: {count} features stored in Redis")
    return count


if __name__ == "__main__":
    load_from_delta_and_push_feast()
