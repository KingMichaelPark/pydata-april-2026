import argparse
import csv
import json
import time
from pathlib import Path

import orjson
import ujson
import msgpack
import fastavro
from faker import Faker

# Initialize Faker
fake = Faker()

# Schema mapping from your SQLAlchemy model
HOUSING_AVRO_SCHEMA = {
    "type": "record",
    "name": "HousingData",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "num_rooms", "type": "int"},
        {"name": "num_bathrooms", "type": "float"},
        {"name": "sq_feet", "type": "int"},
        {"name": "aesthetic", "type": "string"},
        {"name": "price", "type": "int"},
        {"name": "address", "type": "string"},
        {"name": "city", "type": "string"},
        {"name": "year_built", "type": ["null", "int"], "default": None},
        {"name": "is_available", "type": "boolean", "default": True},
        {"name": "has_garage", "type": "boolean", "default": False},
    ],
}


def generate_records(count: int = 1000):
    records = []
    for i in range(1, count + 1):
        records.append(
            {
                "id": i,
                "num_rooms": fake.random_int(min=1, max=10),
                "num_bathrooms": float(fake.random_int(min=1, max=5)),
                "sq_feet": fake.random_int(min=500, max=5000),
                "aesthetic": fake.word(),
                "price": fake.random_int(min=100000, max=2000000),
                "address": fake.address().replace("\n", ", "),
                "city": fake.city(),
                "year_built": fake.year() if fake.boolean() else None,
                "is_available": fake.boolean(),
                "has_garage": fake.boolean(),
            }
        )
    return records


def save_and_time(fmt: str, data: list, output_dir: Path, index: int = 1) -> float:
    file_path = output_dir / f"housing_test_{index}.{fmt}"

    start_time = time.perf_counter()

    if fmt == "json":
        with open(file_path, "w") as f:
            json.dump(data, f)

    elif fmt == "orjson":
        with open(file_path, "wb") as f:
            f.write(orjson.dumps(data))

    elif fmt == "ujson":
        with open(file_path, "w") as f:
            ujson.dump(data, f)

    elif fmt == "msgpack":
        with open(file_path, "wb") as f:
            f.write(msgpack.packb(data))

    elif fmt == "csv":
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

    elif fmt == "avro":
        with open(file_path, "wb") as f:
            fastavro.writer(f, HOUSING_AVRO_SCHEMA, data)

    end_time = time.perf_counter()
    return end_time - start_time


def main():
    supported = ["json", "orjson", "ujson", "msgpack", "csv", "avro"]

    parser = argparse.ArgumentParser(
        description="Generate and time mock housing data generation."
    )
    parser.add_argument("--all", action="store_true", help="Generate all formats")
    parser.add_argument(
        "--num-files", type=int, default=1, help="Number of files to generate per format"
    )
    parser.add_argument(
        "formats", nargs="*", help=f"Individual formats: {', '.join(supported)}"
    )

    args = parser.parse_args()
    selected_formats = supported if args.all else args.formats
    valid_formats = [f for f in selected_formats if f in supported]

    if not valid_formats:
        print(f"No valid formats selected. Use --all or one of: {supported}")
        return

    output_path = Path("data")
    output_path.mkdir(parents=True, exist_ok=True)

    results = {fmt: [] for fmt in valid_formats}

    for i in range(1, args.num_files + 1):
        print(f"Iteration {i}/{args.num_files}: Generating 1000 records...")
        records = generate_records(1000)

        for fmt in valid_formats:
            duration = save_and_time(fmt, records, output_path, index=i)
            results[fmt].append(duration)
            print(f"Finished {fmt}...")

    print("\n" + "=" * 45)
    print(f"{'Format':<10} | {'Avg Time (s)':<15} | {'Total Time (s)':<15}")
    print("-" * 45)
    for fmt, durations in results.items():
        avg_time = sum(durations) / len(durations)
        total_time = sum(durations)
        print(f"{fmt:<10} | {avg_time:<15.6f} | {total_time:<15.6f}")
    print("=" * 45)


if __name__ == "__main__":
    main()

