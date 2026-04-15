import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Comparison of Common Data Formats for Web Services Payloads

    Choosing the right data format for transmitting data is critical for performance, reliability, and ease of use when integrating services. This guide compares four popular formats: JSON, CSV, MessagePack, and Avro.

    ---

    ## 📐 Format Deep Dive

    ### 🌐 JSON (JavaScript Object Notation)
    JSON is a text-based, language-independent format that uses human-readable key-value pairs. It is the de facto standard for modern APIs.

    **✅ Pros:**
    * **Readability:** Extremely human-readable and intuitive.
    * **Universality:** Supported natively by almost every programming language and web service.
    * **Ease of Use:** Simple structure (object/array) makes parsing straightforward.

    **❌ Cons:**
    * **Verbosity/Overhead:** Since it is text-based, it includes many delimiters, quotation marks, and whitespace, leading to larger payload sizes compared to binary formats.
    * **Strict Typing:** It does not inherently enforce schema (though tools like JSON Schema help).

    **💡 When to use it:**
    When human readability, simplicity, and maximum compatibility across various systems are the top priorities (e.g., public APIs, configuration files).

    ### 📄 CSV (Comma Separated Values)
    CSV is a plain text format that stores tabular data where each row is a data record, and fields are separated by a delimiter (usually a comma).

    **✅ Pros:**
    * **Simplicity:** Extremely simple structure; nearly every spreadsheet program can read/write it.
    * **Compatibility:** Works well for simple, flat, homogeneous datasets (e.g., logs, basic data dumps).
    * **Compact (for simple data):** Can be very compact for uniformly structured data.

    **❌ Cons:**
    * **Data Loss of Structure:** It is inherently difficult to represent complex, nested, or hierarchical data (like JSON objects).
    * **Ambiguity:** Handling non-text data, delimiters within data fields (e.g., commas containing text), and quoting rules can be complex and error-prone.
    * **Lack of Metadata:** No native way to describe the schema, types, or context of the data within the file itself.

    **💡 When to use it:**
    For transferring simple, tabular datasets (e.g., CSV files uploaded to a service for batch processing). Generally discouraged for core, complex API payloads.

    ### 📦 MsgPack (MessagePack)
    MsgPack is a binary serialization format. It aims to be faster and smaller than JSON by using binary representations for data types instead of text.

    **✅ Pros:**
    * **Efficiency:** Significantly more compact and faster to serialize/deserialize than JSON because it eliminates textual overhead.
    * **Speed:** Ideal for high-throughput, internal microservice communications where speed is paramount.
    * **Simplicity (Conceptually):** Maintains the key-value structure of JSON but in binary.

    **❌ Cons:**
    * **Readability:** Completely unreadable to humans without specialized tools.
    * **Adoption:** While gaining traction, it is less universally adopted than JSON, requiring stable libraries on both ends.

    **💡 When to use it:**
    When you need JSON-like structure and high efficiency, but performance and payload size are major concerns (e.g., internal real-time data streams).

    ### 🏛️ Avro (Apache Avro)
    Avro is a data serialization system that typically includes a robust schema definition language (JSON Schema) alongside the data. It is schema-based and highly optimized for handling complex, evolving data schemas.

    **✅ Pros:**
    * **Schema Evolution:** This is its biggest advantage. It handles schema changes (e.g., adding or removing fields) gracefully without breaking older consumers, making it perfect for long-lived APIs.
    * **Efficiency:** Highly efficient, schema-enforced binary serialization, often comparable to Protocol Buffers.
    * **Data Integrity:** The schema ensures that both the sender and receiver agree on how the data is structured, minimizing runtime errors.

    **❌ Cons:**
    * **Complexity:** Requires strict schema definition and implementation, adding initial complexity compared to JSON.
    * **Tooling:** While excellent, setting up the schema registry and serialization framework is more involved than just sending a JSON string.

    **💡 When to use it:**
    When reliability, schema enforcement, and the ability to change data structure over time (schema evolution) without service disruption are critical (e.g., streaming data pipelines, Kafka topics).

    ---

    ## 🚀 Comparison Summary Table

    | Feature | JSON | CSV | MsgPack | Avro |
    | :--- | :---: | :---: | :---: | :---: |
    | **Readability** | Excellent | Excellent | Very Poor | Poor (Needs Schema) |
    | **Payload Size** | Medium (High Overhead) | Low (Flat Data) | Very Low | Very Low |
    | **Structure Limit** | Nested/Complex | Flat Only | Nested/Complex | Nested/Complex |
    | **Schema Enforcement** | Low (Schema required via external tool) | None | Medium | High (Mandatory) |
    | **Schema Evolution** | Poor | None | Medium | Excellent |
    | **Use Case** | Public APIs | Simple Datasets/Logs | High-Speed Internal Transfer | Data Pipelines/Message Brokers |
    """)
    return


@app.cell
def _():
    from pathlib import Path
    import msgpack
    import os
    import json
    import orjson

    # 1. Create a sample dictionary/object
    sample_data = {
        "name": "Tech Company",
        "employees": 1500,
        "departments": ["Engineering", "Marketing", "HR"],
        "active": True,
    }

    # Define paths using pathlib
    msgpack_filename = Path("test_data.msgpack")
    json_filename = Path("test_data.json")

    # 2. Serialize the object to MsgPack format (bytes)
    packed_data = msgpack.packb(sample_data)
    print(f"MsgPack object created. Size: {len(packed_data)} bytes.")

    # 3. Write the bytes object to disk (MsgPack)
    with open(msgpack_filename, "wb") as f:
        f.write(packed_data)
    print(f"Data successfully written to {msgpack_filename}")

    # 3b. Serialize the object to JSON format (bytes) using orjson
    json_bytes = orjson.dumps(sample_data)
    # 3c. Write the bytes object to disk (JSON)
    with open(json_filename, "wb") as f:
        f.write(json_bytes)
    print(f"Data successfully written to {json_filename}")


    # 4. List file statistics
    print("\n--- File Statistics ---")
    try:
        # Get stats for MsgPack file
        file_stats_msgpack = msgpack_filename.stat()
        print(f"MsgPack File Size (Bytes): {file_stats_msgpack.st_size}")
    except FileNotFoundError:
        print(f"MsgPack file ({msgpack_filename}) not found.")

    try:
        # Get stats for JSON file
        file_stats_json = json_filename.stat()
        print(f"JSON File Size (Bytes): {file_stats_json.st_size}")
    except FileNotFoundError:
        print(f"JSON file ({json_filename}) not found.")
    return (Path,)


@app.cell
def _(Path):
    data_dir = Path("./data")
    json_file = data_dir / "housing_test_1.json"
    msgpack_file = data_dir / "housing_test_1.msgpack"
    avro_file = data_dir / "housing_test_1.avro"

    print("--- File Size Check (pathlib st_size) ---")

    # Check and list size for JSON file
    if json_file.exists():
        size_json = json_file.stat().st_size
        print(f"'{json_file}' size: {size_json / 1_000} kb")
    else:
        print(f"'{json_file}' not found.")

    # Check and list size for MsgPack file
    if msgpack_file.exists():
        size_msgpack = msgpack_file.stat().st_size
        print(f"'{msgpack_file}' size: {size_msgpack / 1_000} kb")
    else:
        print(f"'{msgpack_file}' not found.")


    # Check and list size for Avro file
    if avro_file.exists():
        size_msgpack = avro_file.stat().st_size
        print(f"'{avro_file}' size: {size_msgpack / 1_000} kb")
    else:
        print(f"'{avro_file}' not found.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 💡 Why Avro Can Be Smaller Than MsgPack (The Structural Difference)

    The observed size difference between Avro and MsgPack is not necessarily a fixed rule, as it depends heavily on the *data being serialized* and the *specific version* of the format. However, Avro is fundamentally designed for **data stream efficiency, schema evolution, and bulk record processing**, giving it structural advantages that can result in a smaller file size compared to a general-purpose binary serializer like MsgPack.

    Here is a breakdown of the makeup and the source of the size difference:

    ---

    #### 📦 MsgPack: Self-Contained Data Serialization

    **Makeup:** MsgPack is designed to serialize a structured object (like a Python dictionary) into the most compact binary equivalent possible.

    **How it works:**
    1.  It treats the entire dataset as a single, cohesive *object*.
    2.  It replaces text strings, numbers, lists, and maps with their specific binary type identifiers.
    3.  The overhead is minimal (just the type markers and length prefixes).

    **Size implication:** The size is primarily dictated by the data payload itself. It is excellent at minimizing overhead compared to JSON, but it still encodes the structure and keys for *every single object* it serializes.

    ---

    #### 🏛️ Avro: Schema-Enforced, Streaming Serialization

    **Makeup:** Avro is *schema-based*. It separates the schema (the blueprint) from the data (the payloads).

    **How it works:**
    1.  **Schema is Paramount:** Avro requires a defined schema (`.avsc` or defined in code). This schema acts as the master reference for how the data is constructed.
    2.  **Data Encoding:** Avro doesn't just serialize one object; it is optimized for writing potentially millions of records sequentially (streaming).
    3.  **Efficiency Gain (The Key):** Because the schema is defined and known beforehand (and often stored once or implied), **Avro does not need to repeatedly encode the field names or the data types for every single record.**

    **Size implication:**

    *   **Minimal Redundancy:** If you are writing 1,000 records, MsgPack effectively encodes "field\_a: type\_X" 1,000 times (even if implicitly). Avro, because it relies on the external schema, only has to encode the structural information once, allowing the data section to be much more dense and optimized.
    *   **Optimized Data Types:** Avro's encoding can be highly optimized for the underlying type system, sometimes utilizing techniques—like reading structured data in a nearly columnar fashion—that achieve greater compression than MsgPack's general-purpose serialization.

    ---

    ### 📊 Summary of Size Drivers

    | Feature | MsgPack | Avro | Why the difference? |
    | :--- | :--- | :--- | :--- |
    | **Core Design Goal** | Compact object serialization | Data stream reliability & evolution | Different use cases lead to different optimizations. |
    | **Schema Handling** | Implicit (structure is encoded per message) | Explicit (fixed schema referenced) | Avro avoids repeating structural metadata. |
    | **Primary Overhead** | Type markers, array/map overhead | Often minimal, determined by data distribution | Avro's dedicated, stream-focused encoding is superior for bulk data. |
    | **Best for** | Single, complete payload transfer | Continuous, structured data pipelines (Kafka, etc.) | |
    """)
    return


@app.cell
def _():
    # This demonstration simulates Avro's use of a predefined Reader Schema when consuming a stream of records
    # written across different 'batches' (or files) that follow differing field structures.

    # --- 1. Setup and Data Simulation ---

    # Define the overall expected schema (The *Reader Schema* or *Target Schema*).
    # This schema dictates what the consumer expects the data structure to be, regardless of how it was written.
    # Let's define a target that has: 'id', 'name', 'age', 'city'
    TARGET_SCHEMA = {
        "id": "int",  # Always expected
        "name": "string",  # Always expected
        "age": "int",  # Expected, might be missing in older records
        "city": "string",  # Expected, might be missing
    }

    print("--- 📝 Stage 1: Simulation Setup ---")
    print(f"Target Schema (What the system expects): {TARGET_SCHEMA}")

    # --- 2. Batch 1: The Initial Schema (Only 'id' and 'name' exist) ---
    # Schema Used for writing: {id: int, name: string} (Minimal data set)
    batch_1_records = [
        {"id": 101, "name": "Alice"},
        {"id": 102, "name": "Bob"},
    ]
    print("\n[Batch 1 Written] Schema: id, name (Minimal structure)")

    # --- 3. Batch 2: Schema Evolution (Adding 'age') ---
    # Schema Used for writing: {id: int, name: string, age: int}
    # We add the 'age' field to the records.
    batch_2_records = [
        {"id": 201, "name": "Charles", "age": 30},
        {"id": 202, "name": "David", "age": 24},
    ]
    print("[Batch 2 Written] Schema: id, name, age (Field 'age' added)")

    # --- 4. Batch 3: Schema Drifting (Adding 'city', but dropping 'name' field temporarily is handled by default) ---
    # Schema Used for writing: {id: int, age: int, city: string}
    # Note: This batch writes the 'name' field but skips it to demonstrate handling changes.
    batch_3_records = [
        {"id": 301, "name": "Eve", "age": 45, "city": "New York"},
        {"id": 302, "name": "Frank", "age": 32, "city": "Boston"},
        {"id": 303, "name": "Grace", "age": 28, "city": "Austin"},
    ]
    print("[Batch 3 Written] Schema: id, name, age, city (Full structure achieved)")


    # --- 5. The Avro Reading Process (Conceptual Implementation) ---

    print("\n\n--- ⚙️ Stage 2: Structured Reading with Avro (Stream Processing) ---")
    print("--- Avro's Advantage: The Reader Schema handles compatibility. ---")

    all_records = []

    # Simulate reading Batch 1 using the TARGET_SCHEMA (which expects 'age' and 'city')
    print("Reading Batch 1 (Missing 'age' and 'city')...")
    for record in batch_1_records:
        # Avro reader code automatically fills in defaults for missing fields
        # If 'age' was not available in the batch, it defaults it; if 'city' was missing, it defaults it.
        readable_record = {
            "id": record["id"],
            "name": record["name"],
            "age": record.get("age", None),  # Defaulting or handling nulls
            "city": None,  # Missing field defaults to None
        }
        all_records.append(readable_record)
        print(
            f"  -> Read 101: Name={record['name']}, Age={readable_record['age']} (Defaulted), City=None"
        )


    # Simulate reading Batch 2 using the TARGET_SCHEMA
    print("\nReading Batch 2 (Contains 'age', Missing 'city')...")
    for record in batch_2_records:
        readable_record = {
            "id": record["id"],
            "name": record["name"],
            "age": record["age"],
            "city": None,  # Still missing, defaults to None
        }
        all_records.append(readable_record)
        print(
            f"  -> Read 201: Name={record['name']}, Age={readable_record['age']}, City=None"
        )


    # Simulate reading Batch 3 using the TARGET_SCHEMA
    print("\nReading Batch 3 (Complete structure)...\n")
    for record in batch_3_records:
        readable_record = {
            "id": record["id"],
            "name": record["name"],
            "age": record["age"],
            "city": record["city"],
        }
        all_records.append(readable_record)
        print(
            f"  -> Read 301: Name={record['name']}, Age={readable_record['age']}, City={readable_record['city']}"
        )


    # --- 6. Final Result ---
    print("\n==================================================================")
    print(
        "✅ SUCCESS: All records, despite having differing structures, were successfully read!"
    )
    print(f"Total records processed: {len(all_records)}")
    print("Example final read format (homogeneous):")
    print(all_records)
    print("==================================================================")


    # --- 🚀 Contrast with JSON ---

    print("\n\n--- 💔 Contrast: JSON Approach ---")
    print("When using JSON, you must load and process the file boundaries manually.")
    print(
        "If the consumer logic assumes a structure (e.g., looking for 'age' in a record), and a file (like Batch 1) was generated without that field, the entire consuming application must handle the variation."
    )

    print(
        "\nScenario: You write all three batches to separate JSON files (file1.json, file2.json, file3.json)."
    )
    print("A consumer reading these files must implement complex logic like:")
    print(
        "1. Check if key 'age' exists in the current object. If not, assume a default (e.g., None)."
    )
    print("2. Handle potential type mismatches if a batch changes its output format.")
    print(
        "3. The coupling is tighter: the reader must know, or guess, the expected structure changes."
    )

    print("\n🔑 Key Takeaway:")
    print(
        "Avro's schema-based approach (Reader Schema) guarantees that the consumer always receives data conforming to the *target structure*, because the format handles the mapping (and defaulting) internally, invisible to the application code."
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Compression

    1. Snappy: The Speed Demon

    Developed by Google, Snappy doesn't aim for maximum compression. Instead, it
    aims for speeds so high that you barely notice the CPU overhead.

        The Vibe: "I don't care if the file is still a bit big, just get it done
        now."

        Pros: Extremely fast compression and decompression; very stable; low CPU
        footprint.

        Cons: Poor compression ratios compared to the others.

        Ideal For: Internal RPCs, MapReduce jobs, and scenarios where disk I/O is
        faster than the time it takes to compress heavily.

    2. Zstd: The Modern Gold Standard

    Created by Facebook (Meta), Zstd is the "Swiss Army Knife." It is designed to
    scale from Snappy-like speeds to LZMA-level (7-Zip) compression ratios.

        The Vibe: "Why choose? I can be fast and small."

        Pros: Incredible flexibility (levels 1–22); provides a "Long Distance" mode
        for massive files; consistently outperforms Gzip in almost every metric.

        Cons: Slightly higher memory usage at very high compression levels.

        Ideal For: Almost everything. It’s replacing Gzip as the default in many
        Linux distributions and databases (like RocksDB or ClickHouse).

    3. Gzip: The Reliable Veteran

    Gzip (based on the DEFLATE algorithm) has been the industry standard for
    decades. While it’s being eclipsed by Zstd, it remains the most compatible.

        The Vibe: "I'm everywhere. Everyone knows how to talk to me."

        Pros: Built into every web browser, server, and OS by default.

        Cons: Slower than Zstd for the same compression ratio; showing its age on
        modern multi-core CPUs.

        Ideal For: Serving web content (HTML/JS/CSS) and situations where you aren't
        sure what software the recipient is using.

    Which one should you choose?

        Use Snappy if: You are managing a massive data pipeline (like Kafka or
        Spark) and the bottleneck is CPU time, not storage space.

        Use Zstd if: You want the best overall performance. It is the smartest
        choice for 90% of modern applications, especially if you can control both
        the compressor and the decompressor.

        Use Gzip if: You are sending data over the public internet to a browser, or
        working with legacy systems that don't support modern libraries.

    Pro Tip: If you're using Zstd, try the "dictionary compression" feature. It’s a
    game-changer for compressing very small, repetitive chunks of data (like JSON
    logs) by pre-training the algorithm on what your data looks like.

    When working with **PySpark**, the conversation changes from just "file size" to
    **"distributed efficiency."** In a big data environment, the most important
    factor is often whether a file is **splittable**.

    If a file is not splittable (like a standard Gzip-compressed CSV), Spark can
    only use **one CPU core** to read that entire file, even if you have a cluster
    of 100 machines.

    ---

    ## The "Big Data" Options You Missed

    Beyond the "Big Three," there are two other contenders often used in data
    engineering:

    ### 1. LZ4 (The Speed King)

    If you think Snappy is fast, LZ4 is the athlete that makes it look slow.

    * **The Vibe:** "I want to decompress data at the speed of RAM."
    * **In PySpark:** It is often used for **Shuffle** operations (the internal moving of data between nodes) because the decompression speed is nearly instant.
    * **Trade-off:** Very low compression ratio. It's best for transient data that needs to be moved across the network quickly.

    ### 2. Bzip2 (The Space Saver)

    Bzip2 is the "old school" high-compression choice.

    * **The Vibe:** "I will squeeze this file until it's tiny, even if it takes all day."
    * **In PySpark:** Bzip2's killer feature is that it is **natively splittable**. Unlike Gzip, Spark can take one giant `.bz2` file and split it across multiple executors.
    * **Trade-off:** It is incredibly CPU-intensive. Your Spark job might get bottlenecked by the CPUs trying to unpack the data rather than the actual analysis.

    ---

    ## Comparison for PySpark (The "Splittability" Factor)

    | Format | Splittable? | Best Used With | Recommendation |
    | :--- | :--- | :--- | :--- |
    | **Snappy** | **No*** | Parquet / Avro | Use inside Parquet for the best balance. |
    | **Zstd** | **No*** | Parquet / ORC | Use inside Parquet for high-density storage. |
    | **Gzip** | **No** | Small CSV/JSON | Avoid for large datasets; creates "hot partitions." |
    | **Bzip2** | **Yes** | Large CSV/JSON | Use when you MUST store text files and save space. |
    | **LZ4** | **No** | Spark Shuffles | Great for high-speed temporary data. |

    > **\*Important Distinction:** While Snappy and Zstd are not "splittable" as raw
    files (e.g., `data.csv.snappy`), they **are** splittable when used as the
    compression codec inside a **Parquet** or **Avro** file. This is because those
    file formats handle the splitting at the block level.

    ---

    ## How to use them in PySpark

    To read or write these in PySpark, you generally set the `compression` option in your write command.

    ### Writing with Zstd (Recommended for Storage)
    ```python
    df.write.parquet("path/to/data", compression="zstd")
    ```

    ### Writing with Snappy (Spark Default)
    ```python
    df.write.parquet("path/to/data", compression="snappy")
    ```

    ### Dealing with "The Gzip Trap"
    If you receive a massive `10GB.csv.gz` file, PySpark will process it using only **one task**. To fix this, you should read it once, and immediately write it back out as **Snappy-compressed Parquet**. This "re-hydrates" the data so future jobs can process it in parallel.

    ```python
    # Step 1: Read the slow, non-splittable Gzip file
    df = spark.read.csv("massive_file.csv.gz")

    # Step 2: Write it as Parquet (now it's splittable and fast!)
    df.write.parquet("optimized_data", compression="snappy")
    ```
    """)
    return


@app.cell
def _(Path):
    import snappy

    for ext in ["avro", "msgpack", "json"]:
        with Path(f"./data/housing_test_1.{ext}").open('rb') as f_in:
            data = snappy.compress(data=f_in.read())
            with Path(f"./data/housing_test_1.{ext}.snappy").open('wb') as f_out:
                f_out.write(data)

    return


@app.cell
def _(Path):
    for e in ["avro", "msgpack", "json"]:
        p = Path(f"./data/housing_test_1.{e}.snappy")
        if p.exists():
            p_size = p.stat().st_size
            print(f"'{p}' size: {p_size / 1_000} kb")
        else:
            print(f"'{p}' not found.")

    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
