import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    import pandas as pd
    import polars as pl
    import duckdb
    import time
    import os
    from pathlib import Path
    return Path, duckdb, mo, os, pd, pl, time


@app.cell
def __(Path):
    # Load the data path
    data_path = Path("data/housing_test_1.parquet")
    return data_path,


@app.cell
def __(data_path, pd):
    # Load data for pandas
    df_pd = pd.read_parquet(data_path, columns=["sq_feet", "aesthetic", "price"])
    return df_pd,


@app.cell
def __():
    # Shared aesthetic mapping
    aesthetic_map = {
        "ability": "main character energy",
        "fine": "no cap",
        "ok": "mid",
        "picture": "aesthetic AF",
        "drug": "dope",
        "traditional": "old money",
        "memory": "core memory",
        "agreement": "bet",
        "skin": "glow up",
        "word": "periodt"
    }
    return aesthetic_map,


@app.cell
def __(Path, aesthetic_map, df_pd, pd, time):
    # Performance benchmarking for pandas iterrows

    start_time_iterrows = time.perf_counter()

    # 1. Calculate quartiles
    sq_feet_quartiles = pd.qcut(df_pd['sq_feet'], 4, labels=["Charming Nook", "Palatial Suite", "Majestic Manor", "Galactic Estate"])
    price_quartiles = pd.qcut(df_pd['price'], 4, labels=["Affordable Gem", "Exclusive Portfolio", "Sovereign Luxury", "Transcendent Wealth"])

    # Add them to a temporary dataframe
    temp_df = df_pd.copy()
    temp_df['sq_feet_desc'] = sq_feet_quartiles
    temp_df['price_desc'] = price_quartiles

    # 2. Iterrows transformation
    rizzlevels = []
    for index, row in temp_df.iterrows():
        aest = row['aesthetic']
        slang = aesthetic_map.get(aest, "standard vibe")
        rizz = f"{row['sq_feet_desc']} with {slang} at a {row['price_desc']} price"
        rizzlevels.append(rizz)

    temp_df['rizzlevel'] = rizzlevels
    final_df_iterrows = temp_df[['sq_feet', 'aesthetic', 'price', 'rizzlevel']]

    # 3. Write to disk
    output_dir = Path("data/benchmarking/out_pandas_iterrows.parquet")
    output_dir.mkdir(parents=True, exist_ok=True)
    final_df_iterrows.to_parquet(output_dir / "data.parquet")

    duration_iterrows = time.perf_counter() - start_time_iterrows
    return duration_iterrows, final_df_iterrows


@app.cell
def __(Path, aesthetic_map, df_pd, pd, time):
    # Performance benchmarking for optimized pandas (vectorized)

    start_time_opt = time.perf_counter()

    # 1. Calculate quartiles
    sq_feet_q = pd.qcut(df_pd['sq_feet'], 4, labels=["Charming Nook", "Palatial Suite", "Majestic Manor", "Galactic Estate"])
    price_q = pd.qcut(df_pd['price'], 4, labels=["Affordable Gem", "Exclusive Portfolio", "Sovereign Luxury", "Transcendent Wealth"])

    # 2. Vectorized transformation
    slang_series = df_pd['aesthetic'].map(aesthetic_map).fillna("standard vibe")

    rizzlevel_opt = (
        sq_feet_q.astype(str) +
        " with " +
        slang_series +
        " at a " +
        price_q.astype(str) +
        " price"
    )

    final_df_opt = df_pd.copy()
    final_df_opt['rizzlevel'] = rizzlevel_opt
    final_df_opt = final_df_opt[['sq_feet', 'aesthetic', 'price', 'rizzlevel']]

    # 3. Write to disk
    output_dir_opt = Path("data/benchmarking/pandas_optimised.parquet")
    output_dir_opt.mkdir(parents=True, exist_ok=True)
    final_df_opt.to_parquet(output_dir_opt / "data.parquet")

    duration_opt = time.perf_counter() - start_time_opt
    return duration_opt, final_df_opt


@app.cell
def __(Path, aesthetic_map, data_path, pl, time):
    # Performance benchmarking for Polars (Eager)

    start_time_pl = time.perf_counter()

    # 1. Load data
    df_pl = pl.read_parquet(data_path, columns=["sq_feet", "aesthetic", "price"])

    # 2. Transformations
    final_df_pl = df_pl.with_columns([
        pl.col("sq_feet").qcut(
            4,
            labels=["Charming Nook", "Palatial Suite", "Majestic Manor", "Galactic Estate"]
        ).cast(pl.String).alias("sq_feet_desc"),
        pl.col("price").qcut(
            4,
            labels=["Affordable Gem", "Exclusive Portfolio", "Sovereign Luxury", "Transcendent Wealth"]
        ).cast(pl.String).alias("price_desc"),
        pl.col("aesthetic").replace(aesthetic_map, default="standard vibe").alias("slang")
    ]).with_columns(
        rizzlevel = pl.format(
            "{} with {} at a {} price",
            pl.col("sq_feet_desc"),
            pl.col("slang"),
            pl.col("price_desc")
        )
    ).select(["sq_feet", "aesthetic", "price", "rizzlevel"])

    # 3. Write to disk
    output_dir_pl = Path("data/benchmarking/polars_hard_worker.parquet")
    output_dir_pl.mkdir(parents=True, exist_ok=True)
    final_df_pl.write_parquet(output_dir_pl / "data.parquet")

    duration_pl = time.perf_counter() - start_time_pl
    return duration_pl, final_df_pl


@app.cell
def __(Path, aesthetic_map, data_path, pl, time):
    # Performance benchmarking for Polars (Lazy)

    start_time_pl_lazy = time.perf_counter()

    # 1. Define Lazy Computation
    q = (
        pl.scan_parquet(data_path)
        .select(["sq_feet", "aesthetic", "price"])
        .with_columns([
            pl.col("sq_feet").qcut(
                4,
                labels=["Charming Nook", "Palatial Suite", "Majestic Manor", "Galactic Estate"]
            ).cast(pl.String).alias("sq_feet_desc"),
            pl.col("price").qcut(
                4,
                labels=["Affordable Gem", "Exclusive Portfolio", "Sovereign Luxury", "Transcendent Wealth"]
            ).cast(pl.String).alias("price_desc"),
            pl.col("aesthetic").replace(aesthetic_map, default="standard vibe").alias("slang")
        ])
        .with_columns(
            rizzlevel = pl.format(
                "{} with {} at a {} price",
                pl.col("sq_feet_desc"),
                pl.col("slang"),
                pl.col("price_desc")
            )
        )
        .select(["sq_feet", "aesthetic", "price", "rizzlevel"])
    )

    # 2. Write to disk
    output_dir_lazy = Path("data/benchmarking/polars_lazy.parquet")
    output_dir_lazy.mkdir(parents=True, exist_ok=True)
    q.collect().write_parquet(output_dir_lazy / "data.parquet")

    duration_pl_lazy = time.perf_counter() - start_time_pl_lazy
    return duration_pl_lazy,


@app.cell
def __(Path, aesthetic_map, data_path, duckdb, time):
    # Performance benchmarking for DuckDB

    start_time_duck = time.perf_counter()

    output_dir_duck = Path("data/benchmarking/duckdb_performance.parquet")
    output_dir_duck.mkdir(parents=True, exist_ok=True)
    output_file_duck = output_dir_duck / "data.parquet"

    # Construct the CASE statement for aesthetic mapping
    case_parts = [f"WHEN '{k}' THEN '{v}'" for k, v in aesthetic_map.items()]
    slang_case = f"CASE aesthetic {' '.join(case_parts)} ELSE 'standard vibe' END"

    # DuckDB SQL for the transformation
    query = f"""
    COPY (
        SELECT
            sq_feet,
            aesthetic,
            price,
            (sq_feet_desc || ' with ' || slang || ' at a ' || price_desc || ' price') as rizzlevel
        FROM (
            SELECT
                sq_feet,
                aesthetic,
                price,
                CASE NTILE(4) OVER (ORDER BY sq_feet)
                    WHEN 1 THEN 'Charming Nook'
                    WHEN 2 THEN 'Palatial Suite'
                    WHEN 3 THEN 'Majestic Manor'
                    WHEN 4 THEN 'Galactic Estate'
                END as sq_feet_desc,
                CASE NTILE(4) OVER (ORDER BY price)
                    WHEN 1 THEN 'Affordable Gem'
                    WHEN 2 THEN 'Exclusive Portfolio'
                    WHEN 3 THEN 'Sovereign Luxury'
                    WHEN 4 THEN 'Transcendent Wealth'
                END as price_desc,
                {slang_case} as slang
            FROM read_parquet('{data_path}')
        )
    ) TO '{output_file_duck}' (FORMAT PARQUET);
    """

    duckdb.query(query)

    duration_duck = time.perf_counter() - start_time_duck
    return duration_duck, output_file_duck


@app.cell
def __(
    duration_duck,
    duration_iterrows,
    duration_opt,
    duration_pl,
    duration_pl_lazy,
    mo,
):
    mo.md(
        f"""
        # Performance Comparison

        | Method | Duration (seconds) | Speedup (vs iterrows) |
        | :--- | :--- | :--- |
        | Pandas `iterrows` | {duration_iterrows:.4f}s | 1x |
        | Pandas Vectorized | {duration_opt:.4f}s | {duration_iterrows / duration_opt:.2f}x |
        | Polars (Eager) | {duration_pl:.4f}s | {duration_iterrows / duration_pl:.2f}x |
        | Polars (Lazy) | {duration_pl_lazy:.4f}s | {duration_iterrows / duration_pl_lazy:.2f}x |
        | DuckDB | {duration_duck:.4f}s | {duration_iterrows / duration_duck:.2f}x |
        """
    )
    return


if __name__ == "__main__":
    app.run()
