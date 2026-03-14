import duckdb
import time
import os

print("Starting OLAP Data Prep (Building the 'One Big Table')...")
start_time = time.time()

# Ensure the output directory exists
os.makedirs("obt_parquet", exist_ok=True)

# We use DuckDB in-memory to do the heavy lifting
con = duckdb.connect(':memory:')

build_obt_query = """
COPY (
    SELECT
        p.OBSERVATION AS sensor_name,
        p.TIMESTAMP AS sensor_time,
        p.VALUE AS sensor_value,
        p.BATCH AS batch_id,
        r.TIME_START AS batch_start,
        r.TIME_END AS batch_end,
        q.OBSERVATION AS quality_test,
        q.VALUE AS quality_value,
        p.EQUIPMENT AS equipment_id  -- Partition column must be last
    FROM read_csv_auto('synthetic_data/parameter_data.csv') p
    LEFT JOIN read_csv_auto('synthetic_data/routing_data.csv') r ON p.BATCH = r.BATCH
    LEFT JOIN read_csv_auto('synthetic_data/quality_data.csv') q ON p.BATCH = q.BATCH
) TO 'obt_parquet' (FORMAT PARQUET, PARTITION_BY (equipment_id), OVERWRITE_OR_IGNORE 1);
"""

con.execute(build_obt_query)
duration = time.time() - start_time

print(f"Success! Denormalized and chunked Parquet files created in {duration:.2f} seconds.")
print("Go check the 'obt_parquet' folder. Notice how small the files are!")