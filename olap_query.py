import time
import duckdb

print("Connecting to DuckDB...")
con = duckdb.connect(':memory:')

PARQUET_SOURCE = "'obt_parquet/**/*.parquet'"

queries = {
    "0. The Total Count (Full Table Scan)": f"""
        SELECT 
            COUNT(*) as total_sensor_readings,
            COUNT(DISTINCT batch_id) as total_batches,
            COUNT(DISTINCT equipment_id) as total_machines
        FROM read_parquet({PARQUET_SOURCE});
    """,

    "1. The Big Scan (I/O & Memory Stress)": f"""
        SELECT 
            equipment_id, 
            sensor_name,
            COUNT(sensor_value) as total_readings,
            AVG(sensor_value) as avg_sensor_value,
            MAX(sensor_value) as max_sensor_value
        FROM read_parquet({PARQUET_SOURCE})
        WHERE quality_test = 'Final_Yield_Voltage' 
          AND quality_value < 3.65
        GROUP BY equipment_id, sensor_name
        ORDER BY avg_sensor_value DESC;
    """,

    "2. The Bigger Run Chart (Time-Series Plotting Data)": f"""
        SELECT 
            time_bucket(INTERVAL '5 minutes', sensor_time) AS timestamp_bucket,
            equipment_id,
            AVG(sensor_value) AS avg_value,
            MIN(sensor_value) AS min_value,
            MAX(sensor_value) AS max_value,
            STDDEV_POP(sensor_value) AS standard_deviation
        FROM read_parquet({PARQUET_SOURCE})
        WHERE sensor_name = 'Sensor_0'
        GROUP BY timestamp_bucket, equipment_id
        ORDER BY timestamp_bucket, equipment_id;
    """,

    "3. The Heat Map (2D Matrix Pivot)": f"""
        PIVOT (
            SELECT 
                equipment_id,
                CAST(EXTRACT('hour' FROM sensor_time) AS INT) AS hour_of_day,
                sensor_value
            FROM read_parquet({PARQUET_SOURCE})
            WHERE sensor_name = 'Sensor_1'
        )
        ON hour_of_day 
        USING AVG(sensor_value) 
        GROUP BY equipment_id
        ORDER BY equipment_id;
    """,

    "4. The Percentile Crunch (Heavy Math Stress)": f"""
        SELECT 
            equipment_id,
            sensor_name,
            quantile_cont(sensor_value, 0.50) as median_value,
            quantile_cont(sensor_value, 0.95) as p95_value,
            quantile_cont(sensor_value, 0.99) as p99_value
        FROM read_parquet({PARQUET_SOURCE})
        GROUP BY equipment_id, sensor_name;
    """,

    "5. The Data Detective (CTE & Multi-Pass Stress)": f"""
        WITH equip_stats AS (
            SELECT equipment_id, sensor_name, 
                   AVG(sensor_value) as mean_val, 
                   STDDEV_SAMP(sensor_value) as std_val
            FROM read_parquet({PARQUET_SOURCE})
            GROUP BY equipment_id, sensor_name
        ),
        spikes AS (
            SELECT p.batch_id, COUNT(*) as spike_count
            FROM read_parquet({PARQUET_SOURCE}) p
            JOIN equip_stats s ON p.equipment_id = s.equipment_id AND p.sensor_name = s.sensor_name
            WHERE p.sensor_value > (s.mean_val + (2 * s.std_val))
            GROUP BY p.batch_id
        )
        SELECT 
            r.equipment_id,
            COUNT(DISTINCT r.batch_id) as batches_processed,
            SUM(s.spike_count) as total_sensor_spikes,
            AVG(date_diff('second', r.batch_start, r.batch_end)) as avg_process_time_seconds
        FROM read_parquet({PARQUET_SOURCE}) r
        JOIN spikes s ON r.batch_id = s.batch_id
        GROUP BY r.equipment_id
        ORDER BY total_sensor_spikes DESC;
    """,

    "6. The Needle (Point Lookup)": f"""
        SELECT 
            batch_id,
            equipment_id,
            batch_start,
            batch_end,
            quality_test,
            quality_value
        FROM read_parquet({PARQUET_SOURCE})
        WHERE batch_id = 'BATCH_000402'
        LIMIT 1;
    """
}

# Run the race
print("\nStarting DuckDB / Parquet Stress Test...\n")
for name, sql in queries.items():
    print(f"{'-' * 60}")
    print(f"Running: {name}")
    start_time = time.time()

    result_df = con.execute(sql).df()

    duration = time.time() - start_time
    print(f" -> Completed in {duration:.4f} seconds.")

    if "Heat Map" in name:
        print(result_df.head(5).to_string())
    else:
        print(result_df.head(3))