import psycopg2
import time
import pandas as pd
import warnings

warnings.filterwarnings('ignore', category=UserWarning)

# --- Database Credentials ---
DB_HOST = "localhost"
DB_NAME = "db"
DB_USER = "admin"
DB_PASS = "admin"
DB_PORT = "5432"

wide_queries = {
    "0. The Total Count (Full Table Scan)": """
        SELECT 
            COUNT(*) as total_sensor_readings,
            COUNT(DISTINCT "BATCH") as total_batches,
            COUNT(DISTINCT "EQUIPMENT") as total_machines
        FROM wide_parameters;
    """,

    "1. The Big Scan (I/O & Memory Stress)": """
        SELECT 
            "EQUIPMENT", 
            "OBSERVATION" as sensor_name,
            COUNT("VALUE") as total_readings,
            AVG("VALUE") as avg_sensor_value,
            MAX("VALUE") as max_sensor_value
        FROM wide_parameters
        WHERE "QUALITY_TEST" = 'Final_Yield_Voltage' 
          AND "QUALITY_VALUE" < 3.65
        GROUP BY "EQUIPMENT", "OBSERVATION"
        ORDER BY avg_sensor_value DESC;
    """,

    "2. The Bigger Run Chart (Time-Series Plotting Data)": """
        SELECT 
            time_bucket('5 minutes', "TIMESTAMP") AS timestamp_bucket,
            "EQUIPMENT",
            AVG("VALUE") AS avg_value,
            MIN("VALUE") AS min_value,
            MAX("VALUE") AS max_value,
            STDDEV_POP("VALUE") AS standard_deviation
        FROM wide_parameters
        WHERE "OBSERVATION" = 'Sensor_0'
        GROUP BY timestamp_bucket, "EQUIPMENT"
        ORDER BY timestamp_bucket, "EQUIPMENT";
    """,

    "3. The Heat Map (Manual Pivot via Filter)": """
        SELECT 
            "EQUIPMENT",
            AVG("VALUE") FILTER (WHERE EXTRACT('hour' FROM "TIMESTAMP") = 0) AS hour_00,
            AVG("VALUE") FILTER (WHERE EXTRACT('hour' FROM "TIMESTAMP") = 1) AS hour_01,
            AVG("VALUE") FILTER (WHERE EXTRACT('hour' FROM "TIMESTAMP") = 2) AS hour_02,
            AVG("VALUE") FILTER (WHERE EXTRACT('hour' FROM "TIMESTAMP") = 3) AS hour_03,
            AVG("VALUE") FILTER (WHERE EXTRACT('hour' FROM "TIMESTAMP") = 4) AS hour_04,
            AVG("VALUE") FILTER (WHERE EXTRACT('hour' FROM "TIMESTAMP") = 23) AS hour_23
        FROM wide_parameters
        WHERE "OBSERVATION" = 'Sensor_1'
        GROUP BY "EQUIPMENT"
        ORDER BY "EQUIPMENT";
    """,

    "4. The Percentile Crunch (Heavy Math Stress)": """
        SELECT 
            "EQUIPMENT",
            "OBSERVATION" as sensor_name,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY "VALUE") as median_value,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY "VALUE") as p95_value,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY "VALUE") as p99_value
        FROM wide_parameters
        GROUP BY "EQUIPMENT", "OBSERVATION";
    """,

    "5. The Data Detective (CTE & Multi-Pass Stress)": """
        WITH equip_stats AS MATERIALIZED (
            SELECT "EQUIPMENT", "OBSERVATION", 
                   AVG("VALUE") as mean_val, 
                   STDDEV_SAMP("VALUE") as std_val
            FROM wide_parameters
            GROUP BY "EQUIPMENT", "OBSERVATION"
        ),
        spikes AS MATERIALIZED (
            SELECT p."BATCH", COUNT(*) as spike_count
            FROM wide_parameters p
            JOIN equip_stats s ON p."EQUIPMENT" = s."EQUIPMENT" AND p."OBSERVATION" = s."OBSERVATION"
            WHERE p."VALUE" > (s.mean_val + (2 * s.std_val))
            GROUP BY p."BATCH"
        )
        SELECT 
            w."EQUIPMENT",
            COUNT(DISTINCT w."BATCH") as batches_processed,
            SUM(s.spike_count) as total_sensor_spikes,
            AVG(EXTRACT(EPOCH FROM (w."BATCH_END" - w."BATCH_START"))) as avg_process_time_seconds
        FROM wide_parameters w
        JOIN spikes s ON w."BATCH" = s."BATCH"
        GROUP BY w."EQUIPMENT"
        ORDER BY total_sensor_spikes DESC;
    """,

    "6. The Needle (Point Lookup)": """
        SELECT 
            "BATCH",
            "EQUIPMENT",
            "BATCH_START",
            "BATCH_END",
            "QUALITY_TEST",
            "QUALITY_VALUE"
        FROM wide_parameters
        WHERE "BATCH" = 'BATCH_000402'
        LIMIT 1;
    """
}


def main():
    print("Connecting to TimescaleDB to run Wide Table Benchmarks...\n")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        cursor = conn.cursor()

        for name, sql in wide_queries.items():
            print(f"{'-' * 60}")
            print(f"Running: {name}")
            start_query = time.time()

            cursor.execute(sql)
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]
            result_df = pd.DataFrame(rows, columns=col_names)

            duration = time.time() - start_query
            print(f" -> Completed in {duration:.4f} seconds.")

            if "Heat Map" in name:
                print(result_df.head(5).to_string())
            else:
                print(result_df.head(3))

    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()


if __name__ == "__main__":
    main()