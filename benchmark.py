import psycopg2
import duckdb
import time
import pandas as pd
import warnings
from collections import defaultdict

warnings.filterwarnings('ignore', category=UserWarning)

# =============================================================================
# --- 1. BENCHMARK CONFIGURATION ---
# =============================================================================
NUM_SAMPLES = 10  # <-- Change this to run queries more or fewer times

DB_HOST = "localhost"
DB_NAME = "db"
DB_USER = "admin"
DB_PASS = "admin"
DB_PORT = "5432"

PARQUET_SOURCE = "'obt_parquet/**/*.parquet'"

# =============================================================================
# --- 2. QUERY DICTIONARIES ---
# =============================================================================

# -- A. Normalized (OLTP) Queries --
oltp_queries = {
    "0. The Total Count": """SELECT COUNT(*) as total_sensor_readings, COUNT(DISTINCT "BATCH") as total_batches, COUNT(DISTINCT "EQUIPMENT") as total_machines FROM parameters;""",
    "1. The Big Scan": """SELECT p."EQUIPMENT", p."OBSERVATION" as sensor_name, COUNT(p."VALUE") as total_readings, AVG(p."VALUE") as avg_sensor_value, MAX(p."VALUE") as max_sensor_value FROM parameters p JOIN quality q ON p."BATCH" = q."BATCH" WHERE q."OBSERVATION" = 'Final_Yield_Voltage' AND q."VALUE" < 3.65 GROUP BY p."EQUIPMENT", p."OBSERVATION" ORDER BY avg_sensor_value DESC;""",
    "2. The Bigger Run Chart": """SELECT time_bucket('5 minutes', "TIMESTAMP") AS timestamp_bucket, "EQUIPMENT", AVG("VALUE") AS avg_value, MIN("VALUE") AS min_value, MAX("VALUE") AS max_value, STDDEV_POP("VALUE") AS standard_deviation FROM parameters WHERE "OBSERVATION" = 'Sensor_0' GROUP BY timestamp_bucket, "EQUIPMENT" ORDER BY timestamp_bucket, "EQUIPMENT";""",
    "3. The Heat Map": """SELECT "EQUIPMENT", AVG("VALUE") FILTER (WHERE EXTRACT('hour' FROM "TIMESTAMP") = 0) AS hour_00, AVG("VALUE") FILTER (WHERE EXTRACT('hour' FROM "TIMESTAMP") = 1) AS hour_01, AVG("VALUE") FILTER (WHERE EXTRACT('hour' FROM "TIMESTAMP") = 23) AS hour_23 FROM parameters WHERE "OBSERVATION" = 'Sensor_1' GROUP BY "EQUIPMENT" ORDER BY "EQUIPMENT";""",
    "4. The Percentile Crunch": """SELECT "EQUIPMENT", "OBSERVATION" as sensor_name, percentile_cont(0.50) WITHIN GROUP (ORDER BY "VALUE") as median_value, percentile_cont(0.95) WITHIN GROUP (ORDER BY "VALUE") as p95_value, percentile_cont(0.99) WITHIN GROUP (ORDER BY "VALUE") as p99_value FROM parameters GROUP BY "EQUIPMENT", "OBSERVATION";""",
    "5. The Data Detective": """WITH equip_stats AS (SELECT "EQUIPMENT", "OBSERVATION", AVG("VALUE") as mean_val, STDDEV_SAMP("VALUE") as std_val FROM parameters GROUP BY "EQUIPMENT", "OBSERVATION"), spikes AS (SELECT p."BATCH", COUNT(*) as spike_count FROM parameters p JOIN equip_stats s ON p."EQUIPMENT" = s."EQUIPMENT" AND p."OBSERVATION" = s."OBSERVATION" WHERE p."VALUE" > (s.mean_val + (2 * s.std_val)) GROUP BY p."BATCH") SELECT r."EQUIPMENT", COUNT(DISTINCT r."BATCH") as batches_processed, SUM(s.spike_count) as total_sensor_spikes, AVG(EXTRACT(EPOCH FROM (r."TIME_END" - r."TIME_START"))) as avg_process_time_seconds FROM routing r JOIN spikes s ON r."BATCH" = s."BATCH" GROUP BY r."EQUIPMENT" ORDER BY total_sensor_spikes DESC;""",
    "6. The Needle": """SELECT r."BATCH", r."EQUIPMENT", r."TIME_START" as batch_start, r."TIME_END" as batch_end, q."OBSERVATION" as quality_test, q."VALUE" as quality_value FROM routing r JOIN quality q ON r."BATCH" = q."BATCH" WHERE r."BATCH" = 'BATCH_000402' LIMIT 1;"""
}

# -- B. Wide Table Queries --
wide_queries = {
    "0. The Total Count": """SELECT COUNT(*) as total_sensor_readings, COUNT(DISTINCT "BATCH") as total_batches, COUNT(DISTINCT "EQUIPMENT") as total_machines FROM wide_parameters;""",
    "1. The Big Scan": """SELECT "EQUIPMENT", "OBSERVATION" as sensor_name, COUNT("VALUE") as total_readings, AVG("VALUE") as avg_sensor_value, MAX("VALUE") as max_sensor_value FROM wide_parameters WHERE "QUALITY_TEST" = 'Final_Yield_Voltage' AND "QUALITY_VALUE" < 3.65 GROUP BY "EQUIPMENT", "OBSERVATION" ORDER BY avg_sensor_value DESC;""",
    "2. The Bigger Run Chart": """SELECT time_bucket('5 minutes', "TIMESTAMP") AS timestamp_bucket, "EQUIPMENT", AVG("VALUE") AS avg_value, MIN("VALUE") AS min_value, MAX("VALUE") AS max_value, STDDEV_POP("VALUE") AS standard_deviation FROM wide_parameters WHERE "OBSERVATION" = 'Sensor_0' GROUP BY timestamp_bucket, "EQUIPMENT" ORDER BY timestamp_bucket, "EQUIPMENT";""",
    "3. The Heat Map": """SELECT "EQUIPMENT"::text AS "EQUIPMENT", AVG(CASE WHEN EXTRACT('hour' FROM "TIMESTAMP") = 0 THEN "VALUE" END) AS hour_00, AVG(CASE WHEN EXTRACT('hour' FROM "TIMESTAMP") = 1 THEN "VALUE" END) AS hour_01, AVG(CASE WHEN EXTRACT('hour' FROM "TIMESTAMP") = 23 THEN "VALUE" END) AS hour_23 FROM wide_parameters WHERE "OBSERVATION"::text = 'Sensor_1' GROUP BY "EQUIPMENT"::text ORDER BY "EQUIPMENT"::text; """,
    "4. The Percentile Crunch": """SELECT "EQUIPMENT", "OBSERVATION" as sensor_name, percentile_cont(0.50) WITHIN GROUP (ORDER BY "VALUE") as median_value, percentile_cont(0.95) WITHIN GROUP (ORDER BY "VALUE") as p95_value, percentile_cont(0.99) WITHIN GROUP (ORDER BY "VALUE") as p99_value FROM wide_parameters GROUP BY "EQUIPMENT", "OBSERVATION";""",
    "5. The Data Detective": """WITH equip_stats AS MATERIALIZED (SELECT "EQUIPMENT", "OBSERVATION", AVG("VALUE") as mean_val, STDDEV_SAMP("VALUE") as std_val FROM wide_parameters GROUP BY "EQUIPMENT", "OBSERVATION"), spikes AS MATERIALIZED (SELECT p."BATCH", COUNT(*) as spike_count FROM wide_parameters p JOIN equip_stats s ON p."EQUIPMENT" = s."EQUIPMENT" AND p."OBSERVATION" = s."OBSERVATION" WHERE p."VALUE" > (s.mean_val + (2 * s.std_val)) GROUP BY p."BATCH") SELECT w."EQUIPMENT", COUNT(DISTINCT w."BATCH") as batches_processed, SUM(s.spike_count) as total_sensor_spikes, AVG(EXTRACT(EPOCH FROM (w."BATCH_END" - w."BATCH_START"))) as avg_process_time_seconds FROM wide_parameters w JOIN spikes s ON w."BATCH" = s."BATCH" GROUP BY w."EQUIPMENT" ORDER BY total_sensor_spikes DESC;""",
    "6. The Needle": """SELECT "BATCH", "EQUIPMENT", "BATCH_START", "BATCH_END", "QUALITY_TEST", "QUALITY_VALUE" FROM wide_parameters WHERE "BATCH" = 'BATCH_000402' LIMIT 1;"""
}

# -- C. DuckDB Parquet Queries --
duckdb_queries = {
    "0. The Total Count": f"SELECT COUNT(*) as total_sensor_readings, COUNT(DISTINCT batch_id) as total_batches, COUNT(DISTINCT equipment_id) as total_machines FROM read_parquet({PARQUET_SOURCE});",
    "1. The Big Scan": f"SELECT equipment_id, sensor_name, COUNT(sensor_value) as total_readings, AVG(sensor_value) as avg_sensor_value, MAX(sensor_value) as max_sensor_value FROM read_parquet({PARQUET_SOURCE}) WHERE quality_test = 'Final_Yield_Voltage' AND quality_value < 3.65 GROUP BY equipment_id, sensor_name ORDER BY avg_sensor_value DESC;",
    "2. The Bigger Run Chart": f"SELECT time_bucket(INTERVAL '5 minutes', sensor_time) AS timestamp_bucket, equipment_id, AVG(sensor_value) AS avg_value, MIN(sensor_value) AS min_value, MAX(sensor_value) AS max_value, STDDEV_POP(sensor_value) AS standard_deviation FROM read_parquet({PARQUET_SOURCE}) WHERE sensor_name = 'Sensor_0' GROUP BY timestamp_bucket, equipment_id ORDER BY timestamp_bucket, equipment_id;",
    "3. The Heat Map": f"PIVOT (SELECT equipment_id, CAST(EXTRACT('hour' FROM sensor_time) AS INT) AS hour_of_day, sensor_value FROM read_parquet({PARQUET_SOURCE}) WHERE sensor_name = 'Sensor_1') ON hour_of_day USING AVG(sensor_value) GROUP BY equipment_id ORDER BY equipment_id;",
    "4. The Percentile Crunch": f"SELECT equipment_id, sensor_name, quantile_cont(sensor_value, 0.50) as median_value, quantile_cont(sensor_value, 0.95) as p95_value, quantile_cont(sensor_value, 0.99) as p99_value FROM read_parquet({PARQUET_SOURCE}) GROUP BY equipment_id, sensor_name;",
    "5. The Data Detective": f"WITH equip_stats AS (SELECT equipment_id, sensor_name, AVG(sensor_value) as mean_val, STDDEV_SAMP(sensor_value) as std_val FROM read_parquet({PARQUET_SOURCE}) GROUP BY equipment_id, sensor_name), spikes AS (SELECT p.batch_id, COUNT(*) as spike_count FROM read_parquet({PARQUET_SOURCE}) p JOIN equip_stats s ON p.equipment_id = s.equipment_id AND p.sensor_name = s.sensor_name WHERE p.sensor_value > (s.mean_val + (2 * s.std_val)) GROUP BY p.batch_id) SELECT r.equipment_id, COUNT(DISTINCT r.batch_id) as batches_processed, SUM(s.spike_count) as total_sensor_spikes, AVG(date_diff('second', r.batch_start, r.batch_end)) as avg_process_time_seconds FROM read_parquet({PARQUET_SOURCE}) r JOIN spikes s ON r.batch_id = s.batch_id GROUP BY r.equipment_id ORDER BY total_sensor_spikes DESC;",
    "6. The Needle": f"SELECT batch_id, equipment_id, batch_start, batch_end, quality_test, quality_value FROM read_parquet({PARQUET_SOURCE}) WHERE batch_id = 'BATCH_000402' LIMIT 1;"
}


# =============================================================================
# --- 3. EXECUTION ENGINE ---
# =============================================================================

def execute_pg_query(cursor, query):
    start = time.time()
    cursor.execute(query)
    cursor.fetchall()  # Fetch to ensure query is fully resolved
    return time.time() - start


def execute_duck_query(con, query):
    start = time.time()
    con.execute(query).fetchall()
    return time.time() - start


def main():
    print(f"🚀 Starting Benchmark Engine (Samples per query: {NUM_SAMPLES})...\n")

    # Store times in a dictionary: times[query_name][system_name] = [time1, time2, ...]
    times = defaultdict(lambda: defaultdict(list))

    # --- Connect to Databases ---
    try:
        pg_conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
        pg_cursor = pg_conn.cursor()
        duck_con = duckdb.connect(':memory:')

        query_names = list(oltp_queries.keys())  # All dictionaries have the same keys

        for sample in range(1, NUM_SAMPLES + 1):
            print(f"--- Running Iteration {sample}/{NUM_SAMPLES} ---")

            for q_name in query_names:
                # 3. DuckDB (Parquet)
                t_duck = execute_duck_query(duck_con, duckdb_queries[q_name])
                times[q_name]['DuckDB (Parquet)'].append(t_duck)

                # 2. Wide Table (Postgres/Timescale)
                t_wide = execute_pg_query(pg_cursor, wide_queries[q_name])
                times[q_name]['Wide Table (PG)'].append(t_wide)

                # 1. Normalized OLTP (Postgres/Timescale)
                t_oltp = execute_pg_query(pg_cursor, oltp_queries[q_name])
                times[q_name]['Normalized OLTP (PG)'].append(t_oltp)

        # --- Calculate Averages and Format Output ---
        print("\n📊 Calculating Averages...\n")

        avg_results = []
        for q_name in query_names:
            row = {"Query": q_name}
            for system in ['Normalized OLTP (PG)', 'Wide Table (PG)', 'DuckDB (Parquet)']:
                # Calculate mean time for this system & query
                avg_time = sum(times[q_name][system]) / NUM_SAMPLES
                row[system] = round(avg_time, 4)
            avg_results.append(row)

        # Create Pandas DataFrame for a clean table output
        df = pd.DataFrame(avg_results)
        df.set_index('Query', inplace=True)

        print(df.to_markdown())

        # Optional: Save to CSV
        df.to_csv("benchmark_results.csv")
        print("\n✅ Results saved to benchmark_results.csv")

    except Exception as e:
        print(f"\n❌ Benchmark failed: {e}")
    finally:
        if 'pg_cursor' in locals(): pg_cursor.close()
        if 'pg_conn' in locals(): pg_conn.close()
        if 'duck_con' in locals(): duck_con.close()


if __name__ == "__main__":
    main()