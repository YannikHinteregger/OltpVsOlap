import s
import time
import os

# --- 1. Database Credentials ---
DB_HOST = "localhost"
DB_NAME = "db"
DB_USER = "admin"
DB_PASS = "admin"
DB_PORT = "5432"

# Where your generated CSV files live
CSV_DIR = "synthetic_data"

# --- 2. SQL Definitions ---
# We drop tables if they exist so you can safely run this script multiple times
SETUP_SQL = """
DROP TABLE IF EXISTS routing CASCADE;
DROP TABLE IF EXISTS quality CASCADE;
DROP TABLE IF EXISTS parameters CASCADE;

-- 1. Create Context Table: Routing
CREATE TABLE routing (
    "BATCH" VARCHAR(50) PRIMARY KEY,
    "EQUIPMENT" VARCHAR(50),
    "TIME_START" TIMESTAMP,
    "TIME_END" TIMESTAMP
);

-- 2. Create Target Table: Quality metrics
CREATE TABLE quality (
    id SERIAL PRIMARY KEY,
    "BATCH" VARCHAR(50),
    "OBSERVATION" VARCHAR(50),
    "VALUE" DOUBLE PRECISION,
    "TIMESTAMP" TIMESTAMP
);

-- 3. Create Fact Table: High-frequency sensor parameters 
CREATE TABLE parameters (
    "EQUIPMENT" VARCHAR(50),
    "OBSERVATION" VARCHAR(50),
    "TIMESTAMP" TIMESTAMP NOT NULL,
    "VALUE" DOUBLE PRECISION,
    "BATCH" VARCHAR(50)
);

-- 4. Convert parameters to a TimescaleDB Hypertable
SELECT create_hypertable('parameters', 'TIMESTAMP', if_not_exists => TRUE);
"""

INDEX_SQL = """
-- Build indexes AFTER loading data for maximum upload speed
CREATE INDEX idx_param_obs ON parameters("OBSERVATION", "TIMESTAMP" DESC);
CREATE INDEX idx_param_batch ON parameters("BATCH", "TIMESTAMP" DESC);
CREATE INDEX idx_param_equip ON parameters("EQUIPMENT", "TIMESTAMP" DESC);
CREATE INDEX idx_quality_batch ON quality("BATCH");
"""

# Map the files to their exact columns so the DB doesn't get confused by the auto-incrementing ID
FILES_TO_UPLOAD = {
    "routing": {
        "path": os.path.join(CSV_DIR, "routing_data.csv"),
        "columns": '("BATCH", "EQUIPMENT", "TIME_START", "TIME_END")'
    },
    "quality": {
        "path": os.path.join(CSV_DIR, "quality_data.csv"),
        "columns": '("BATCH", "OBSERVATION", "VALUE", "TIMESTAMP")'
    },
    "parameters": {
        "path": os.path.join(CSV_DIR, "parameter_data.csv"),
        "columns": '("EQUIPMENT", "OBSERVATION", "TIMESTAMP", "VALUE", "BATCH")'
    }
}


def main():
    print("Connecting to TimescaleDB...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Step 1: Create Tables
        print("\nCreating tables and setting up TimescaleDB Hypertable...")
        cursor.execute(SETUP_SQL)
        print(" -> Tables created successfully.")

        # Step 2: Upload Data
        print("\nStarting Data Upload (this might take a minute for the parameters table)...")
        for table_name, file_info in FILES_TO_UPLOAD.items():
            file_path = file_info["path"]
            columns = file_info["columns"]

            if not os.path.exists(file_path):
                print(f" -> ERROR: File not found: {file_path}. Skipping.")
                continue

            print(f" -> Uploading {file_path} into table '{table_name}'...")
            start_time = time.time()

            with open(file_path, 'r') as f:
                # Tell Postgres exactly which columns are coming from the CSV
                sql_command = f"COPY {table_name}{columns} FROM STDIN WITH CSV HEADER DELIMITER as ','"
                cursor.copy_expert(sql=sql_command, file=f)

            duration = time.time() - start_time
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            row_count = cursor.fetchone()[0]
            print(f"    Success! Loaded {row_count:,} rows in {duration:.2f} seconds.")

        # Step 3: Build Indexes
        print("\nBuilding indexes to optimize query speed...")
        index_start = time.time()
        cursor.execute(INDEX_SQL)
        print(f" -> Indexes built in {time.time() - index_start:.2f} seconds.")

        print("\nAll done! Your database is fully armed and operational.")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()