import psycopg2
import time

# --- Database Credentials ---
DB_HOST = "localhost"
DB_NAME = "db"
DB_USER = "admin"
DB_PASS = "admin"
DB_PORT = "5432"


def main():
    print("Connecting to TimescaleDB to build the OLAP Wide Table...")
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

        # STEP 1: Create the table and hypertable
        print("\n[1/4] Creating 'wide_parameters' table and Hypertable...")
        cursor.execute("""
            DROP TABLE IF EXISTS wide_parameters CASCADE;

            CREATE TABLE wide_parameters (
                "TIMESTAMP" TIMESTAMP NOT NULL,
                "EQUIPMENT" VARCHAR(50),
                "OBSERVATION" VARCHAR(50),
                "VALUE" DOUBLE PRECISION,
                "BATCH" VARCHAR(50),
                "BATCH_START" TIMESTAMP,
                "BATCH_END" TIMESTAMP,
                "QUALITY_TEST" VARCHAR(50),
                "QUALITY_VALUE" DOUBLE PRECISION
            );

            SELECT create_hypertable('wide_parameters', 'TIMESTAMP');
        """)
        print(" -> Done.")

        # STEP 2: The ELT Migration
        print("\n[2/4] Flattening and migrating data (This is the heavy join. Grab a coffee!)...")
        start_elt = time.time()
        cursor.execute("""
            INSERT INTO wide_parameters
            SELECT 
                p."TIMESTAMP",
                p."EQUIPMENT",
                p."OBSERVATION",
                p."VALUE",
                p."BATCH",
                r."TIME_START" as "BATCH_START",
                r."TIME_END" as "BATCH_END",
                q."OBSERVATION" as "QUALITY_TEST",
                q."VALUE" as "QUALITY_VALUE"
            FROM parameters p
            LEFT JOIN routing r ON p."BATCH" = r."BATCH"
            LEFT JOIN quality q ON p."BATCH" = q."BATCH";
        """)
        print(f" -> Migration complete in {time.time() - start_elt:.2f} seconds.")

        # STEP 3: Indexing
        print("\n[3/4] Building OLAP indexes...")
        start_idx = time.time()
        cursor.execute("""
            CREATE INDEX idx_wide_equip_obs ON wide_parameters("EQUIPMENT", "OBSERVATION", "TIMESTAMP" DESC);
            CREATE INDEX idx_wide_batch ON wide_parameters("BATCH", "TIMESTAMP" DESC);
        """)
        print(f" -> Indexes built in {time.time() - start_idx:.2f} seconds.")

        # STEP 4: Compression
        print("\n[4/4] Enabling and triggering TimescaleDB columnar compression...")
        start_comp = time.time()
        cursor.execute("""
            ALTER TABLE wide_parameters SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = '"EQUIPMENT", "OBSERVATION"',
                timescaledb.compress_orderby = '"TIMESTAMP" DESC'
            );
            SELECT compress_chunk(i) FROM show_chunks('wide_parameters') i;
        """)
        print(f" -> Compression finished in {time.time() - start_comp:.2f} seconds.")

        print("\n✅ OLAP Schema successfully built and populated!")

    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()


if __name__ == "__main__":
    main()