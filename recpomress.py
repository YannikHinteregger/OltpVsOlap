import psycopg2
import time

conn = psycopg2.connect(host="localhost", database="db", user="admin", password="admin", port="5432")
conn.autocommit = True
cursor = conn.cursor()

try:
    cursor.execute("SELECT show_chunks('wide_parameters');")
    chunks = [c[0] for c in cursor.fetchall()]

    print(f"Found {len(chunks)} chunks to re-process.")

    for i, chunk in enumerate(chunks, 1):
        print(f"[{i}/{len(chunks)}] Processing {chunk}...")

        # Decompress if already compressed (ignore error if already decompressed)
        try:
            print("  -> Decompressing... ", end="", flush=True)
            cursor.execute(f"SELECT decompress_chunk('{chunk}', if_compressed => True);")
            print("Done.")
        except Exception as e:
            print(f"Already decompressed or error: {e}")

        # Compress
        print("  -> Compressing...   ", end="", flush=True)
        start = time.time()
        cursor.execute(f"SELECT compress_chunk('{chunk}');")
        print(f"Done in {time.time() - start:.2f}s")

    print("\n✅ All chunks re-compressed successfully!")

finally:
    cursor.close()
    conn.close()