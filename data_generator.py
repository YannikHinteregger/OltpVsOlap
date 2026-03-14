import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# --- Configuration (Tweak these for your "big" dataset) ---
NUM_BATCHES = 50_000  # Start with 50k batches. Increase to 1M+ for a real stress test.
EQUIPMENT_COUNT = 100  # Number of machines in the factory
SENSORS_PER_EQUIP = 10  # Number of parameters tracked per machine
READINGS_PER_BATCH = 500  # e.g., 1 reading per second for a 1-minute process
START_DATE = datetime(2025, 1, 1)

# Ensure output directory exists
os.makedirs("synthetic_data", exist_ok=True)

print(f"Generating synthetic factory data for {NUM_BATCHES} batches...")

# ---------------------------------------------------------
# 1. Generate Routing Data (Maps to FileType.ROUTING)
# ---------------------------------------------------------
print("Generating Routing Data...")
batch_ids = [f"BATCH_{str(i).zfill(6)}" for i in range(NUM_BATCHES)]
equip_ids = [f"EQ_{str(i).zfill(2)}" for i in range(EQUIPMENT_COUNT)]

# Randomize which equipment processes which batch, and create timestamps
routing_records = {
    "BATCH": batch_ids,
    "EQUIPMENT": np.random.choice(equip_ids, NUM_BATCHES),
    "TIME_START": [START_DATE + timedelta(minutes=i * 2) for i in range(NUM_BATCHES)],
}
# Add 1 minute process time to get TIME_END
routing_records["TIME_END"] = [t + timedelta(minutes=1) for t in routing_records["TIME_START"]]

df_routing = pd.DataFrame(routing_records)
df_routing.to_csv("synthetic_data/routing_data.csv", index=False)
print(f" -> Saved {len(df_routing)} routing rows.")

# ---------------------------------------------------------
# 2. Generate Parameter Data (Maps to FileType.PARAMETER)
# THIS IS THE HIGH VOLUME TABLE
# ---------------------------------------------------------
print("Generating Parameter (Time-Series) Data...")
# We generate data in chunks to avoid blowing up RAM on your machine
CHUNK_SIZE = 10_000
total_param_rows = 0

with open("synthetic_data/parameter_data.csv", "w") as f:
    # Write header
    f.write("EQUIPMENT,OBSERVATION,TIMESTAMP,VALUE,BATCH\n")

    for i in range(0, NUM_BATCHES, CHUNK_SIZE):
        chunk_batches = df_routing.iloc[i:i + CHUNK_SIZE]

        chunk_rows = []
        for _, row in chunk_batches.iterrows():
            # Generate 60 seconds of data for each sensor on this equipment
            timestamps = pd.date_range(start=row["TIME_START"], periods=READINGS_PER_BATCH, freq='S')

            for sensor_id in range(SENSORS_PER_EQUIP):
                obs_name = f"Sensor_{sensor_id}"
                # Generate a random walk for sensor values (looks more realistic than pure noise)
                values = np.cumsum(np.random.normal(0, 0.5, READINGS_PER_BATCH)) + 50

                # Build the dataframe for this specific sensor's batch run
                temp_df = pd.DataFrame({
                    "EQUIPMENT": row["EQUIPMENT"],
                    "OBSERVATION": obs_name,
                    "TIMESTAMP": timestamps,
                    "VALUE": values,
                    "BATCH": row["BATCH"]
                })
                chunk_rows.append(temp_df)

        # Concat and write chunk to disk
        df_chunk = pd.concat(chunk_rows)
        df_chunk.to_csv(f, header=False, index=False)
        total_param_rows += len(df_chunk)
        print(f" -> Processed {i + CHUNK_SIZE} batches...")

print(f" -> Saved {total_param_rows} parameter rows.")

# ---------------------------------------------------------
# 3. Generate Quality Data (Maps to FileType.QUALITY)
# ---------------------------------------------------------
print("Generating Quality Data...")
# Simulate an end-of-line test for every batch
df_quality = pd.DataFrame({
    "BATCH": batch_ids,
    "OBSERVATION": "Final_Yield_Voltage",
    # Generate values around 3.7V, with some random outliers
    "VALUE": np.random.normal(3.7, 0.05, NUM_BATCHES),
    "TIMESTAMP": df_routing["TIME_END"] + timedelta(minutes=5)  # Quality test happens 5 mins after process
})

df_quality.to_csv("synthetic_data/quality_data.csv", index=False)
print(f" -> Saved {len(df_quality)} quality rows.")
print("\nDone! Ready for OLTP vs OLAP destruction testing.")