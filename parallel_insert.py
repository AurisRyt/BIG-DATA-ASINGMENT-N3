import pandas as pd
from pymongo import MongoClient
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import os
import sys
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("insert_processing.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# SETTINGS
CSV_PATH = os.path.join("MONGO CSV", "aisdk-2025-03-17.csv")
CHUNK_SIZE = 5000  # Optimal chunk size for memory management
N_PROCESSES = 4  # Number of parallel processes
MONGO_URI = "mongodb://mongos:27017"
DB_NAME = "vesselDB"
COLLECTION_NAME = "raw_data"
TARGET_ROWS = 5000000  # Target number of rows to process

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


def process_chunk(chunk_df):
    """Insert one chunk of data into MongoDB with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
            collection = client[DB_NAME][COLLECTION_NAME]

            records = chunk_df.to_dict(orient="records")
            if records:
                collection.insert_many(records, ordered=False)  # Using unordered for better performance
            client.close()
            return len(records)
        except Exception as e:
            logging.error(f"Error processing chunk (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)  # Wait before retrying
            else:
                # After all retries, try to insert records one by one as last resort
                try:
                    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
                    collection = client[DB_NAME][COLLECTION_NAME]
                    successful = 0
                    records = chunk_df.to_dict(orient="records")
                    for record in records:
                        try:
                            collection.insert_one(record)
                            successful += 1
                        except Exception:
                            pass  # Skip failed records
                    client.close()
                    return successful
                except Exception as e2:
                    logging.error(f"Failed to process chunk even with individual inserts: {str(e2)}")
                    return 0


def main():
    start_time = time.time()

    try:
        # Check if CSV file exists
        if not os.path.exists(CSV_PATH):
            logging.error(f"CSV file not found: {CSV_PATH}")
            print(f"CSV file not found: {CSV_PATH}")
            return

        total_rows_possible = sum(1 for _ in open(CSV_PATH)) - 1
        logging.info(f"Total rows in file (excluding header): {total_rows_possible}")
        print(f"Total rows in file (excluding header): {total_rows_possible}")
        print(f"Target rows to process: {TARGET_ROWS}")

        # Connect to MongoDB to ensure it's available
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            client.close()
            logging.info("MongoDB connection successful")

            # Create indexes in advance
            client = MongoClient(MONGO_URI)
            db = client[DB_NAME]
            db[COLLECTION_NAME].create_index("MMSI")
            client.close()
            logging.info("Created index on MMSI field")

        except Exception as e:
            logging.error(f"MongoDB connection error: {str(e)}")
            print(f"Failed to connect to MongoDB: {str(e)}")
            return

        # Process data in batches to limit memory usage
        batch_size = 10  # Process 10 chunks at a time
        total_chunks = (TARGET_ROWS + CHUNK_SIZE - 1) // CHUNK_SIZE
        total_inserted = 0

        for batch_start in range(0, total_chunks, batch_size):
            batch_end = min(batch_start + batch_size, total_chunks)
            chunks = []

            # Read a batch of chunks
            with pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE) as reader:
                for i, chunk_df in enumerate(reader):
                    if i < batch_start * CHUNK_SIZE // CHUNK_SIZE:
                        continue
                    if i >= batch_end * CHUNK_SIZE // CHUNK_SIZE:
                        break

                    # Handle last chunk that might exceed TARGET_ROWS
                    if total_inserted + len(chunk_df) > TARGET_ROWS:
                        remaining = TARGET_ROWS - total_inserted
                        chunk_df = chunk_df.head(remaining)

                    chunks.append(chunk_df)

            logging.info(f"Processing batch {batch_start // batch_size + 1}: chunks {batch_start}-{batch_end - 1}")
            print(f"Processing batch {batch_start // batch_size + 1}: chunks {batch_start}-{batch_end - 1}")

            # Process this batch of chunks
            batch_inserted = 0
            with Pool(processes=N_PROCESSES) as pool:
                for result in tqdm(pool.imap_unordered(process_chunk, chunks), total=len(chunks)):
                    batch_inserted += result
                    total_inserted += result

                    # Log progress periodically
                    if total_inserted % 100000 == 0:
                        elapsed = time.time() - start_time
                        rate = total_inserted / elapsed
                        logging.info(
                            f"Progress: {total_inserted} rows inserted in {elapsed:.2f} seconds ({rate:.2f} docs/sec)")

            logging.info(f"Batch complete: inserted {batch_inserted} rows")

            # Check if we've reached the target
            if total_inserted >= TARGET_ROWS:
                break

        end_time = time.time()
        elapsed = end_time - start_time
        rate = total_inserted / elapsed

        print(f"Total rows inserted: {total_inserted}")
        print(f"Total time: {elapsed:.2f} seconds ({elapsed / 60:.2f} minutes)")
        print(f"Insertion rate: {rate:.2f} documents per second")
        logging.info(f"Total rows inserted: {total_inserted}")
        logging.info(f"Total time: {elapsed:.2f} seconds ({elapsed / 60:.2f} minutes)")
        logging.info(f"Insertion rate: {rate:.2f} documents per second")

    except Exception as e:
        logging.error(f"Unhandled error: {str(e)}")
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()