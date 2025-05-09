import pandas as pd
from pymongo import MongoClient
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import sys
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("filter_processing.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# SETTINGS
MONGO_URI = "mongodb://mongos:27017"
DB_NAME = "vesselDB"
SOURCE_COLLECTION = "raw_data"
TARGET_COLLECTION = "filtered_data"
N_PROCESSES = 4  # Parallel processes
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


def process_vessel_batch(mmsi_batch):
    """Process a batch of MMSIs in parallel with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
            db = client[DB_NAME]

            vessels_processed = 0
            vessels_filtered = 0
            total_docs = 0
            filtered_docs = 0

            for mmsi in mmsi_batch:
                # Get all docs for this MMSI
                docs = list(db[SOURCE_COLLECTION].find({"MMSI": mmsi}))
                total_docs += len(docs)
                vessels_processed += 1

                # Skip vessels with less than 100 data points
                if len(docs) < 100:
                    continue

                # Process documents for this vessel
                filtered = []
                for doc in docs:
                    # Remove _id
                    if '_id' in doc:
                        del doc['_id']

                    # Check required fields
                    required_fields = ['MMSI', 'Latitude', 'Longitude', 'Navigational status']
                    has_required = all(
                        field in doc and
                        doc[field] is not None and
                        str(doc[field]).lower() != 'nan' and
                        doc[field] != '' and
                        not pd.isna(doc[field])
                        for field in required_fields
                    )

                    if not has_required:
                        continue

                    # Validate numeric fields
                    numeric_fields = ['ROT', 'SOG', 'COG', 'Heading']
                    for field in numeric_fields:
                        if field in doc and doc[field] is not None:
                            try:
                                val = float(doc[field])
                                if not pd.isna(val):
                                    doc[field] = val
                                else:
                                    doc[field] = None
                            except (ValueError, TypeError):
                                doc[field] = None

                    # Add to filtered documents
                    filtered.append(doc)

                # Insert filtered documents in smaller batches to avoid size limits
                if filtered:
                    batch_size = 500  # Smaller batches for better performance
                    for i in range(0, len(filtered), batch_size):
                        batch = filtered[i:i + batch_size]
                        if batch:
                            db[TARGET_COLLECTION].insert_many(batch, ordered=False)

                    filtered_docs += len(filtered)
                    vessels_filtered += 1

            client.close()
            return vessels_processed, vessels_filtered, total_docs, filtered_docs

        except Exception as e:
            logging.error(f"Error processing batch (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                logging.error("Failed all retries for batch processing")
                return 0, 0, 0, 0


def main():
    try:
        start_time = time.time()

        # Connect to MongoDB
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
            client.admin.command('ping')
            logging.info("MongoDB connection successful")
        except Exception as e:
            logging.error(f"MongoDB connection error: {str(e)}")
            print(f"Failed to connect to MongoDB: {str(e)}")
            return

        db = client[DB_NAME]

        # Clear any existing data
        db[TARGET_COLLECTION].drop()
        logging.info(f"Dropped existing {TARGET_COLLECTION} collection")

        # Create needed indexes
        db[SOURCE_COLLECTION].create_index("MMSI")
        db[TARGET_COLLECTION].create_index("MMSI")
        db[TARGET_COLLECTION].create_index("# Timestamp")
        logging.info("Created required indexes")

        # Get all unique MMSIs
        distinct_mmsis = db[SOURCE_COLLECTION].distinct("MMSI")
        count_distinct = len(distinct_mmsis)
        logging.info(f"Found {count_distinct} distinct vessel IDs")
        print(f"Found {count_distinct} distinct vessel IDs")

        # Divide MMSIs into batches for parallel processing
        batch_size = max(1,
                         count_distinct // (N_PROCESSES * 2))  # More batches than processes for better load balancing
        mmsi_batches = [distinct_mmsis[i:i + batch_size] for i in range(0, count_distinct, batch_size)]

        print(f"Processing in {len(mmsi_batches)} batches with {N_PROCESSES} processes...")
        logging.info(f"Processing in {len(mmsi_batches)} batches with {N_PROCESSES} processes")

        # Process in parallel
        total_vessels_processed = 0
        total_vessels_filtered = 0
        total_docs = 0
        total_filtered_docs = 0

        with Pool(processes=N_PROCESSES) as pool:
            results = []
            for result in tqdm(pool.imap_unordered(process_vessel_batch, mmsi_batches), total=len(mmsi_batches)):
                vessels_processed, vessels_filtered, docs, filtered_docs = result
                total_vessels_processed += vessels_processed
                total_vessels_filtered += vessels_filtered
                total_docs += docs
                total_filtered_docs += filtered_docs

        client.close()

        elapsed_time = time.time() - start_time

        # Print results
        print("\nFiltering Results:")
        print(f"Vessels processed: {total_vessels_processed}")
        print(f"Vessels with ≥100 data points: {total_vessels_filtered}")
        print(f"Total documents: {total_docs}")
        print(f"Filtered documents: {total_filtered_docs}")
        if total_docs > 0:
            print(f"Reduction: {((total_docs - total_filtered_docs) / total_docs * 100):.2f}%")
        print(f"Total processing time: {elapsed_time:.2f} seconds")

        logging.info("\nFiltering Results:")
        logging.info(f"Vessels processed: {total_vessels_processed}")
        logging.info(f"Vessels with ≥100 data points: {total_vessels_filtered}")
        logging.info(f"Total documents: {total_docs}")
        logging.info(f"Filtered documents: {total_filtered_docs}")
        if total_docs > 0:
            logging.info(f"Reduction: {((total_docs - total_filtered_docs) / total_docs * 100):.2f}%")
        logging.info(f"Total processing time: {elapsed_time:.2f} seconds")

    except Exception as e:
        logging.error(f"Unhandled error: {str(e)}")
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()