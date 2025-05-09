import time
import sys
from pymongo import MongoClient
from datetime import datetime
import random
import threading

# Configuration
MONGO_URI = "mongodb://mongos:27017"
DB_NAME = "vesselDB"
COLLECTION = "filtered_data"
QUERY_INTERVAL = 1  # seconds between queries
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


def print_separator():
    print("=" * 80)


def print_header(title):
    """Print a formatted header."""
    print_separator()
    print(f" {title} ".center(80))
    print_separator()


def run_continuous_queries(stop_event):
    """Run continuous queries against the MongoDB cluster."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        collection = db[COLLECTION]

        # Get all MMSIs for random sampling - without limit parameter
        distinct_mmsis = list(collection.distinct("MMSI"))
        # Take a sample of 20 MMSIs if there are more
        if len(distinct_mmsis) > 20:
            distinct_mmsis = random.sample(distinct_mmsis, 20)

        if not distinct_mmsis:
            print("No MMSIs found in the filtered_data collection!")
            return

        query_count = 0

        while not stop_event.is_set():
            try:
                # Random MMSI for this query
                mmsi = random.choice(distinct_mmsis)

                # Track query timing
                start_time = time.time()

                # Execute query
                result = list(collection.find({'MMSI': mmsi}, limit=5))

                # Calculate query time
                query_time = (time.time() - start_time) * 1000  # in ms

                timestamp = datetime.now().strftime("%H:%M:%S")
                print(
                    f"[{timestamp}] Query #{query_count}: MMSI={mmsi}, Found {len(result)} docs, Time: {query_time:.2f}ms")
                query_count += 1

                # Wait for next query
                time.sleep(QUERY_INTERVAL)

            except Exception as e:
                # If query fails, report but keep trying
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"[{timestamp}] Query #{query_count} ERROR: {str(e)}")

                # Retry with backoff
                for retry in range(MAX_RETRIES):
                    try:
                        # Reconnect to MongoDB
                        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
                        db = client[DB_NAME]
                        collection = db[COLLECTION]

                        # Attempt query again
                        start_time = time.time()
                        result = list(collection.find({'MMSI': mmsi}, limit=5))
                        query_time = (time.time() - start_time) * 1000

                        timestamp = datetime.now().strftime("%H:%M:%S")
                        print(
                            f"[{timestamp}] Query #{query_count} RECOVERED: MMSI={mmsi}, Found {len(result)} docs, Time: {query_time:.2f}ms")
                        break
                    except Exception:
                        retry_delay = RETRY_DELAY * (retry + 1)  # Exponential backoff
                        print(f"[{timestamp}] Retry {retry + 1}/{MAX_RETRIES} failed, waiting {retry_delay}s")
                        time.sleep(retry_delay)

                time.sleep(QUERY_INTERVAL)
                query_count += 1

        client.close()

    except Exception as e:
        print(f"Error in continuous queries: {str(e)}")


def main():
    """Main function for fault tolerance demo."""

    # Start continuous queries in a separate thread
    print_header("Starting Continuous Queries")

    stop_event = threading.Event()
    query_thread = threading.Thread(target=run_continuous_queries, args=(stop_event,))
    query_thread.daemon = True
    query_thread.start()

    # Wait for queries to stabilize
    time.sleep(5)

    try:
        # Keep running until user interrupts
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping demo...")
        stop_event.set()
        query_thread.join(timeout=2)
        print("Demo stopped")


if __name__ == "__main__":
    main()