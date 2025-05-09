import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
from tqdm import tqdm
from datetime import datetime
import numpy as np
import os
import time
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('delta_t_processing.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

print('Script starting...')
print('Modules imported successfully')

# Create output directories
os.makedirs('plots', exist_ok=True)

# Connection settings
MONGO_URI = 'mongodb://mongos:27017'
DB_NAME = 'vesselDB'
COLLECTION_NAME = 'filtered_data'
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


def parse_timestamp(timestamp_str):
    """Parse timestamp string into datetime object."""
    try:
        return datetime.strptime(timestamp_str, '%d/%m/%Y %H:%M:%S')
    except ValueError:
        # Try alternative format if first fails
        try:
            return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None


def process_vessel_data(mmsi, documents):
    """Process documents for a vessel and calculate time differences."""
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(documents)

    # Parse timestamps
    df['datetime'] = df['# Timestamp'].apply(parse_timestamp)

    # Drop rows with invalid timestamps
    df = df.dropna(subset=['datetime'])

    # Sort by timestamp
    df = df.sort_values('datetime')

    # Calculate time differences in milliseconds
    df['delta_t_ms'] = df['datetime'].diff().dt.total_seconds() * 1000

    # Drop the first row (NaN delta_t)
    df = df.dropna(subset=['delta_t_ms'])

    return df


def get_vessel_documents(collection, mmsi):
    """Get documents for a vessel with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            # Get all documents for this MMSI
            docs = list(collection.find({'MMSI': mmsi}))
            return docs
        except Exception as e:
            logging.error(f'Error retrieving documents for MMSI {mmsi} (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}')
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
    return []


def main():
    start_time = time.time()

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # Get distinct MMSIs
        distinct_mmsis = collection.distinct('MMSI')
        logging.info(f'Found {len(distinct_mmsis)} distinct vessel IDs in filtered data')
        print(f'Found {len(distinct_mmsis)} distinct vessel IDs in filtered data')

        # Store all delta_t values
        all_delta_t = []

        # Process each vessel
        processed_vessels = 0

        # Process all vessels
        for mmsi in tqdm(distinct_mmsis, desc='Processing vessels'):
            # Get all documents for this MMSI
            docs = get_vessel_documents(collection, mmsi)

            # Skip if too few documents
            if len(docs) < 2:
                continue

            # Process this vessel's data
            df = process_vessel_data(mmsi, docs)

            # If valid data exists
            if not df.empty and len(df) > 1:
                # Collect delta_t values
                values = df['delta_t_ms'].tolist()
                # Filter out unreasonable values (negative or extremely large)
                values = [v for v in values if 0 < v < 3600000]  # Between 0 and 1 hour
                all_delta_t.extend(values)
                processed_vessels += 1

        logging.info(f'Processed {processed_vessels} vessels with valid time data')
        print(f'Processed {processed_vessels} vessels with valid time data')
        logging.info(f'Collected {len(all_delta_t)} delta_t values')
        print(f'Collected {len(all_delta_t)} delta_t values')

        # Convert to numpy array
        delta_t_array = np.array(all_delta_t)

        # Print the statistics
        print('\nTime Difference (Delta t) Statistics:')
        print(f'Total measurements: {len(delta_t_array)}')
        print(f'Mean: {np.mean(delta_t_array):.2f} ms')
        print(f'Median: {np.median(delta_t_array):.2f} ms')
        print(f'Min: {np.min(delta_t_array):.2f} ms')
        print(f'Max: {np.max(delta_t_array):.2f} ms')
        print(f'Standard Deviation: {np.std(delta_t_array):.2f} ms')

        # Analysis of time intervals
        intervals = [
            (0, 1000, '0-1 second'),
            (1000, 5000, '1-5 seconds'),
            (5000, 60000, '5 seconds - 1 minute'),
            (60000, 300000, '1-5 minutes'),
            (300000, 3600000, '5 minutes - 1 hour'),
            (3600000, float('inf'), 'More than 1 hour')
        ]

        print('\nDistribution of Time Intervals:')
        for start, end, label in intervals:
            count = np.sum((delta_t_array >= start) & (delta_t_array < end))
            percentage = (count / len(delta_t_array)) * 100
            print(f'{label}: {count} ({percentage:.2f}%)')

        # Detailed time categories
        detailed_intervals = [
            (0, 1, '0-1 ms'),
            (1, 10, '1-10 ms'),
            (10, 100, '10-100 ms'),
            (100, 1000, '100-1000 ms'),
            (1000, 5000, '1-5 seconds'),
            (5000, 10000, '5-10 seconds'),
            (10000, 30000, '10-30 seconds'),
            (30000, 60000, '30-60 seconds'),
            (60000, 300000, '1-5 minutes'),
            (300000, 3600000, '5-60 minutes'),
            (3600000, float('inf'), 'Over 1 hour')
        ]

        for start, end, label in detailed_intervals:
            count = np.sum((delta_t_array >= start) & (delta_t_array < end))
            percentage = (count / len(delta_t_array)) * 100
            print(f'{label}: {count} ({percentage:.2f}%)')

        # VISUALIZATION 1: Overall Time Difference Histogram (1st-99th percentile)
        # Filter outliers (keep values between 1st and 99th percentile)
        p1 = np.percentile(delta_t_array, 1)
        p99 = np.percentile(delta_t_array, 99)
        filtered_delta_t = delta_t_array[(delta_t_array >= p1) & (delta_t_array <= p99)]

        plt.figure(figsize=(12, 8))
        plt.hist(filtered_delta_t, bins=100, alpha=0.75)

        # Add statistics
        mean_delta = np.mean(filtered_delta_t)
        median_delta = np.median(filtered_delta_t)
        min_delta = np.min(filtered_delta_t)
        max_delta = np.max(filtered_delta_t)

        # Add text box with statistics
        stats_text = (
            f'Mean: {mean_delta:.2f} ms\n'
            f'Median: {median_delta:.2f} ms\n'
            f'Min: {min_delta:.2f} ms\n'
            f'Max: {max_delta:.2f} ms\n'
            f'Sample size: {len(filtered_delta_t)} points'
        )

        plt.annotate(stats_text, xy=(0.7, 0.8), xycoords='axes fraction',
                     bbox=dict(boxstyle='round,pad=0.5', fc='white', alpha=0.8))

        plt.title('Overall Time Difference Histogram (1st-99th percentile)')
        plt.xlabel('Time Difference (ms)')
        plt.ylabel('Frequency')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('plots/1_overall_histogram.png')
        plt.close()

        # VISUALIZATION 3: Non-zero Time Difference Histogram
        non_zero_delta_t = filtered_delta_t[filtered_delta_t > 0]
        plt.figure(figsize=(12, 8))
        plt.hist(non_zero_delta_t, bins=100, alpha=0.75)

        non_zero_stats = (
            f'Mean: {np.mean(non_zero_delta_t):.2f} ms\n'
            f'Median: {np.median(non_zero_delta_t):.2f} ms\n'
            f'Min: {np.min(non_zero_delta_t):.2f} ms\n'
            f'Max: {np.max(non_zero_delta_t):.2f} ms\n'
            f'Sample size: {len(non_zero_delta_t)} points'
        )

        plt.annotate(non_zero_stats, xy=(0.7, 0.8), xycoords='axes fraction',
                     bbox=dict(boxstyle='round,pad=0.5', fc='white', alpha=0.8))

        plt.title('Time Difference Histogram (Excluding Zero Values)')
        plt.xlabel('Time Difference (ms)')
        plt.ylabel('Frequency')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('plots/3_non_zero_histogram.png')
        plt.close()

        # VISUALIZATION 2: Time Categories Distribution
        # Calculate category counts
        counts = []
        labels = []
        for start, end, label in detailed_intervals:
            count = np.sum((delta_t_array >= start) & (delta_t_array < end))
            percentage = (count / len(delta_t_array)) * 100
            labels.append(f'{label}\n{percentage:.1f}%')
            counts.append(count)

        # Create a bar chart of categories
        plt.figure(figsize=(14, 8))
        bars = plt.bar(range(len(labels)), counts, width=0.7)
        plt.xticks(range(len(labels)), labels, rotation=0)
        plt.title('Time Difference Categories Distribution')
        plt.ylabel('Number of Data Points')
        plt.grid(axis='y', alpha=0.3)

        # Add count labels on top of bars
        for bar in bars:
            height = bar.get_height()
            if height > 0:  # Only add text for non-zero bars
                plt.text(bar.get_x() + bar.get_width() / 2., height,
                         f'{height:,}',
                         ha='center', va='bottom', rotation=0)

        plt.tight_layout()
        plt.savefig('plots/2_category_distribution.png')
        plt.close()

        # Log completion time
        elapsed_time = time.time() - start_time
        logging.info(f'Analysis complete! Total time: {elapsed_time:.2f} seconds')
        print(f'Analysis complete! Total time: {elapsed_time:.2f} seconds')
        print('All visualizations have been created successfully!')

    except Exception as e:
        logging.error(f'Error during delta t analysis: {str(e)}')
        print(f'An error occurred: {str(e)}')


if __name__ == '__main__':
    main()

