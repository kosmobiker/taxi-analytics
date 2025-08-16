import pandas as pd
import glob
from pathlib import Path
import numpy as np
from tqdm import tqdm
import time
from clickhouse_connect import get_client
import gc
from typing import Generator
import os
import argparse
from urllib.parse import urlparse
import pyarrow.parquet as pq
from datetime import date

class TaxiDataUploader:
    """
    Base class for uploading taxi data to a ClickHouse database.
    Handles connection, batch processing, and general file operations.
    """
    def __init__(self, connection_string: str, table_name: str, batch_size: int = 50000):
        """
        Initialize the uploader with connection details.

        Args:
            connection_string: ClickHouse connection string.
            table_name: The name of the ClickHouse table to upload to.
            batch_size: Number of rows to process in each batch.
        """
        self.batch_size = batch_size
        self.table_name = table_name
        self.client = self._create_client(connection_string)
        print(f"✅ Connected to ClickHouse successfully for table: {self.table_name}")

    def _create_client(self, connection_string: str):
        """Create ClickHouse client from connection string."""
        try:
            parsed = urlparse(connection_string)
            host = parsed.hostname
            port = parsed.port or 8123
            username = parsed.username
            password = parsed.password
            database = parsed.path.lstrip('/') or 'default'
            secure = 'secure=true' in parsed.query or port == 8443
            
            client = get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database,
                secure=secure,
                query_limit=1_000_000_000 # Increased query limit to avoid throttling on large inserts
            )
            
            client.query("SELECT 1").result_rows
            return client
        except Exception as e:
            print(f"❌ Failed to connect to ClickHouse: {str(e)}")
            print("Expected format: 'clickhouse://username:password@host:port/database?secure=true'")
            raise
            
    def process_parquet_in_batches(self, file_path: str) -> Generator[pd.DataFrame, None, None]:
        """Read a parquet file in batches using PyArrow for memory optimization."""
        try:
            parquet_file = pq.ParquetFile(file_path)
            num_row_groups = parquet_file.num_row_groups
            total_rows = parquet_file.metadata.num_rows

            print(f"  Total rows in file: {total_rows:,}, across {num_row_groups} row groups.")

            for i in tqdm(range(num_row_groups), desc="Processing row groups"):
                table = parquet_file.read_row_group(i)
                batch_df = table.to_pandas()
                yield batch_df
                
                del table, batch_df
                gc.collect()
        except Exception as e:
            print(f"❌ Error reading parquet file {file_path}: {str(e)}")
            raise
            
    def _standardize_columns(self, df: pd.DataFrame, mappings: dict) -> pd.DataFrame:
        """Standardize column names to a consistent casing based on a mapping."""
        for old_name, new_name in mappings.items():
            if old_name in df.columns:
                df = df.rename(columns={old_name: new_name})
        return df

    def transform_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """Placeholder for transformations, to be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement this method.")
    
    def upload_file(self, file_path: str) -> dict:
        """Upload a single parquet file to ClickHouse with performance monitoring."""
        file_name = Path(file_path).name
        print(f"\n🚀 Processing: {file_name}")

        start_time = time.time()
        total_uploaded = 0
        batch_count = 0
        total_processed = 0

        try:
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            print(f"  📁 File size: {file_size_mb:.1f} MB")

            for batch in tqdm(self.process_parquet_in_batches(file_path), desc=f"Uploading {file_name}", unit="batch"):
                total_processed += len(batch)
                
                transformed_batch = self.transform_batch(batch)
                
                if len(transformed_batch) > 0:
                    self.client.insert_df(table=self.table_name, df=transformed_batch)
                    total_uploaded += len(transformed_batch)
                    batch_count += 1
                
                del batch, transformed_batch
                gc.collect()

            end_time = time.time()
            elapsed_time = end_time - start_time
            
            return {
                'file': file_name,
                'file_size_mb': file_size_mb,
                'rows_processed': total_processed,
                'rows_uploaded': total_uploaded,
                'rows_filtered': total_processed - total_uploaded,
                'batches_processed': batch_count,
                'time_seconds': elapsed_time,
                'rows_per_second': total_uploaded / elapsed_time if elapsed_time > 0 else 0,
                'mb_per_second': file_size_mb / elapsed_time if elapsed_time > 0 else 0
            }
        except Exception as e:
            print(f"❌ Error processing {file_name}: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'file': file_name,
                'error': str(e),
                'rows_uploaded': total_uploaded,
                'time_seconds': time.time() - start_time
            }
            
    def upload_all_files(self, data_path: str, file_pattern: str, specific_files: list = None) -> None:
        """Upload all files matching a pattern from the specified directory."""
        if specific_files:
            print(f"🔍 Uploading specific files as requested.")
            all_files_in_dir = {Path(p).name: p for p in glob.glob(f"{data_path}/{file_pattern}")}
            parquet_files = []
            for f in specific_files:
                if f in all_files_in_dir:
                    parquet_files.append(all_files_in_dir[f])
                else:
                    print(f"❌ Warning: File '{f}' not found in the data directory. Skipping.")
        else:
            print(f"🔍 Found files matching pattern: {file_pattern}")
            parquet_files = sorted(glob.glob(f"{data_path}/{file_pattern}"))
        
        if not parquet_files:
            print(f"❌ No files found in {data_path} matching pattern: {file_pattern}")
            return
            
        print(f"📊 Found {len(parquet_files)} files to upload.")
        total_size_mb = sum(os.path.getsize(file) for file in parquet_files) / (1024 * 1024)
        print(f"📊 Total data size: {total_size_mb:.1f} MB")
        
        try:
            self.client.query(f"SELECT 1 FROM {self.table_name} LIMIT 1")
            print("✅ ClickHouse connection verified, table exists.")
        except Exception as e:
            print(f"❌ Table verification failed for '{self.table_name}': {str(e)}")
            print("💡 Make sure you've run the CREATE TABLE script first!")
            return
            
        total_start_time = time.time()
        results = [self.upload_file(file_path) for file_path in parquet_files]
        total_end_time = time.time()
        total_elapsed = total_end_time - total_start_time
        
        self.display_summary(results, total_size_mb, total_elapsed)

    def display_summary(self, results: list, total_size_mb: float, total_elapsed: float) -> None:
        """Displays a summary of the upload process."""
        total_rows_uploaded = sum(r['rows_uploaded'] for r in results if 'error' not in r)
        total_rows_processed = sum(r['rows_processed'] for r in results if 'error' not in r)
        
        successful_files = [r for r in results if 'error' not in r]
        failed_files = [r for r in results if 'error' in r]
        
        print(f"\n{'='*60}")
        print(f"🎉 UPLOAD COMPLETE FOR TABLE '{self.table_name}'!")
        print(f"{'='*60}")
        print(f"📈 Summary:")
        print(f"  ✅ Successful files: {len(successful_files)}")
        print(f"  ❌ Failed files: {len(failed_files)}")
        print(f"  📊 Total rows processed: {total_rows_processed:,}")
        print(f"  ⬆️  Total rows uploaded: {total_rows_uploaded:,}")
        print(f"  🗑️  Total rows filtered: {total_rows_processed - total_rows_uploaded:,}")
        print(f"  📁 Total data processed: {total_size_mb:.1f} MB")
        print(f"  🕒 Total time: {total_elapsed:.1f} seconds ({total_elapsed/60:.1f} minutes)")
        print(f"  ⚡ Overall speed: {total_rows_uploaded/total_elapsed:,.0f} rows/sec")
        print(f"  💾 Throughput: {total_size_mb/total_elapsed:.1f} MB/sec")
        
        try:
            row_count = self.client.query(f"SELECT COUNT(*) FROM {self.table_name}").result_rows[0][0]
            print(f"  🔍 Verified in ClickHouse: {row_count:,} rows in {self.table_name}")
            stats = self.client.query(f"SELECT MIN(pickup_date), MAX(pickup_date), COUNT(DISTINCT pickup_date) FROM {self.table_name}").result_rows[0]
            print(f"  📅 Date range: {stats[0]} to {stats[1]} ({stats[2]} unique days)")
        except Exception as e:
            print(f"  ❌ Could not verify data in ClickHouse: {str(e)}")

# =================================================================================
# Specific Uploader Classes for Yellow and Green Taxi Data
# =================================================================================

class YellowTaxiUploader(TaxiDataUploader):
    def __init__(self, connection_string: str, batch_size: int = 50000):
        super().__init__(connection_string, "yellow_taxi_trips", batch_size)

    def transform_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformations and filtering to a batch of yellow taxi data."""
        df = df.copy()

        # Handle inconsistent column casing and rename to a consistent format
        column_mappings = {
            'tpep_pickup_datetime': 'tpep_pickup_datetime',
            'tpep_dropoff_datetime': 'tpep_dropoff_datetime',
            'Airport_fee': 'Airport_fee',
            'airport_fee': 'Airport_fee',
            'Congestion_Surcharge': 'congestion_surcharge',
            'congestion_surcharge': 'congestion_surcharge',
            'RatecodeID': 'RatecodeID',
            'Ratecodeid': 'RatecodeID',
            'store_and_fwd_flag': 'store_and_fwd_flag'
        }
        
        # Rename columns to a consistent casing
        df.columns = [column_mappings.get(col, col) for col in df.columns]

        # Fix for FutureWarning: Fill NaNs *before* mapping to prevent downcasting issues.
        df['store_and_fwd_flag'] = df['store_and_fwd_flag'].fillna('N').map({'Y': True, 'N': False})
        
        # Fix for the new FutureWarning: Convert column to numeric before filling NaNs.
        if 'Airport_fee' in df.columns:
            df['Airport_fee'] = pd.to_numeric(df['Airport_fee'], errors='coerce').fillna(0)

        # Convert to appropriate dtypes for ClickHouse
        df['passenger_count'] = df['passenger_count'].fillna(1).astype('uint8')
        df['RatecodeID'] = df['RatecodeID'].fillna(1).astype('uint8')
        df['congestion_surcharge'] = df['congestion_surcharge'].fillna(0)
        df['PULocationID'] = df['PULocationID'].astype('uint16')
        df['DOLocationID'] = df['DOLocationID'].astype('uint16')
        df['payment_type'] = df['payment_type'].astype('uint8')
        
        # Optimize Float64 to Float32
        float_cols = ['trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
                      'tolls_amount', 'improvement_surcharge', 'total_amount', 
                      'congestion_surcharge', 'Airport_fee']
        for col in float_cols:
            if col in df.columns:
                df[col] = df[col].astype('float32')

        # Add derived columns
        df['trip_duration_minutes'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
        df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour.astype('uint8')
        df['pickup_day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek.astype('uint8')
        df['pickup_date'] = df['tpep_pickup_datetime'].dt.date
        
        df['tip_percentage'] = np.where(df['fare_amount'] > 0, (df['tip_amount'] / df['fare_amount'] * 100), 0.0).clip(0, 100).astype('float32')
        df['avg_speed_mph'] = np.where(df['trip_duration_minutes'] > 0, (df['trip_distance'] / (df['trip_duration_minutes'] / 60)), 0.0).clip(0, 100).astype('float32')
        
        # Less strict data quality filters and date validation
        initial_rows = len(df)
        df = df[
            (df['tpep_pickup_datetime'].dt.date >= pd.to_datetime('2020-01-01').date()) &
            (df['tpep_pickup_datetime'].dt.date <= date.today()) &
            (df['trip_distance'] > 0) &
            (df['trip_distance'] < 500) &
            (df['trip_duration_minutes'] > 0) &
            (df['trip_duration_minutes'] <= 1440) &
            (df['fare_amount'] >= 0) &
            (df['total_amount'] >= 0)
        ].copy()
        
        print(f"  📊 Filtered out {initial_rows - len(df):,} rows with data quality issues.")
        return df
        
class GreenTaxiUploader(TaxiDataUploader):
    def __init__(self, connection_string: str, batch_size: int = 50000):
        super().__init__(connection_string, "green_taxi_trips", batch_size)

    def transform_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformations and filtering to a batch of green taxi data."""
        df = df.copy()

        # Handle inconsistent column casing and rename to a consistent format
        column_mappings = {
            'lpep_pickup_datetime': 'lpep_pickup_datetime',
            'lpep_dropoff_datetime': 'lpep_dropoff_datetime',
            'congestion_surcharge': 'congestion_surcharge',
            'RatecodeID': 'RatecodeID',
            'Ratecodeid': 'RatecodeID',
            'store_and_fwd_flag': 'store_and_fwd_flag'
        }
        df.columns = [column_mappings.get(col, col) for col in df.columns]

        # Drop columns not in the target schema
        columns_to_drop = ['ehail_fee']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')
        
        # Fix for FutureWarning: Fill NaNs *before* mapping
        df['store_and_fwd_flag'] = df['store_and_fwd_flag'].fillna('N').map({'Y': True, 'N': False})

        # Convert to appropriate dtypes for ClickHouse
        df['passenger_count'] = df['passenger_count'].fillna(1).astype('uint8')
        df['RatecodeID'] = df['RatecodeID'].fillna(1).astype('uint8')
        df['congestion_surcharge'] = df['congestion_surcharge'].fillna(0)
        df['PULocationID'] = df['PULocationID'].astype('uint16')
        df['DOLocationID'] = df['DOLocationID'].astype('uint16')
        df['payment_type'] = df['payment_type'].fillna(0).astype('uint8')

        # Optimize Float64 to Float32
        float_cols = ['trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
                      'tolls_amount', 'improvement_surcharge', 'total_amount',
                      'congestion_surcharge']
        for col in float_cols:
            if col in df.columns:
                df[col] = df[col].astype('float32')
        
        # Add derived columns
        df['trip_duration_minutes'] = (df['lpep_dropoff_datetime'] - df['lpep_pickup_datetime']).dt.total_seconds() / 60
        df['pickup_hour'] = df['lpep_pickup_datetime'].dt.hour.astype('uint8')
        df['pickup_day_of_week'] = df['lpep_pickup_datetime'].dt.dayofweek.astype('uint8')
        df['pickup_date'] = df['lpep_pickup_datetime'].dt.date
        
        df['tip_percentage'] = np.where(df['fare_amount'] > 0, (df['tip_amount'] / df['fare_amount'] * 100), 0.0).clip(0, 100).astype('float32')
        df['avg_speed_mph'] = np.where(df['trip_duration_minutes'] > 0, (df['trip_distance'] / (df['trip_duration_minutes'] / 60)), 0.0).clip(0, 100).astype('float32')
        
        # Less strict data quality filters and date validation
        initial_rows = len(df)
        df = df[
            (df['lpep_pickup_datetime'].dt.date >= pd.to_datetime('2020-01-01').date()) &
            (df['lpep_pickup_datetime'].dt.date <= date.today()) &
            (df['trip_distance'] > 0) &
            (df['trip_distance'] < 500) &
            (df['trip_duration_minutes'] > 0) &
            (df['trip_duration_minutes'] <= 1440) &
            (df['fare_amount'] >= 0) &
            (df['total_amount'] >= 0)
        ].copy()
        
        print(f"  📊 Filtered out {initial_rows - len(df):,} rows with data quality issues.")
        return df


# =================================================================================
# Main Execution
# =================================================================================

if __name__ == "__main__":
    # Configure your ClickHouse connection string and data path here
    CONNECTION_STRING = os.getenv("CLICKHOUSE_CONNECTION_STRING", "clickhouse://default:@localhost:8123/default")
    DATA_PATH = r"C:\USERS\VLAD\TAXI-ANALYTICS\DATA"
    
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Upload NYC taxi data to ClickHouse.")
    parser.add_argument(
        '--taxi_type', 
        choices=['yellow', 'green', 'all'], 
        default='all',
        help='Specify which taxi data to upload (yellow, green, or all). Defaults to all.'
    )
    parser.add_argument(
        '--files',
        type=str,
        help='Comma-separated list of specific parquet filenames to upload (e.g., "yellow_tripdata_2020-01.parquet,yellow_tripdata_2020-02.parquet").'
    )
    args = parser.parse_args()
    
    print("🚕 NYC Taxi Data Uploader")
    print("=" * 50)
    print(f"Data path: {DATA_PATH}")
    print(f"Connection: {CONNECTION_STRING.split('@')[1] if '@' in CONNECTION_STRING else 'Not configured'}")
    print(f"Processing taxi type: {args.taxi_type}")
    print()
    
    if "your_username" in CONNECTION_STRING or "your_host" in CONNECTION_STRING:
        print("❌ Please configure your ClickHouse connection string first!")
        print("Update the CONNECTION_STRING variable in the script.")
        exit(1)
        
    try:
        # Determine which taxi type(s) to process
        if args.taxi_type == 'all':
            taxi_types = ["yellow", "green"]
        else:
            taxi_types = [args.taxi_type]
            
        # Parse specified files if the argument is provided
        specific_files_list = [f.strip() for f in args.files.split(',')] if args.files else None
            
        # Create and run uploaders for the selected taxi type(s)
        for taxi_type in taxi_types:
            if taxi_type == "yellow":
                uploader = YellowTaxiUploader(CONNECTION_STRING)
                uploader.upload_all_files(DATA_PATH, "yellow_tripdata_*.parquet", specific_files_list)
            elif taxi_type == "green":
                uploader = GreenTaxiUploader(CONNECTION_STRING)
                uploader.upload_all_files(DATA_PATH, "green_tripdata_*.parquet", specific_files_list)
            
            print("\n" + "="*60)
            print(f"Completed processing for {taxi_type.upper()} taxi data.")
            print("="*60)
            
    except Exception as e:
        print(f"❌ Script failed: {str(e)}")
        print("\n💡 Troubleshooting tips:")
        print("1. Verify your ClickHouse connection string.")
        print("2. Make sure the `yellow_taxi_trips` and `green_taxi_trips` tables exist.")
        print("3. Check that parquet files exist in the specified directory.")
        print("4. Ensure you have sufficient memory and disk space.")