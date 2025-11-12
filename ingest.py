import requests
import os
from bs4 import BeautifulSoup
import pandas as pd
from clickhouse_driver import Client
import logging as lgn
# from typing import Optional
from dotenv import load_dotenv
from pathlib import Path

log_dir = Path("logs")
log_file = log_dir / f"subscribers_records_ingestion.log"

lgn.basicConfig(level=lgn.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
        lgn.FileHandler(log_file, mode='a'),
        lgn.StreamHandler()
    ])

logger = lgn.getLogger(__name__)

# load env variables needed
load_dotenv()

google_sheet = os.getenv("GOOGLE_SHEET")
click_host = os.getenv("CLICK_HOST")
click_user = os.getenv("CLICK_USER")
click_password = os.getenv("CLICK_PASSWORD")
click_database = os.getenv("CLICK_DATABASE")
sub_table = os.getenv("SUBSCRIBERS_TABLE")
batch = 1000

def extract_to_df(sheet_url: str) -> pd.DataFrame:
    """Extract data from Google Sheet URL and convert to pandas DataFrame."""
    try:
        logger.info(f"Fetching data from: {sheet_url}")
        response = requests.get(sheet_url,timeout=30)
        response.raise_for_status()

        # parse HTML with beautifulsoup
        soup = BeautifulSoup(response.text, 'html.parser')
        logger.info("Successfully extracted HTML content")

        # find all tables in the html
        tables = soup.find_all('table')

        if not tables:
            logger.warning("No tables found inside the html content")
            return pd.DataFrame()
        
        # use the first table found in the html
        logger.info(f"Found {len(tables)} table(s). Will be using the first one.")

        # convert the html table to a dataframe
        df_consent = pd.read_html(str(tables[0]))[0]

        print(df_consent.head())

        logger.info(f"DataFrame created with shape: {df_consent.shape}")
        return df_consent
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to parse html to DataFrame: {e}")
        raise


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess the DataFrame."""
    try:
        logger.info("Cleaning Dataframe...")
        
        # remove empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')

        print(f"OK{df.iloc[0]}")
        
        # Remove Googlesheet row identifiers
        first_col = df.iloc[:, 0]
        if first_col.astype(str).str.match(r'^\d+').all():
            logger.info("Removing Googlesheet row identifiers from first column")
            df = df.iloc[:, 1:]

        
        # check if first row contains actual headers, if not use as headers
        if df.iloc[0].astype(str).str.match(r'^[A-Z]$').sum() < len(df.columns) / 2:
            if not df.iloc[0].isnull().all():
                df.columns = df.iloc[0]
                df = df.iloc[1:].reset_index(drop=True)
        
        # trim whitespaces from column names
        df.columns = df.columns.astype(str).str.strip()

        # remove rows that are all thesame value
        df = df.loc[~(df.astype(str).nunique(axis=1) == 1)]

        # reset index after cleaning
        df = df.reset_index(drop=True)
        print(df.head())

        logger.info(f"Cleaned Dataframe shape: {df.shape}")
        return df
    
    except Exception as e:
        logger.error(f"Failed to clean Dataframe: {e}")
        raise

def save_to_csv(df: pd.DataFrame, filepath: str, create_path: bool = True) -> None:
    """Save DataFrame to local CSV file."""
    try:
        # creates directory if it doesn't exist
        if create_path:
            directory = os.path.dirname(filepath)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
                logger.info(f"Created directory: {directory}")

        logger.info(f"Saving Dataframe to {filepath}")
        df.to_csv(filepath, index=False, encoding='utf-8')
        logger.info(f"Successfully saved to {filepath}")
    
    except Exception as e:
        logger.error(f"Failed to save to CSV: {e}")
        raise




def load_csv_to_clickhouse(csv_file_path: str, host: str, database: str, table: str, user: str = 'default',
                            password: str = '', batch_size: int = 1000, incremental: bool = False,
                            timestamp_column: str = None) -> None:
    """
    Load CSV file directly into ClickHouse database using cursor.
    Supports incremental loads based on timestamp column.
    """

    try:
        logger.info(f"Connecting to ClickHouse at {host}")
        client = Client(host=host, user=user, password=password, database=database)

        # Get max timestamp if incremental mode
        max_timestamp = None
        if incremental:
            if not timestamp_column:
                raise ValueError("timestamp_column must be specified when incremental=True")
            
            try:
                # Checks if table exists
                result = client.execute(f"EXISTS TABLE {database}.{table}")
                if result[0][0] == 1:
                    # Get max timestamp from the table
                    query = f"SELECT max(`{timestamp_column}`) FROM {database}.{table}"
                    result = client.execute(query)
                    max_timestamp = result[0][0]
                    
                    if max_timestamp:
                        logger.info(f"Last timestamp in database: {max_timestamp}")
                    else:
                        logger.info("Table is empty, will load all data")
                else:
                    logger.info(f"Table {database}.{table} doesn't exist yet, will load all data")
            except Exception as e:
                logger.warning(f"Could not get max timestamp: {e}. Will load all data.")
                max_timestamp = None

        logger.info(f"Reading csv file: {csv_file_path}")

        # Reading csv in chunks to manage memory when the records volume increases massively
        total_rows = 0
        skipped_rows = 0

        for chunk_num, chunk in enumerate(pd.read_csv(csv_file_path, chunksize=batch_size)):

            # Filter for incremental load
            if incremental and max_timestamp and timestamp_column in chunk.columns:
                # Convert timestamp column to datetime for comparison
                chunk[timestamp_column] = pd.to_datetime(chunk[timestamp_column])
                max_ts = pd.to_datetime(max_timestamp)
                
                # Only keep rows with timestamp > max_timestamp
                original_len = len(chunk)
                chunk = chunk[chunk[timestamp_column] > max_ts]
                skipped_rows += (original_len - len(chunk))
                
                if len(chunk) == 0:
                    logger.info(f"Batch {chunk_num + 1}: No new records to insert")
                    continue

            #prepare data as list of tuples
            data = [tuple(row) for row in chunk.values]

            if len(data) == 0:
                continue
            
            # creates columns names
            columns = ', '.join([f"`{col}`" for col in chunk.columns])
            
            # Insert in batch
            query = f"INSERT INTO {table} ({columns}) VALUES"
            client.execute(query, data)
            total_rows += len(data)
            logger.info(f"Batch {chunk_num + 1}: Inserted {len(data)} rows (Total: {total_rows})")
        
        if incremental:
            logger.info(f"Incremental load complete: {total_rows} new rows inserted, {skipped_rows} existing rows skipped")
        else:
            logger.info(f"Successfully loaded {total_rows} rows to ClickHouse")
        
        client.disconnect()
    
    except Exception as e:
        logger.error(f"Failed to load CSV to ClickHouse: {e}")
        raise



CSV_FILE_PATH = os.getenv('CSV_FILE_PATH','subscribers.csv')

extract_to_df(google_sheet)
df=extract_to_df(google_sheet)
clean_dataframe(df)
df_clean=clean_dataframe(df)
save_to_csv(df_clean, CSV_FILE_PATH)
load_csv_to_clickhouse(
    csv_file_path=CSV_FILE_PATH,
    host=click_host,
    database=click_database,
    table=sub_table,
    user=click_user,
    password=click_password
)