"""
Airflow DAG: HDFS to BigQuery Daily Sync
Đọc dữ liệu Parquet từ HDFS và upload lên BigQuery
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import pandas as pd
import pyarrow.parquet as pq
import requests
import os
import logging

# ============================================
# CONFIGURATION
# ============================================
GCP_PROJECT_ID = "magnetic-clone-478711-i6"
BQ_DATASET_ID = "olap_project"
BQ_TABLE_NAME = "processed_transactions"
GCP_CREDENTIALS_PATH = "/opt/airflow/config/gcp-service-account.json"

# HDFS Configuration
HDFS_NAMENODE = "http://namenode:9870"
HDFS_BASE_PATH = "/credit-card/processed/valid"

# Logging
logger = logging.getLogger(__name__)

# ============================================
# BIGQUERY SCHEMA DEFINITION
# ============================================
BIGQUERY_SCHEMA = [
    bigquery.SchemaField("DateTime_Hour_Key", "STRING"),
    bigquery.SchemaField("User", "STRING"),
    bigquery.SchemaField("Card", "STRING"),
    bigquery.SchemaField("Year", "INTEGER"),
    bigquery.SchemaField("Month", "INTEGER"),
    bigquery.SchemaField("Day", "INTEGER"),
    bigquery.SchemaField("Hour", "INTEGER"),
    bigquery.SchemaField("Day_of_Week", "STRING"),
    bigquery.SchemaField("Is_Weekend", "STRING"),
    bigquery.SchemaField("Amount_USD", "FLOAT"),
    bigquery.SchemaField("Amount_VND", "FLOAT"),
    bigquery.SchemaField("Exchange_Rate", "INTEGER"),
    bigquery.SchemaField("Use_Chip", "STRING"),
    bigquery.SchemaField("Merchant_Name", "STRING"),
    bigquery.SchemaField("Merchant_City", "STRING"),
    bigquery.SchemaField("Merchant_State", "STRING"),
    bigquery.SchemaField("Zip", "STRING"),
    bigquery.SchemaField("MCC", "STRING"),
    bigquery.SchemaField("Errors", "STRING"),
    bigquery.SchemaField("Is_Fraud", "STRING"),
    bigquery.SchemaField("Processed_Timestamp", "STRING"),
]


def list_hdfs_files(hdfs_path: str) -> list:
    """
    List all Parquet files in HDFS directory using WebHDFS REST API
    """
    url = f"{HDFS_NAMENODE}/webhdfs/v1{hdfs_path}?op=LISTSTATUS"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        files = []
        file_statuses = response.json().get("FileStatuses", {}).get("FileStatus", [])
        
        for file_status in file_statuses:
            file_name = file_status.get("pathSuffix", "")
            file_type = file_status.get("type", "")
            
            if file_type == "DIRECTORY":
                # Recursively list subdirectories
                sub_files = list_hdfs_files(f"{hdfs_path}/{file_name}")
                files.extend(sub_files)
            elif file_name.endswith(".parquet"):
                files.append(f"{hdfs_path}/{file_name}")
        
        return files
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error listing HDFS files: {e}")
        return []


def extract_partition_values(hdfs_path: str) -> dict:
    """
    Extract Year, Month, Day from partition path
    Example path: /credit-card/processed/valid/Year=2024/Month=1/Day=15/part-00000.parquet
    """
    import re
    
    partition_values = {}
    
    # Extract Year
    year_match = re.search(r'Year=(\d+)', hdfs_path)
    if year_match:
        partition_values['Year'] = int(year_match.group(1))
    
    # Extract Month
    month_match = re.search(r'Month=(\d+)', hdfs_path)
    if month_match:
        partition_values['Month'] = int(month_match.group(1))
    
    # Extract Day
    day_match = re.search(r'Day=(\d+)', hdfs_path)
    if day_match:
        partition_values['Day'] = int(day_match.group(1))
    
    return partition_values


def read_parquet_from_hdfs(hdfs_path: str) -> pd.DataFrame:
    """
    Read a Parquet file from HDFS using WebHDFS
    Also extracts partition columns (Year, Month, Day) from the path
    """
    url = f"{HDFS_NAMENODE}/webhdfs/v1{hdfs_path}?op=OPEN"
    
    try:
        response = requests.get(url, stream=True, timeout=60, allow_redirects=True)
        response.raise_for_status()
        
        # Save to temp file and read with pyarrow
        temp_file = f"/tmp/{os.path.basename(hdfs_path)}"
        with open(temp_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        df = pq.read_table(temp_file).to_pandas()
        os.remove(temp_file)
        
        # ✅ Extract partition values from path and add to DataFrame
        partition_values = extract_partition_values(hdfs_path)
        for col, value in partition_values.items():
            df[col] = value
        
        logger.info(f"📁 Added partition columns: {partition_values}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading Parquet from HDFS: {e}")
        return pd.DataFrame()


def get_hdfs_partition_path(execution_date: datetime) -> str:
    """
    Build HDFS partition path based on execution date
    Format: /credit-card/processed/valid/year=YYYY/month=M/day=D/
    """
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day
    
    return f"{HDFS_BASE_PATH}/Year={year}/Month={month}/Day={day}"


def read_hdfs_data(**context):
    """
    Task 1: Read all Parquet files from HDFS for the current date
    """
    execution_date = context['execution_date']
    existent_execution_date = datetime.strptime(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
    
    # Get partition path for today
    partition_path = get_hdfs_partition_path(existent_execution_date)
    logger.info(f"📂 Reading from HDFS partition: {partition_path}")
    
    # List all Parquet files in the partition
    parquet_files = list_hdfs_files(partition_path)
    logger.info(f"📑 Found {len(parquet_files)} Parquet files")
    
    if not parquet_files:
        logger.warning(f"⚠️ No Parquet files found for date: {execution_date.date()}")
        return None
    
    # Read and concatenate all Parquet files
    all_dfs = []
    for file_path in parquet_files:
        df = read_parquet_from_hdfs(file_path)
        if not df.empty:
            all_dfs.append(df)
            logger.info(f"✅ Read {len(df)} records from {file_path}")
    
    if not all_dfs:
        logger.warning("⚠️ No data read from Parquet files")
        return None
    
    # Combine all DataFrames
    combined_df = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"📊 Total records to upload: {len(combined_df)}")
    
    # ✅ Reorder columns to match BigQuery schema
    expected_columns = [
        "DateTime_Hour_Key",
        "User",
        "Card",
        "Year",
        "Month",
        "Day",
        "Hour",
        "Day_of_Week",
        "Is_Weekend",
        "Amount_USD",
        "Amount_VND",
        "Exchange_Rate",
        "Use_Chip",
        "Merchant_Name",
        "Merchant_City",
        "Merchant_State",
        "Zip",
        "MCC",
        "Errors",
        "Is_Fraud",
        "Processed_Timestamp",
    ]
    
    # Reorder DataFrame columns
    combined_df = combined_df[expected_columns]
    logger.info(f"✅ Reordered columns to match BigQuery schema: {list(combined_df.columns)}")
    
    # Save to temp CSV for next task
    temp_path = "/tmp/hdfs_data_for_bigquery.csv"
    combined_df.to_csv(temp_path, index=False)
    
    return temp_path


def upload_to_bigquery(**context):
    """
    Task 2: Upload data to BigQuery
    """
    ti = context['ti']
    temp_path = ti.xcom_pull(task_ids='read_hdfs_data')
    
    if not temp_path or not os.path.exists(temp_path):
        logger.warning("⚠️ No data to upload")
        return
    
    # Set credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDENTIALS_PATH
    
    # Initialize BigQuery client
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    # Full table ID
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_NAME}"
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        schema=BIGQUERY_SCHEMA,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    # Load data
    logger.info(f"📤 Uploading to BigQuery: {table_id}")
    
    with open(temp_path, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            table_id,
            job_config=job_config,
        )
    
    # Wait for job to complete
    job.result()
    
    # Get result
    table = client.get_table(table_id)
    logger.info(f"✅ Upload complete! Total rows in table: {table.num_rows}")
    
    # Cleanup temp file
    os.remove(temp_path)
    
    return table.num_rows


# ============================================
# DAG DEFINITION
# ============================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='hdfs_to_bigquery_daily_sync',
    default_args=default_args,
    description='Sync processed transactions from HDFS to BigQuery daily',
    schedule_interval='0 23 * * *',  # Run at 11 PM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigquery', 'hdfs', 'etl'],
) as dag:
    
    # Task 1: Read data from HDFS
    read_hdfs_task = PythonOperator(
        task_id='read_hdfs_data',
        python_callable=read_hdfs_data,
        provide_context=True,
    )
    
    # Task 2: Upload to BigQuery
    upload_bq_task = PythonOperator(
        task_id='upload_to_bigquery',
        python_callable=upload_to_bigquery,
        provide_context=True,
    )
    
    # Define task dependencies
    read_hdfs_task >> upload_bq_task
