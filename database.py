import os
import time
import pyodbc
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def clean_news_dataframe(news_df):
    """Clean the news DataFrame: ensure all fields are string or None, and drop duplicate Titles."""
    # Clean fields
    for col in ['Title', 'News_Hyperlinks', 'Published_Date', 'Sector', 'Extracted_Entities', 'Related_Stock', 'Img','Body']:
        if col in news_df.columns:
            news_df[col] = news_df[col].apply(lambda x: str(x).strip() if pd.notna(x) else None)
    
    # Drop duplicates based on Title (keeping the first occurrence)
    news_df = news_df.drop_duplicates(subset='Title', keep='first').reset_index(drop=True)

    return news_df



# Validate required env vars
def validate_env():
    required_vars = ['DB_SERVER', 'DB_NAME', 'DB_USERNAME', 'DB_PASSWORD']
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise EnvironmentError(f"Missing environment variables: {', '.join(missing)}")

# Connect to Azure SQL with retry logic
def connect_to_azure_sql(max_retries=5, delay_seconds=10):
    validate_env()
    attempt = 0
    while attempt < max_retries:
        try:
            connection = pyodbc.connect(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={os.getenv('DB_SERVER')};"
                f"DATABASE={os.getenv('DB_NAME')};"
                f"UID={os.getenv('DB_USERNAME')};"
                f"PWD={os.getenv('DB_PASSWORD')};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=60;"  # 60s enough for serverless wake
            )
            print(f"[INFO] Successfully connected to Azure SQL Database on attempt {attempt + 1}.")
            return connection
        except Exception as e:
            print(f"[WARNING] Attempt {attempt + 1} failed: {e}")
            attempt += 1
            if attempt < max_retries:
                print(f"[INFO] Retrying in {delay_seconds} seconds...")
                time.sleep(delay_seconds)
            else:
                print("[ERROR] All connection attempts failed.")
                return None

# Read entire table into DataFrame
def read_sql(table_name):
    try:
        conn = connect_to_azure_sql()
        if conn:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        print(f"[ERROR] Error reading SQL data: {e}")
        return pd.DataFrame()

# Insert DataFrame into SQL table
def insert_news(news_df, news_table):
    try:
        conn = connect_to_azure_sql()
        if not conn:
            print("[ERROR] DB connection failed for batch insert.")
            return

        # 💥 CLEAN data before inserting
        news_df = clean_news_dataframe(news_df)

        cursor = conn.cursor()
        insert_query = f"""
            INSERT INTO {news_table}
            (Title, News_Hyperlinks, Published_Date, Related_Stock, Img,Body)
            VALUES (?, ?, ?, ?, ?, ?, ?,?)
        """

        records = [
            (
                row['Title'],
                row['News_Hyperlinks'],
                row['Published_Date'],
                row['Related_Stock'],
                row['Img'],
                row['Body']
            )
            for _, row in news_df.iterrows()
        ]

        cursor.executemany(insert_query, records)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"[INFO] Inserted batch of {len(records)} rows.")
    except Exception as e:
        print(f"[ERROR] Error inserting batch: {e}")


# Get the most recent news
def extract_last_news(news_table):
    try:
        conn = connect_to_azure_sql()
        if conn:
            query = f"SELECT TOP 1 * FROM {news_table} ORDER BY Published_Date DESC"
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        print(f"[ERROR] Error extracting last news: {e}")
        return pd.DataFrame()

# Manual test connection
if __name__ == "__main__":
    try:
        conn = connect_to_azure_sql()
        if conn:
            print("[INFO] Test connection successful.")
            conn.close()
    except Exception as e:
        print(f"[ERROR] Test connection failed: {e}")

