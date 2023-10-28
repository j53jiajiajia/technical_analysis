import sqlite3
import pandas as pd

DB_NAME = 'db.sqlite3'

def get_historical_data_from_db(end_time):
    """Fetch historical data for all symbols from the SQLite database up to a specific end time."""
    conn = sqlite3.connect(DB_NAME)
    query = f"SELECT * FROM SPY_stock_data WHERE Date <= '{end_time}'"
    df = pd.read_sql(query, conn)
    # Convert the 'Date' column to datetime format
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    conn.close()
    return df

def create_table():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Create the technical_analysis_score table if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS technical_analysis_score (
        id INTEGER PRIMARY KEY,
        Symbol TEXT NOT NULL,
        technical_analysis_score INTEGER NOT NULL,
        Rank INTEGER NOT NULL,
        Analysis TEXT NOT NULL,
        Timestamp TEXT,
        UNIQUE(Symbol, Timestamp)  -- This ensures that combinations of Symbol and Timestamp are unique
    )
    ''')

    conn.commit()
    conn.close()

def save_to_sqlite(Symbol, score, rank, analysis, end_date):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Insert the data or do nothing if Timestamp and Symbol already exist
    cursor.execute('''
    INSERT OR IGNORE INTO technical_analysis_score 
    (Symbol, technical_analysis_score, Rank, Analysis, Timestamp)
    VALUES (?, ?, ?, ?, ?)
    ''', (Symbol, score, rank, analysis, end_date))

    conn.commit()
    conn.close()

