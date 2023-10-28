import yfinance as yf
import sqlite3
import pandas as pd

def download_spy_stocks():
    conn = sqlite3.connect('db.sqlite3')
    # conn = sqlite3.connect('tmp/db.sqlite3')
    cursor = conn.cursor()
    cursor.execute("SELECT symbol, company_name FROM spy_holdings_symbols")
    symbols_with_names = cursor.fetchall()
    symbol_list = [(symbol, company_name) for symbol, company_name in symbols_with_names]
    table_name = "SPY_stock_data"

    # Create the table
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Date TEXT,
            Market TEXT,
            Symbol TEXT,
            Company_name TEXT,
            Open FLOAT,
            High FLOAT,
            Low FLOAT,
            Close FLOAT,
            Volume BIGINT,
            Market_Cap FLOAT,  -- Add a column for Market Cap
            Turnover_Rate FLOAT,  -- Add a column for Turnover Rate
            PRIMARY KEY (Date, Symbol)  -- Use Date and Symbol as the primary key
        )
    """
    conn.execute(create_table_query)

    # Iterate through the list of stock symbols and update the table
    for symbol, company_name in symbol_list:
        try:
            cursor.execute(f"SELECT MAX(Date) FROM {table_name} WHERE Symbol = ?", (symbol,))
            max_date = cursor.fetchone()[0]
            if max_date is not None:
                start_date = (pd.Timestamp(max_date) + pd.DateOffset(1)).strftime('%Y-%m-%d')
                # start_date = (pd.Timestamp.now() - pd.DateOffset(1)).strftime('%Y-%m-%d')
            else:
                start_date = (pd.Timestamp.now() - pd.DateOffset(730)).strftime('%Y-%m-%d')
            end_date = pd.Timestamp.now().strftime('%Y-%m-%d')
            stock_data = yf.download(symbol, start=start_date, end=end_date, interval="1d")

            if not stock_data.empty:
                stock_data.reset_index(inplace=True)
                stock_data["Date"] = stock_data["Date"].dt.strftime('%Y-%m-%d')
                stock_data["Market"] = "US"
                stock_data["Symbol"] = symbol
                stock_data["Company_name"] = company_name  # Add Company_name column
                stock_data = stock_data[
                    ["Date", "Market", "Symbol", "Company_name", "Open", "High", "Low", "Close", "Volume"]]

                if 'marketCap' in yf.Ticker(symbol).info:
                    market_cap = yf.Ticker(symbol).info['marketCap']
                else:
                    market_cap = None  # Set marketCap to None if data is not available
                stock_data['Market_Cap'] = market_cap
                if 'floatShares' in yf.Ticker(symbol).info:
                    stock_data['Turnover_Rate'] = 100 * stock_data["Volume"] / yf.Ticker(symbol).info['floatShares']
                else:
                    stock_data['Turnover_Rate'] = None
                stock_data.to_sql(table_name, conn, if_exists="append", index=False)
        except Exception as e:
            print(f"Error downloading data for {symbol}: {str(e)}")
            pass
    # Close the database connection
    conn.close()

# download_spy_stocks()
