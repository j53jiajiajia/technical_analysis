import sqlite3
import pandas as pd
from train_backtest import optimize_parameters


def calculate_fees(num_shares, price, sell=False):
    # eg.tiger website
    total_value = num_shares * price
    commission = max(0.0039 * num_shares, 0.99)
    platform_fee = max(0.004 * num_shares, 1)
    external_institution_fee = max(0.00396 * num_shares, 0.99)

    total_fees = commission + platform_fee + external_institution_fee
    return total_fees

def test_technical_analysis():
    # m, n = optimize_parameters()
    m, n = 2, -20
    create_backtest_table()

    # Connect to the database
    conn = sqlite3.connect('db.sqlite3')

    # Fetch all data
    technical_scores = pd.read_sql(
        'SELECT * FROM technical_analysis_score WHERE Timestamp >= "2023-05-01" and Timestamp <= "2023-10-16"', conn)
    stock_data = pd.read_sql('SELECT * FROM SPY_stock_data', conn)

    # Merge dataframes based on Symbol and Date/Timestamp
    merged = pd.merge(stock_data, technical_scores, left_on=['Date', 'Symbol'], right_on=['Timestamp', 'Symbol'])

    # Variables for backtesting
    total_investment = 0
    sold_earning = 0

    previous_net_earning = 0
    positions = {}  # Dictionary to track our investments

    backtest_data = []
    # print(merged)
    for date, day_data in merged.groupby('Date'):
        eligible_stocks = day_data[day_data['technical_analysis_score'] >= m]
        num_stocks = len(eligible_stocks)

        # Buy stocks
        if num_stocks > 0:
            for index, stock in eligible_stocks.iterrows():

                amount_to_invest = 500000 / num_stocks
                num_shares = amount_to_invest / stock['Close']
                # print(num_shares)
                if stock['Symbol'] not in positions:
                    positions[stock['Symbol']] = num_shares
                else:
                    positions[stock['Symbol']] += num_shares

                # Update total investment
                total_investment += amount_to_invest + calculate_fees(num_shares, stock['Close'])
            print(f'{date}总投资{total_investment}')
        else:
            print(f'{date}无投资')

        # print(positions)

        for index, stock in day_data[day_data['technical_analysis_score'] <= n].iterrows():
            if stock['Symbol'] in positions:
                earned_from_sale = positions[stock['Symbol']] * stock['Close']
                sold_earning += earned_from_sale - calculate_fees(positions[stock['Symbol']], stock['Close'], sell=True)
                del positions[stock['Symbol']]
        print(f'{date}累计卖出股票的收益{sold_earning}')

        holding_earning = sum([stock['Close'] * positions.get(stock['Symbol'], 0) for index, stock in day_data.iterrows()])
        print(f'{date}现在所持有股票的收益{holding_earning}')

        # Update total earning
        total_earning = sold_earning + holding_earning
        print(f'{date}总收益{total_earning}')

        net_earning = total_earning - total_investment
        return_rate = net_earning / total_earning * 100
        daily_net_earning = net_earning - previous_net_earning
        previous_net_earning = net_earning

        # Store results
        backtest_data.append((date, ",".join(eligible_stocks['Symbol'].tolist()), total_investment, total_earning,
                              net_earning, return_rate, daily_net_earning))


    backtest_df = pd.DataFrame(backtest_data,
                               columns=['Timestamp', 'stocks_symbols_invested', 'total_investment', 'total_earning',
                                        'net_earning', 'return_rate', 'daily_net_earning'])

    # Save results to SQLite
    backtest_df.to_sql('test_technical_score', conn, if_exists='replace', index=False)

    # Close the connection
    conn.close()

def create_backtest_table():
    # Connect to the database
    conn = sqlite3.connect('db.sqlite3')
    cursor = conn.cursor()

    # Create the backtest_technical_score table only if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS test_technical_score (
        Timestamp DATE PRIMARY KEY,
        stocks_symbols_invested TEXT,
        total_investment REAL,
        total_earning REAL,
        net_earning REAL,
        return_rate REAL,
        daily_net_earning REAL
    )
    ''')

    # Commit and close the connection
    conn.commit()
    conn.close()

test_technical_analysis()