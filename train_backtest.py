import sqlite3
import pandas as pd

# def calculate_fees(num_shares, price, sell=False):
#     total_value = num_shares * price
#     commission = max(min(total_value * 0.005, 0.0039 * num_shares), 0.99)
#     platform_fee = max(min(total_value * 0.005, 0.004 * num_shares), 1)
#     external_institution_fee = max(min(total_value * 0.01, 0.00396 * num_shares), 0.99)

#     sec_fee = 0.000008 * total_value if sell else 0
#     sec_fee = max(sec_fee, 0.01)

#     total_fees = commission + platform_fee + external_institution_fee + sec_fee
#     return total_fees


def calculate_fees(num_shares, price, sell=False):
    # eg.tiger website
    total_value = num_shares * price
    commission = max(0.0039 * num_shares, 0.99)
    platform_fee = max(0.004 * num_shares, 1)
    external_institution_fee = max(0.00396 * num_shares, 0.99)

    total_fees = commission + platform_fee + external_institution_fee
    return total_fees

def train_technical_analysis(m, n):
    conn = sqlite3.connect('db.sqlite3')

    # Fetch only data for the given timestamps
    technical_scores = pd.read_sql(
        'SELECT * FROM technical_analysis_score WHERE Timestamp >= "2022-01-01" and Timestamp <= "2023-04-30"', conn)
    stock_data = pd.read_sql('SELECT * FROM SPY_stock_data WHERE Date >= "2022-01-01" and Date <= "2023-04-30"', conn)

    merged = pd.merge(stock_data, technical_scores, left_on=['Date', 'Symbol'], right_on=['Timestamp', 'Symbol'])

    total_investment = 0
    sold_earning = 0
    positions = {}
    net_earning = 0
    previous_fees = 0

    for date, day_data in merged.groupby('Date'):
        eligible_stocks = day_data[day_data['technical_analysis_score'] >= m]
        num_stocks = len(eligible_stocks)
        if num_stocks > 0:
            for index, stock in eligible_stocks.iterrows():
                amount_to_invest = 500000 / num_stocks
                num_shares = amount_to_invest / stock['Close']
                if stock['Symbol'] not in positions:
                    positions[stock['Symbol']] = num_shares
                else:
                    positions[stock['Symbol']] += num_shares
                total_investment += amount_to_invest + calculate_fees(num_shares, stock['Close'])
                

        for index, stock in day_data[day_data['technical_analysis_score'] <= n].iterrows():
            if stock['Symbol'] in positions:
                earned_from_sale = positions[stock['Symbol']] * stock['Close']
                # print(calculate_fees(positions[stock['Symbol']], stock['Close'], sell=True))
                sold_earning += earned_from_sale - calculate_fees(positions[stock['Symbol']], stock['Close'], sell=True)
                # sold_earning += earned_from_sale
                del positions[stock['Symbol']]
        
        # holding_earning = 0
        # fees = 0
        # for index, stock in day_data.iterrows():
        #     if stock['Symbol'] in positions:
        #         fees += calculate_fees(positions[stock['Symbol']], stock['Close'], sell=True)
        #         holding_earning += stock['Close'] * positions.get(stock['Symbol'], 0)
        # holding_earning = holding_earning - fees + previous_fees
        # previous_fees = fees

        holding_earning = sum(
            [stock['Close'] * positions.get(stock['Symbol'], 0) for index, stock in day_data.iterrows()])
        total_earning = sold_earning + holding_earning
        net_earning = total_earning - total_investment

    conn.close()

    # Return the net earning for these m, n values
    return net_earning


def optimize_parameters():
    best_m = None
    best_n = None
    best_net_earning = float('-inf')

    m_values = list(range(0, 21))
    n_values = list(range(-20, 1))

    for m in m_values:
        for n in n_values:
            if m > n:
                net_earning = train_technical_analysis(m, n)
                print(f"{m}, {n}: {net_earning}")
                if net_earning > best_net_earning:
                    best_net_earning = net_earning
                    best_m = m
                    best_n = n

    print(f"Best m: {best_m}, Best n: {best_n}, Best Net Earning: {best_net_earning}")
    return best_m, best_n


# optimize_parameters()

