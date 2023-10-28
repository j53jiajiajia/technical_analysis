from download_spy_stocks import download_spy_stocks
from score_technical_analysis import calculate_total_score
from datetime import datetime

def print_time():
    now = datetime.now()
    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(formatted_time)

def lambda_handler():
    print_time()
    download_spy_stocks()
    calculate_total_score()
    print_time()


lambda_handler()