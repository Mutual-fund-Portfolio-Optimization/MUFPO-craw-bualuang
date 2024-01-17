from src.scrape_daily_nav import extract_data, fund_url
from src.cookie_collector import cookie
from config import BASE_URL
import numpy as np
import pandas as pd


def main():
    cookie_string = cookie(BASE_URL)
    dataset = extract_data(fund_url(BASE_URL, cookie_string), cookie_string)
    print('Saving Output...')
    pd.DataFrame(np.array(dataset)[:, :-1].tolist(), columns=['date', 'fund_name', 'nav/unit', 'selling_price', 'redemption_price', 'unknow1', 'nav', 'unknow2'])\
        .to_csv('out/result.csv')
    print('Success!')

main()