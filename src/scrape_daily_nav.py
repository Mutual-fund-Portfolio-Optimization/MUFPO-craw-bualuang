import requests
from bs4 import BeautifulSoup
from mufpo.etl import Pipe
from .datatypes import DailyNav, BaseRow
from functools import reduce
import datetime
from typing import Tuple, List

def get_funds_url(base_url:str, p_code: str, datetime_start: datetime.datetime, datetime_end: datetime.datetime) -> str:
    url = f'{base_url}?p_code={p_code}&date_from={datetime_start.day}' \
          f'&month_from={datetime_start.month}&year_from={datetime_start.year}' \
          f'&date_to={datetime_end.day}&month_to={datetime_end.month}&year_to={datetime_end.year}'
    return url

def map_funds_code_to_dict(p_code):
    container = {}
    container['p_code'] = p_code
    return container

def map_date_to_fund_inner(funds, static_date):
    funds['date_start'] = datetime.datetime(day=1, month=1, year=2010)
    funds['date_end'] = static_date
    return funds

def transfrom_to_tuple(funds):
    return tuple(funds.values())

def request_get(url, cookie_string):
    print(f"Request to {url}")
    return requests.get(url, headers={'Cookie': cookie_string}) 

def convert_to_soup(response):
    return BeautifulSoup(response.content, 'html.parser')

def map_date_to_fund(x):
    return map_date_to_fund_inner(x, datetime.datetime.now())

def process_fund_code(funds) -> str:
    return funds.replace('"code":', "")\
    .replace('"', '')\
    .replace('[{', '')\
    .replace('}]', '')\
    .replace('/', "%2F")\
    .replace('\\', '')\
    .replace(' ', '+')\
    .replace('(', '%28')\
    .replace(')', '%29')\
    .split(",")[0]


def clean_split(soup) -> List[str]:
    return str(soup).split('JSON.parse')[2]\
    .split(';\n')[0]\
    .replace('\'','')\
    .replace('([{', '')\
    .replace('}])', '')\
    .split('},{')


def fund_url(base_url, cookie_string):
    return (
        Pipe((base_url, cookie_string))
        >> request_get
        | convert_to_soup
        | clean_split
        | (lambda x: map(process_fund_code, x))
        | (lambda x: map(map_funds_code_to_dict, x))
        | (lambda x: map(map_date_to_fund, x))
        | (lambda x: map(transfrom_to_tuple, x))
        | (lambda x: map(lambda x: get_funds_url(base_url, *x), x))
        | list
    ).get()

def get_data_section(soup):
    try:
        return str(soup.find_all('script')[8]).split('\n')
    except Exception as error:
        print(error, 'Cannot split(\\n) function: get_data_section')
        raise error

def check_json(x):
    return 'JSON.parse' in x

def check_is_valid_data(x):
    return 'pf_date' in x

def replace_dict(x):
    return x.replace('{', '').replace('}', '')

def replace_quote(x):
    return x.replace('"', '')

def transform_to_data_list(valid_data):
    try:
        return valid_data[0].replace("]\');", '').replace('            let performArray = JSON.parse(\'[', '')\
        .split('},{')
    except Exception as error:
        print(error, f'cannot replace the string of {valid_data} function: transform_to_data_list')
        raise error
    
def split_commas(x) -> List[str]:
    try:
        return x.split(',')
    except Exception as error:
        print(error, 'cannot split function: split_commas')
        raise error
    
def get_values(x) -> BaseRow:
    try:
        return tuple(map(lambda y: y[y.find(':')+1:], x))
    except Exception as error:
        print(error, f'cannot get value of {x} function: get_values')
        raise error
    
def convert_to_datetime(value: str) -> datetime.datetime:
    return datetime.datetime.strptime(value, "%Y-%m-%d") if isinstance(value, str) else value

def convert_to_str(value: str) -> str:
    return str(value) if isinstance(value, (int, float, datetime.datetime)) else value

def convert_to_float(value: str) -> float:
    return float(value) if isinstance(value, (int, float, str)) else value

def convert_to_int(value: str) -> int:
    return int(value) if isinstance(value, (int, float, str)) else value



def define_data_type(row: BaseRow) -> DailyNav:
    # Define the types for each column
    types = [
        convert_to_datetime,  # datetime.datetime
        convert_to_str,       # str
        convert_to_float,     # float
        convert_to_float,     # float
        convert_to_float,     # float
        convert_to_float,     # float
        convert_to_float,     # float
        convert_to_int,       # int
        convert_to_datetime  # datetime.datetime
    ]

    # Apply the type conversions using map
    daily_nav: DailyNav = tuple(map(lambda x: x[0](x[1]), zip(types, row)))
    return daily_nav

def concat_funds(list1, list2):
    return list1 + list2

def extract_data(urls, cookie_string):
    def extract_data__inner(url):
        return (
            Pipe((url, cookie_string))
            >> request_get
            | convert_to_soup
            | get_data_section
            | (lambda x: list(filter(check_json, x)))
            | (lambda x: list(filter(check_is_valid_data, x)))
            | transform_to_data_list
            | (lambda x: map(replace_dict, x))
            | (lambda x: map(replace_quote, x))
            | (lambda x: map(split_commas, x))
            | (lambda x: map(get_values, x))
            | (lambda x: map(define_data_type, x))
            | list
        ).get()
    return (
        Pipe((urls))
        | (lambda x: map(extract_data__inner, x))
        | list
        | (lambda x: reduce(concat_funds, x))
    ).get()
