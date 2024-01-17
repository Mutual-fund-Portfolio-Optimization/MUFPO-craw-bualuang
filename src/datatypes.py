import datetime
from typing import Tuple


BaseRow = Tuple[str, str, str, 
                str, str, str,
                str, str, str]

DailyNav = Tuple[
    datetime.datetime,
    str,
    float,
    float,
    float,
    float,
    float,
    int,
    datetime.datetime
]