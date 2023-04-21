from typing import Dict, Callable, Any
from pandas import DataFrame
from squirrels import Parameter


def main(database_views: Dict[str, DataFrame], prms: Callable[[str], Parameter], ctx: Callable[[str], Any], proj: Callable[[str], str]) -> DataFrame:
    return database_views['ticker_history']
