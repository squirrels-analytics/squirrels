import pandas as pd
import squirrels as sq
from typing import Dict, Callable, Any


def main(database_views: Dict[str, pd.DataFrame], prms: Callable[[str], sq.Parameter], 
         ctx: Callable[[str], Any], proj: Callable[[str], str]) -> pd.DataFrame:
    df = database_views['database_view1']
    return df
