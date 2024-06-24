from typing import Iterable
from squirrels import ModelDepsArgs, ModelArgs
import pandas as pd


def dependencies(sqrl: ModelDepsArgs) -> Iterable[str]:
    """
    Define list of dependent models here. This will determine the dependencies first, at compile-time, 
    before running the model.
    """
    return ["database_view1"]


def main(sqrl: ModelArgs) -> pd.DataFrame:
    """
    Create federated models by joining/processing dependent database views and/or other federated models to
    form and return the result as a new pandas DataFrame.
    """
    df = sqrl.ref("database_view1")
    order_by_cols: str = sqrl.ctx["order_by_cols_list"]
    return df.sort_values(order_by_cols, ascending=False)
