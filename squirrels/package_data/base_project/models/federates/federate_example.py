from typing import Sequence
from squirrels import ModelDepsArgs, ModelArgs
import pandas as pd


def dependencies(sqrl: ModelDepsArgs) -> Sequence[str]:
    """
    Define list of dependent models here. This will determine the dependencies first, at compile-time, 
    before running the model.
    """
    return ["dbview_example"]


def main(sqrl: ModelArgs) -> pd.DataFrame:
    """
    Create federated models by joining/processing dependent database views and/or other federated models to
    form and return the result as a new pandas DataFrame.
    """
    (DBVIEW_EXAMPLE,) = dependencies(sqrl)
    df = sqrl.ref(DBVIEW_EXAMPLE)
    return df.sort_values(sqrl.ctx["order_by_cols_list"], ascending=False)
