from typing import Iterable
import pandas as pd, squirrels as sr


def dependencies(sqrl: sr.FederateModelArgs) -> Iterable[str]:
    """
    Define list of dependent models here. This will compile and create the dependencies first before 
    running the model.
    """
    return ["database_view1"]


def main(sqrl: sr.FederateModelArgs) -> pd.DataFrame:
    """
    Create federated models by transforming dependent database views and other federated models to
    return a new (pandas) dataframe.
    """
    df = sqrl.ref("database_view1")
    group_by_cols: str = sqrl.ctx["group_by_cols_list"]
    return df.sort_values(group_by_cols, ascending=False)
