from typing import Iterable
import pandas as pd, squirrels as sr


def dependencies(sqrl: sr.ModelDepsArgs) -> Iterable[str]:
    """
    Define list of dependent models here. This will determine the dependencies first, at compile-time, 
    before running the model.
    """
    return ["database_view1"]


def main(sqrl: sr.ModelArgs) -> pd.DataFrame:
    """
    Create federated models by joining/processing dependent database views and/or other federated models to
    form and return the result as a new pandas DataFrame.
    """
    df = sqrl.ref("database_view1")
    group_by_cols: str = sqrl.ctx["group_by_cols_list"]
    return df.sort_values(group_by_cols, ascending=False)
