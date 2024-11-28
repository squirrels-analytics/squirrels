from squirrels import ModelArgs
import polars as pl


def main(sqrl: ModelArgs) -> pl.LazyFrame:
    """
    Create federated models by joining/processing dependent database views and/or other federated models to
    form and return the result as a new pandas DataFrame.
    """
    df = sqrl.ref("dbview_example")
    return df.sort(sqrl.ctx["order_by_cols_list"], descending=True)
