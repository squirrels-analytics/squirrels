from squirrels import ModelArgs, parameters as p
import polars as pl, pandas as pd

def dequote(value: str) -> str:
    return value[1:-1]

def joined_str_to_list(value: str) -> list[str]:
    return [dequote(category) for category in str(value).split(",")]


def main(sqrl: ModelArgs) -> pl.LazyFrame | pl.DataFrame | pd.DataFrame:
    """
    Create federated models by joining/processing dependent models (sources, seeds, builds, dbviews, other federates, etc.) to
    form a new Python DataFrame (using polars LazyFrame, polars DataFrame, or pandas DataFrame).
    """
    df = sqrl.ref("build_example")

    df = df.filter(
        (pl.col("amount") >= sqrl.ctx["min_amount_from_range"]) &
        (pl.col("amount") <= sqrl.ctx["max_amount_from_range"]) &
        (pl.col("date") >= dequote(sqrl.ctx["start_date_from_range"])) &
        (pl.col("date") <= dequote(sqrl.ctx["end_date_from_range"]))
    )

    if sqrl.ctx["has_categories"]:
        categories_list = joined_str_to_list(sqrl.ctx["categories"])
        df = df.filter(pl.col("category_id").is_in(categories_list))

    if sqrl.ctx["has_subcategories"]:
        subcategories_list = joined_str_to_list(sqrl.ctx["subcategories"])
        df = df.filter(pl.col("subcategory_id").is_in(subcategories_list))

    dimension_cols = sqrl.ctx["group_by_cols_list"]
    df = df.group_by(dimension_cols).agg(
        pl.sum("amount").cast(pl.Decimal(precision=15, scale=2)).alias("total_amount")
    )
    df = df.sort(dimension_cols, descending=True)

    if sqrl.param_exists("limit"):
        assert isinstance(limit := sqrl.prms["limit"], p.NumberParameter)
        df = df.limit(int(limit.get_selected_value()))

    df = df.select(*dimension_cols, "total_amount")
    return df.rename(sqrl.ctx["rename_dict"])
