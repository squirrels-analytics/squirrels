from squirrels import arguments as args
import polars as pl, pandas as pd


def main(sqrl: args.ModelArgs) -> pl.LazyFrame | pl.DataFrame | pd.DataFrame:
    """
    Create federated models by joining/processing dependent models (sources, seeds, builds, dbviews, other federates, etc.) to
    form a new Python DataFrame (using polars LazyFrame, polars DataFrame, or pandas DataFrame).
    """
    df = sqrl.ref("build_example")

    df = df.filter(
        (pl.col("date") >= sqrl.ctx["start_date_from_range"]) &
        (pl.col("date") <= sqrl.ctx["end_date_from_range"]) &
        (pl.col("amount") >= sqrl.ctx["min_amount_from_range"]) &
        (pl.col("amount") <= sqrl.ctx["max_amount_from_range"])
    )

    if sqrl.ctx["has_categories"]:
        categories: list[str] = sqrl.ctx["categories"]
        df = df.filter(pl.col("category_id").is_in(categories))

    if sqrl.ctx["has_subcategories"]:
        subcategories: list[str] = sqrl.ctx["subcategories"]
        df = df.filter(pl.col("subcategory_id").is_in(subcategories))

    dimension_cols: list[str] = sqrl.ctx["group_by_cols"]
    df = df.group_by(dimension_cols).agg(
        pl.sum("amount").cast(pl.Decimal(precision=15, scale=2)).alias("total_amount")
    )
    df = df.select(*dimension_cols, "total_amount").rename(sqrl.ctx["rename_dict"])

    order_by_cols: list[str] = sqrl.ctx["order_by_cols"]
    df = df.sort(order_by_cols, descending=True)

    if "limit" in sqrl.ctx:
        limit: int = sqrl.ctx["limit"]
        df = df.limit(limit)

    return df
