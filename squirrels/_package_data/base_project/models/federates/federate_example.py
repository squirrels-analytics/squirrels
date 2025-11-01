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

    if sqrl.ctx.get("has_categories"):
        categories: list[str] = sqrl.ctx["categories"]
        df = df.filter(pl.col("category_id").is_in(categories))

    if sqrl.ctx.get("has_subcategories"):
        subcategories: list[str] = sqrl.ctx["subcategories"]
        df = df.filter(pl.col("subcategory_id").is_in(subcategories))

    df = df.rename(sqrl.ctx.get("column_to_alias_mapping", {}))

    dimension_cols: list[str] | None = sqrl.ctx.get("group_by_cols")
    if dimension_cols is not None:
        df = df.group_by(dimension_cols).agg(
            pl.sum("amount").cast(pl.Decimal(precision=15, scale=2)).alias("total_amount")
        )
    else:
        df = df.rename({"amount": "total_amount"})
    
    order_by_cols: list[str] = sqrl.ctx.get("order_by_cols")
    if order_by_cols is not None:
        df = df.select(*order_by_cols, "total_amount").sort(order_by_cols, descending=True)

    # Apply mask_column_function to description column if it exists
    mask_column_func = sqrl.ctx.get("mask_column_function")
    if "description" in order_by_cols and mask_column_func:
        df = df.with_columns(
            pl.col("description").map_elements(mask_column_func, return_dtype=pl.String).alias("description")
        )

    return df
