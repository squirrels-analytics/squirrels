from squirrels import BuildModelArgs
import polars as pl, pandas as pd


def main(sqrl: BuildModelArgs) -> pl.LazyFrame | pl.DataFrame | pd.DataFrame:
    """
    Create a build model by joining/processing sources or other build models to form a new
    Python DataFrame (using polars LazyFrame, polars DataFrame, or pandas DataFrame).
    """
    # sqrl.ref() can be used on a sources, seeds, or other build models
    expenses_df = sqrl.ref("src_transactions")
    categories_df = sqrl.ref("seed_categories")
    subcategories_df = sqrl.ref("seed_subcategories")

    df = expenses_df \
        .join(subcategories_df, on="subcategory_id", how="left") \
        .join(categories_df, on="category_id", how="left")

    df = df.with_columns(
        pl.col("date").dt.strftime("%Y-%m").alias("month"),
        pl.col("date").dt.strftime("%Y-%m-%d").alias("date"),
    )

    return df.select(
        "id", "date", "month", "category_id", "category", "subcategory_id", "subcategory", "amount", "description"
    )
