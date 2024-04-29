import squirrels as sr


def main(sqrl: sr.ParametersArgs) -> None:
    """
    Create all widget parameters in this file. If two or more datasets use a different set of parameters, define them all
    here, and specify the subset of parameters used for each dataset in the "squirrels.yml" file.

    Parameters are created by a factory method associated to some parameters class. For example (note the "Create"):
    > sr.SingleSelectParameter.Create(...)

    The parameter classes available are:
    - SingleSelectParameter, MultiSelectParameter, DateParameter, DateRangeParameter, NumberParameter, NumberRangeParameter
    
    The factory methods available are:
    - Create, CreateSimple, CreateFromSource
    """

    ## Example of creating SingleSelectParameter and specifying each option by code
    group_by_options = [
        sr.SelectParameterOption("g0", "Transaction", columns=["id", "date"]),
        sr.SelectParameterOption("g1", "Date", columns=["date"]),
        sr.SelectParameterOption("g2", "Category", columns=["category"]),
        sr.SelectParameterOption("g3", "Subcategory", columns=["category", "subcategory"]),
    ]
    sr.SingleSelectParameter.Create(
        "group_by", "Group By", group_by_options, description="Dimension to aggregate by"
    )

    ## Example of creating DateParameter
    sr.DateParameter.CreateSimple(
        "start_date", "Start Date", "2023-01-01", description="Filter by transactions after this date"
    )

    ## Example of creating DateParameter from list of DateParameterOption's
    end_date_option = [sr.DateParameterOption("2023-12-31")]
    sr.DateParameter.Create(
        "end_date", "End Date", end_date_option, description="Filter by transactions before this date"
    )

    ## Example of creating DateRangeParameter
    sr.DateRangeParameter.CreateSimple(
        "date_range", "Date Range", "2023-01-01", "2023-12-31", description="Filter by transactions within this date range"
    )

    ## Example of creating MultiSelectParameter from lookup query/table
    category_ds = sr.SelectDataSource("seed_categories", "category_id", "category", from_seeds=True)
    sr.MultiSelectParameter.CreateFromSource(
        "category", "Category Filter", category_ds, description="The expense categories to filter by"
    )

    ## Example of creating MultiSelectParameter with parent from lookup query/table
    subcategory_ds = sr.SelectDataSource(
        "seed_subcategories", "subcategory_id", "subcategory", from_seeds=True, parent_id_col="category_id"
    )
    sr.MultiSelectParameter.CreateFromSource(
        "subcategory", "Subcategory Filter", subcategory_ds, parent_name="category",
        description="The expense subcategories to filter by (available options based on selected 'Category Filter')"
    )

    ## Example of creating NumberParameter
    sr.NumberParameter.CreateSimple(
        "min_filter", "Amounts Greater Than", min_value=0, max_value=500, increment=10,
        description="Filter by transactions greater than this amount"
    )
    
    ## Example of creating NumberParameter from lookup query/table
    query = "SELECT 0 as min_value, max(-amount) as max_value, 10 as increment FROM transactions WHERE category <> 'Income'"
    max_amount_ds = sr.NumberDataSource(query, "min_value", "max_value", increment_col="increment", default_value_col="max_value")
    sr.NumberParameter.CreateFromSource(
        "max_filter", "Amounts Less Than", max_amount_ds, description="Filter by transactions less than this amount"
    )

    ## Example of creating NumberRangeParameter
    sr.NumberRangeParameter.CreateSimple(
        "between_filter", "Amounts Between", 0, 500, default_lower_value=10, default_upper_value=400,
        description="Filter by transaction amounts within this range"
    )
