from squirrels import ParametersArgs, parameters as p, parameter_options as po, data_sources as ds


def main(sqrl: ParametersArgs) -> None:
    """
    Create all widget parameters in this file. If two or more datasets use a different set of parameters, define them all
    here, and specify the subset of parameters used for each dataset in the "squirrels.yml" file.

    Parameters are created by a factory method associated to some parameters class. For example (note the "Create"):
    > p.SingleSelectParameter.Create(...)

    The parameter classes available are:
    - SingleSelectParameter, MultiSelectParameter, DateParameter, DateRangeParameter, NumberParameter, NumberRangeParameter, TextParameter
    
    The factory methods available are:
    - Create, CreateSimple, CreateFromSource
    """

    ## Example of creating SingleSelectParameter and specifying each option by code
    group_by_options = [
        po.SelectParameterOption("g0", "Transaction", columns=["masked_id", "date", "description"], aliases=["id", "date", "description"]),
        po.SelectParameterOption("g1", "Date", columns=["date"]),
        po.SelectParameterOption("g2", "Category", columns=["category"]),
        po.SelectParameterOption("g3", "Subcategory", columns=["category", "subcategory"]),
    ]
    p.SingleSelectParameter.Create(
        "group_by", "Group By", group_by_options, description="Dimension to aggregate by"
    )

    ## Example of creating a TextParameter
    parent_name = "group_by"
    text_options = [po.TextParameterOption(parent_option_ids="g0")]
    p.TextParameter.Create(
        "description_filter", "Description Contains", text_options, parent_name=parent_name,
        description="Filter by transactions with this description"
    )

    ## Example of creating DateParameter
    p.DateParameter.CreateSimple(
        "start_date", "Start Date", "2023-01-01", description="Filter by transactions after this date"
    )

    ## Example of creating DateParameter from list of DateParameterOption's
    end_date_option = [po.DateParameterOption("2023-12-31")]
    p.DateParameter.Create(
        "end_date", "End Date", end_date_option, description="Filter by transactions before this date"
    )

    ## Example of creating DateRangeParameter
    p.DateRangeParameter.CreateSimple(
        "date_range", "Date Range", "2023-01-01", "2023-12-31", description="Filter by transactions within this date range"
    )

    ## Example of creating MultiSelectParameter from lookup query/table
    category_ds = ds.SelectDataSource("seed_categories", "category_id", "category", from_seeds=True)
    p.MultiSelectParameter.CreateFromSource(
        "category", "Category Filter", category_ds, description="The expense categories to filter by"
    )

    ## Example of creating MultiSelectParameter with parent from lookup query/table
    parent_name = "category"
    subcategory_ds = ds.SelectDataSource(
        "seed_subcategories", "subcategory_id", "subcategory", from_seeds=True, parent_id_col="category_id"
    )
    p.MultiSelectParameter.CreateFromSource(
        "subcategory", "Subcategory Filter", subcategory_ds, parent_name=parent_name,
        description="The expense subcategories to filter by (available options based on selected 'Category Filter')"
    )

    ## Example of creating NumberParameter
    p.NumberParameter.CreateSimple(
        "min_filter", "Amounts Greater Than", min_value=0, max_value=500, increment=10,
        description="Filter by transactions greater than this amount"
    )
    
    ## Example of creating NumberParameter from lookup query/table
    query = "SELECT 0 as min_value, max(-amount) as max_value, 10 as increment FROM transactions WHERE category <> 'Income'"
    max_amount_ds = ds.NumberDataSource(query, "min_value", "max_value", increment_col="increment", default_value_col="max_value")
    p.NumberParameter.CreateFromSource(
        "max_filter", "Amounts Less Than", max_amount_ds, description="Filter by transactions less than this amount"
    )

    ## Example of creating NumberRangeParameter
    p.NumberRangeParameter.CreateSimple(
        "between_filter", "Amounts Between", 0, 500, default_lower_value=10, default_upper_value=400,
        description="Filter by transaction amounts within this range"
    )
