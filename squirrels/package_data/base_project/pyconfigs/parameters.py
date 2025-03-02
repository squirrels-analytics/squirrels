from squirrels import ParametersArgs, parameters as p, parameter_options as po, data_sources as ds


def main(sqrl: ParametersArgs) -> None:
    """
    Create all widget parameters in this file. If two or more datasets use a different set of parameters, define them all
    here, and specify the subset of parameters used for each dataset in the "squirrels.yml" file.

    Parameters are created by a factory method associated to the parameter class. For example, "CreateWithOptions" is the factory method used here:
    > p.SingleSelectParameter.CreateWithOptions(...)

    The parameter classes available are:
    - SingleSelectParameter, MultiSelectParameter, DateParameter, DateRangeParameter, NumberParameter, NumberRangeParameter, TextParameter
    
    The factory methods available are:
    - CreateSimple, CreateWithOptions, CreateFromSource
    """

    ## Example of creating SingleSelectParameter and specifying each option by code
    user_attribute = "role"
    group_by_options = [
        po.SelectParameterOption("g0", "Transaction", columns=["date", "category", "subcategory", "description"], user_groups=["manager"]),
        po.SelectParameterOption("g1", "Day", columns=["date"], aliases=["day"], user_groups=["manager", "employee"]),
        po.SelectParameterOption("g4", "Month", columns=["month"], user_groups=["manager", "employee"]),
        po.SelectParameterOption("g2", "Category", columns=["category"], user_groups=["manager", "employee"]),
        po.SelectParameterOption("g3", "Subcategory", columns=["category", "subcategory"], user_groups=["manager", "employee"]),
    ]
    p.SingleSelectParameter.CreateWithOptions(
        "group_by", "Group By", group_by_options, description="Dimension(s) to aggregate by", user_attribute=user_attribute
    )

    ## Example of creating NumberParameter with options
    parent = "group_by"
    limit_options = [po.NumberParameterOption(0, 1000, increment=10, default_value=1000, parent_option_ids="g0")]
    p.NumberParameter.CreateWithOptions(
        "limit", "Max Number of Rows", limit_options, parent_name=parent, description="Maximum number of rows to return"
    )

    ## Example of creating DateParameter
    start_date_source = ds.DateDataSource(
        "SELECT min(date) AS min_date, max(date) AS max_date FROM expenses",
        default_date_col="min_date", min_date_col="min_date", max_date_col="max_date"
    )
    p.DateParameter.CreateFromSource(
        "start_date", "Start Date", start_date_source, description="Start date to filter transactions by"
    )

    ## Example of creating DateParameter from list of DateParameterOption's
    end_date_option = [po.DateParameterOption("2024-12-31", min_date="2024-01-01", max_date="2024-12-31")]
    p.DateParameter.CreateWithOptions(
        "end_date", "End Date", end_date_option, description="End date to filter transactions by"
    )

    ## Example of creating DateRangeParameter
    p.DateRangeParameter.CreateSimple(
        "date_range", "Date Range", "2024-01-01", "2024-12-31", min_date="2024-01-01", max_date="2024-12-31",
        description="Date range to filter transactions by"
    )

    ## Example of creating MultiSelectParameter from lookup query/table
    category_ds = ds.SelectDataSource("seed_categories", "category_id", "category", from_seeds=True)
    p.MultiSelectParameter.CreateFromSource(
        "category", "Category Filter", category_ds, description="The expense categories to filter transactions by"
    )

    ## Example of creating MultiSelectParameter with parent from lookup query/table
    parent_name = "category"
    subcategory_ds = ds.SelectDataSource(
        "seed_subcategories", "subcategory_id", "subcategory", from_seeds=True, parent_id_col="category_id"
    )
    p.MultiSelectParameter.CreateFromSource(
        "subcategory", "Subcategory Filter", subcategory_ds, parent_name=parent_name,
        description="The expense subcategories to filter transactions by (available options are based on selected value(s) of 'Category Filter')"
    )

    ## Example of creating NumberParameter
    p.NumberParameter.CreateSimple(
        "min_filter", "Amounts Greater Than", min_value=0, max_value=300, increment=10,
        description="Number to filter on transactions with an amount greater than this value"
    )
    
    ## Example of creating NumberParameter from lookup query/table
    query = "SELECT 0 as min_value, 300 as max_value, 10 as increment"
    max_amount_ds = ds.NumberDataSource(query, "min_value", "max_value", increment_col="increment", default_value_col="max_value")
    p.NumberParameter.CreateFromSource(
        "max_filter", "Amounts Less Than", max_amount_ds, description="Number to filter on transactions with an amount less than this value"
    )

    ## Example of creating NumberRangeParameter
    p.NumberRangeParameter.CreateSimple(
        "between_filter", "Amounts Between", 0, 300, default_lower_value=0, default_upper_value=300,
        description="Number range to filter on transactions with an amount within this range"
    )
