from squirrels import arguments as args, parameters as p, parameter_options as po, data_sources as ds


## Example of creating SingleSelectParameter and specifying each option by code
@p.SingleSelectParameter.create_with_options(
    "group_by", "Group By", description="Dimension(s) to aggregate by", user_attribute="access_level"
)
def group_by_options():
    return [
        po.SelectParameterOption(
            "trans", "Transaction",  
            columns=["id","date","category","subcategory","description"],
            aliases=["_id","date","category","subcategory","description"], # any alias starting with "_" will not be selected - see context.py for implementation
            user_groups=["admin"]
        ),
        po.SelectParameterOption("day"    , "Day"         , columns=["date"], aliases=["day"]   , user_groups=["admin","member"]),
        po.SelectParameterOption("month"  , "Month"       , columns=["month"]                   , user_groups=["admin","member","guest"]),
        po.SelectParameterOption("cat"    , "Category"    , columns=["category"]                , user_groups=["admin","member","guest"]),
        po.SelectParameterOption("subcat" , "Subcategory" , columns=["category","subcategory"]  , user_groups=["admin","member","guest"]),
    ]


## Example of creating NumberParameter with options
@p.NumberParameter.create_with_options(
    "limit", "Max Number of Rows", description="Maximum number of rows to return", parent_name="group_by"
)
def limit_options():
    return [po.NumberParameterOption(0, 1000, increment=10, default_value=1000, parent_option_ids="trans")]


## Example of creating DateParameter
@p.DateParameter.create_from_source(
    "start_date", "Start Date", description="Start date to filter transactions by"
)
def start_date_source():
    return ds.DateDataSource(
        "SELECT min(date) AS min_date, max(date) AS max_date FROM expenses",
        default_date_col="min_date", min_date_col="min_date", max_date_col="max_date"
    )


## Example of creating DateParameter from list of DateParameterOption's
@p.DateParameter.create_with_options(
    "end_date", "End Date", description="End date to filter transactions by"
)
def end_date_options():
    return [po.DateParameterOption("2024-12-31", min_date="2024-01-01", max_date="2024-12-31")]


## Example of creating DateRangeParameter
@p.DateRangeParameter.create_simple(
    "date_range", "Date Range", "2024-01-01", "2024-12-31", min_date="2024-01-01", max_date="2024-12-31",
    description="Date range to filter transactions by"
)
def date_range_options():
    pass


## Example of creating MultiSelectParameter from lookup query/table
@p.MultiSelectParameter.create_from_source(
    "category", "Category Filter", 
    description="The expense categories to filter transactions by"
)
def category_source():
    return ds.SelectDataSource("seed_categories", "category_id", "category", source="seeds")


## Example of creating MultiSelectParameter with parent from lookup query/table
@p.MultiSelectParameter.create_from_source(
    "subcategory", "Subcategory Filter", parent_name="category",
    description="The expense subcategories to filter transactions by (available options are based on selected value(s) of 'Category Filter')"
)
def subcategory_source():
    return ds.SelectDataSource(
        "seed_subcategories", "subcategory_id", "subcategory", source="seeds", parent_id_col="category_id"
    )


## Example of creating NumberParameter
@p.NumberParameter.create_simple(
    "min_filter", "Amounts Greater Than", min_value=0, max_value=300, increment=10,
    description="Number to filter on transactions with an amount greater than this value"
)
def min_filter_options():
    pass


## Example of creating NumberParameter from lookup query/table
@p.NumberParameter.create_from_source(
    "max_filter", "Amounts Less Than",
    description="Number to filter on transactions with an amount less than this value"
)
def max_filter_source():
    query = "SELECT 0 as min_value, 300 as max_value, 10 as increment"
    return ds.NumberDataSource(
        query, "min_value", "max_value", increment_col="increment", default_value_col="max_value"
    )


## Example of creating NumberRangeParameter
@p.NumberRangeParameter.create_simple(
    "between_filter", "Amounts Between", min_value=0, max_value=300, default_lower_value=0, default_upper_value=300,
    description="Number range to filter on transactions with an amount within this range"
)
def between_filter_options():
    pass
