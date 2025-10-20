from squirrels import parameters as p, parameter_options as po, data_sources as ds


## Example of creating SingleSelectParameter and specifying each option by code
@p.SingleSelectParameter.create_with_options(
    name="group_by", label="Group By", 
    description="Dimension(s) to aggregate by", 
    user_attribute="access_level"
)
def group_by_options():
    return [
        po.SelectParameterOption(
            id="trans", label="Transaction",  
            columns=["id","date","category","subcategory","description"],
            aliases=["_id","date","category","subcategory","description"], # in context.py, any alias starting with "_" will not be selected
            user_groups=["admin"]
        ),
        po.SelectParameterOption(
            id="day", label="Day", 
            columns=["date"], 
            aliases=["day"], 
            user_groups=["admin","member"]
        ),
        po.SelectParameterOption(
            id="month", label="Month", 
            columns=["month"], 
            user_groups=["admin","member","guest"]
        ),
        po.SelectParameterOption(
            id="cat", label="Category", 
            columns=["category"], 
            user_groups=["admin","member","guest"]
        ),
        po.SelectParameterOption(
            id="subcat", label="Subcategory", 
            columns=["category","subcategory"], 
            user_groups=["admin","member","guest"]
        ),
    ]


## Example of creating DateParameter
@p.DateParameter.create_from_source(
    name="start_date", label="Start Date", 
    description="Start date to filter transactions by"
)
def start_date_source():
    return ds.DateDataSource(
        table_or_query="SELECT min(date) AS min_date, max(date) AS max_date FROM expenses",
        default_date_col="min_date", 
        min_date_col="min_date", max_date_col="max_date",
    )


## Example of creating DateParameter from list of DateParameterOption's
@p.DateParameter.create_with_options(
    name="end_date", label="End Date", 
    description="End date to filter transactions by"
)
def end_date_options():
    return [
        po.DateParameterOption(
            default_date="2024-12-31", min_date="2024-01-01", max_date="2024-12-31"
        )
    ]


## Example of creating DateRangeParameter
@p.DateRangeParameter.create_simple(
    name="date_range", label="Date Range", 
    default_start_date="2024-01-01", default_end_date="2024-12-31", 
    min_date="2024-01-01", max_date="2024-12-31",
    description="Date range to filter transactions by"
)
def date_range_options():
    pass


## Example of creating MultiSelectParameter from lookup query/table
@p.MultiSelectParameter.create_from_source(
    name="category", label="Category Filter", 
    description="The expense categories to filter transactions by"
)
def category_source():
    return ds.SelectDataSource(
        table_or_query="seed_categories", 
        id_col="category_id", 
        options_col="category", 
        source=ds.SourceEnum.SEEDS
    )


## Example of creating MultiSelectParameter with parent from lookup query/table
@p.MultiSelectParameter.create_from_source(
    name="subcategory", label="Subcategory Filter",
    description="The expense subcategories to filter transactions by (available options are based on selected value(s) of 'Category Filter')", 
    parent_name="category"
)
def subcategory_source():
    return ds.SelectDataSource(
        table_or_query="seed_subcategories", 
        id_col="subcategory_id", 
        options_col="subcategory", 
        source=ds.SourceEnum.SEEDS, 
        parent_id_col="category_id"
    )


## Example of creating NumberParameter
@p.NumberParameter.create_simple(
    name="min_filter", label="Amounts Greater Than", 
    min_value=0, max_value=300, increment=10,
    description="Number to filter on transactions with an amount greater than this value"
)
def min_filter_options():
    pass


## Example of creating NumberParameter from lookup query/table
@p.NumberParameter.create_from_source(
    name="max_filter", label="Amounts Less Than",
    description="Number to filter on transactions with an amount less than this value"
)
def max_filter_source():
    return ds.NumberDataSource(
        table_or_query="SELECT 0 as min_value, 300 as max_value, 10 as increment",
        min_value_col="min_value", max_value_col="max_value", 
        increment_col="increment", 
        default_value_col="max_value"
    )


## Example of creating NumberRangeParameter
@p.NumberRangeParameter.create_simple(
    name="between_filter", label="Amounts Between", 
    min_value=0, max_value=300, 
    default_lower_value=0, default_upper_value=300,
    description="Number range to filter on transactions with an amount within this range"
)
def between_filter_options():
    pass
