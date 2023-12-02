from typing import Dict, Any
import squirrels as sr


def main(proj: Dict[str, Any], **kwargs) -> None:
    
    ## Example of creating SingleSelectParameter (similar for MultiSelectParameter)
    group_by_options = [
        sr.SelectParameterOption("g0", "Transaction", columns=["ID", "Date"]),
        sr.SelectParameterOption("g1", "Date", columns=["Date"]),
        sr.SelectParameterOption("g2", "Category", columns=["Category"]),
        sr.SelectParameterOption("g3", "Subcategory", columns=["Category", "Subcategory"]),
    ]
    sr.SingleSelectParameter.Create("group_by", "Group By", group_by_options)

    ## Example of creating DateParameter
    sr.DateParameter.CreateSimple("start_date", "Start Date", "2023-01-01")

    ## Example of creating DateParameter from with list of DateParameterOption's
    end_date_option = [sr.DateParameterOption("2023-12-31")]
    sr.DateParameter.Create("end_date", "End Date", end_date_option)

    ## Example of creating DateRangeParameter
    sr.DateRangeParameter.CreateSimple("date_range", "Date Range", "2023-01-01", "2023-12-31")

    ## Example of creating MultiSelectParameter from lookup query/table
    category_ds = sr.MultiSelectDataSource("categories", "Category_ID", "Category")
    sr.MultiSelectParameter.CreateFromSource("category", "Category Filter", category_ds)

    ## Example of creating MultiSelectParameter with parent from lookup query/table
    subcategory_ds = sr.MultiSelectDataSource("subcategories", "Subcategory_ID", "Subcategory", parent_id_col="Category_ID")
    sr.MultiSelectParameter.CreateFromSource("subcategory", "Subcategory Filter", subcategory_ds, parent_name="category")

    ## Example of creating NumberParameter
    sr.NumberParameter.CreateSimple("min_filter", "Amounts Greater Than", min_value=0, max_value=500, increment=10)
    
    ## Example of creating NumberParameter from lookup query/table
    query = "SELECT 0 as min_value, max(-Amount) as max_value, 10 as increment FROM transactions WHERE Category <> 'Income'"
    max_amount_ds = sr.NumberDataSource(query, "min_value", "max_value", increment_col="increment", default_value_col="max_value")
    sr.NumberParameter.CreateFromSource("max_filter", "Amounts Less Than", max_amount_ds)

    ## Example of creating NumberRangeParameter
    sr.NumRangeParameter.CreateSimple("between_filter", "Amounts Between", 0, 500, default_lower_value=10, default_upper_value=400)
