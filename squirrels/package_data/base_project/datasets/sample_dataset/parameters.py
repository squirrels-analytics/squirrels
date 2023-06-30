from typing import Dict, Sequence, Any
import squirrels as sr


def main(args: Dict[str, Any], *p_args, **kwargs) -> Sequence[sr.Parameter]:
    
    ## Example of creating SingleSelectParameter (similar for MultiSelectParameter)
    group_by_options = [
        sr.SelectParameterOption("g0", "Transaction", columns="ID,Date"),
        sr.SelectParameterOption("g1", "Date", columns="Date"),
        sr.SelectParameterOption("g2", "Category", columns="Category"),
        sr.SelectParameterOption("g3", "Subcategory", columns="Category,Subcategory"),
    ]
    group_by_param = sr.SingleSelectParameter("group_by", "Group By", group_by_options)

    ## Example of creating DateParameter
    start_date_param = sr.DateParameter("start_date", "Start Date", "2023-01-01")

    ## Example of creating DateParameter from lookup query/table
    end_date_ds = sr.DateDataSource("SELECT max(Date) as date FROM transactions", "date")
    end_date_param = sr.DataSourceParameter(sr.DateParameter, "end_date", "End Date", end_date_ds)

    ## Example of creating MultiSelectParameter from lookup query/table
    category_ds = sr.SelectionDataSource("SELECT DISTINCT Category_ID, Category FROM categories", "Category_ID", "Category")
    category_filter = sr.DataSourceParameter(sr.MultiSelectParameter, "category", "Category Filter", category_ds)

    ## Example of creating MultiSelectParameter with parent from lookup query/table
    subcategory_ds = sr.SelectionDataSource("categories", "Subcategory_ID", "Subcategory", parent_id_col="Category_ID")
    subcategory_filter = sr.DataSourceParameter(sr.MultiSelectParameter, "subcategory", "Subcategory Filter", subcategory_ds, parent=category_filter)

    ## Example of creating NumberParameter
    min_amount_filter = sr.NumberParameter("min_filter", "Amounts Greater Than", 0, 500, 10)
    
    ## Example of creating NumberParameter from lookup query/table
    query = """
        SELECT 0 as min_value, max(-Amount) as max_value, 10 as increment \
        FROM transactions WHERE Category <> 'Income'
    """
    max_amount_ds = sr.NumberDataSource(query, "min_value", "max_value", "increment", default_value_col="max_value")
    max_amount_filter = sr.DataSourceParameter(sr.NumberParameter, "max_filter", "Amounts Less Than", max_amount_ds)
    
    return [
        group_by_param, 
        start_date_param, end_date_param, 
        category_filter, subcategory_filter, 
        min_amount_filter, max_amount_filter
    ]
