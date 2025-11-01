from typing import cast, Any
from squirrels import arguments as args, parameters as p

from pyconfigs.user import CustomUserFields


def main(ctx: dict[str, Any], sqrl: args.ContextArgs) -> None:
    """
    Define context variables AFTER parameter selections are made by adding entries to the dictionary "ctx". 
    These context variables can then be used in the models.

    Note that the code here is used by all datasets, regardless of the parameters they use. You can use 
    sqrl.param_exists to determine the conditions to execute certain blocks of code.
    """
    custom_fields = cast(CustomUserFields, sqrl.user.custom_fields)

    if sqrl.param_exists("group_by"):
        group_by_param = sqrl.prms["group_by"]
        assert isinstance(group_by_param, p.SingleSelectParameter)
        
        selected_id = group_by_param.get_selected_id()
        columns = group_by_param.get_selected("columns")
        aliases = group_by_param.get_selected("aliases", default_field="columns")
        assert isinstance(columns, list) and isinstance(aliases, list) and len(columns) == len(aliases)

        column_to_alias_mapping = {x: y for x, y in zip(columns, aliases) if not y.startswith("_")}
        order_by_cols = list(column_to_alias_mapping.values())

        ctx["column_to_alias_mapping"] = column_to_alias_mapping
        ctx["group_by_cols"] = order_by_cols if selected_id != "trans" else None

        # Only used if federate_example is a Python model
        mask_column = lambda x: x if custom_fields.role == "manager" else "***MASKED***"
        ctx["order_by_cols"] = order_by_cols
        ctx["mask_column_function"] = mask_column
        
        # Only used if federate_example is a SQL model
        mask_column = lambda x: x if custom_fields.role == "manager" else "'***MASKED***'"
        x_as_y = lambda x, y: (mask_column(x) if x in ["description"] else x)+" as "+y
        ctx["select_dim_cols"] = list(x_as_y(x, y) for x, y in column_to_alias_mapping.items())
        ctx["aggregator"] = "SUM" if selected_id != "trans" else ""
        ctx["order_by_cols_desc"] = list(y+" DESC" for y in order_by_cols)
        
    if sqrl.param_exists("start_date"):
        start_date_param = sqrl.prms["start_date"]
        assert isinstance(start_date_param, p.DateParameter)

        ctx["start_date"] = start_date_param.get_selected_date()
    
    if sqrl.param_exists("end_date"):
        end_date_param = sqrl.prms["end_date"]
        assert isinstance(end_date_param, p.DateParameter)

        ctx["end_date"] = end_date_param.get_selected_date()

    if sqrl.param_exists("date_range"):
        date_range_param = sqrl.prms["date_range"]
        assert isinstance(date_range_param, p.DateRangeParameter)

        ctx["start_date_from_range"] = date_range_param.get_selected_start_date()
        ctx["end_date_from_range"] = date_range_param.get_selected_end_date()
    
    if sqrl.param_exists("category"):
        category_param = sqrl.prms["category"]
        assert isinstance(category_param, p.MultiSelectParameter)

        ctx["has_categories"] = category_param.has_non_empty_selection()
        ctx["categories"] = category_param.get_selected_ids_as_list()
    
    if sqrl.param_exists("subcategory"):
        subcategory_param = sqrl.prms["subcategory"]
        assert isinstance(subcategory_param, p.MultiSelectParameter)

        ctx["has_subcategories"] = subcategory_param.has_non_empty_selection()
        ctx["subcategories"] = subcategory_param.get_selected_ids_as_list()
    
    if sqrl.param_exists("min_filter"):
        min_amount_filter = sqrl.prms["min_filter"]
        assert isinstance(min_amount_filter, p.NumberParameter)

        ctx["min_amount"] = min_amount_filter.get_selected_value()
    
    if sqrl.param_exists("max_filter"):
        max_amount_filter = sqrl.prms["max_filter"]
        assert isinstance(max_amount_filter, p.NumberParameter)

        ctx["max_amount"] = max_amount_filter.get_selected_value()

    if sqrl.param_exists("between_filter"):
        between_filter = sqrl.prms["between_filter"]
        assert isinstance(between_filter, p.NumberRangeParameter)

        ctx["min_amount_from_range"] = between_filter.get_selected_lower_value()
        ctx["max_amount_from_range"] = between_filter.get_selected_upper_value()
    