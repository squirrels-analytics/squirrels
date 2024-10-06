from typing import Any
from squirrels import ContextArgs, parameters as p


def main(ctx: dict[str, Any], sqrl: ContextArgs) -> None:
    """
    Define context variables AFTER parameter selections are made by adding entries to the dictionary "ctx". 
    These context variables can then be used in the models.

    Note that the code here is used by all datasets, regardless of the parameters they use. You can use 
    sqrl.prms and/or sqrl.traits to determine the conditions to execute certain blocks of code.
    """
    if sqrl.param_exists("group_by"):
        group_by_param = sqrl.prms["group_by"]
        assert isinstance(group_by_param, p.SingleSelectParameter)
        
        columns = group_by_param.get_selected("columns")
        aliases = group_by_param.get_selected("aliases", default_field="columns")
        assert isinstance(columns, list) and isinstance(aliases, list)

        ctx["select_dim_cols"] = ", ".join(x+" as "+y for x, y in zip(columns, aliases))
        ctx["group_by_cols"] = ", ".join(columns)
        ctx["order_by_cols"] = ", ".join((x+" DESC") for x in aliases)
        ctx["order_by_cols_list"] = aliases

    if sqrl.param_exists("description_filter"):
        descript_param = sqrl.prms["description_filter"]
        assert isinstance(descript_param, p.TextParameter)

        desc_pattern = descript_param.get_entered_text().apply_percent_wrap()

        sqrl.set_placeholder("desc_pattern", desc_pattern)

    if sqrl.param_exists("start_date"):
        start_date_param = sqrl.prms["start_date"]
        assert isinstance(start_date_param, p.DateParameter)

        start_date = start_date_param.get_selected_date()
        
        sqrl.set_placeholder("start_date", start_date)
    
    if sqrl.param_exists("end_date"):
        end_date_param = sqrl.prms["end_date"]
        assert isinstance(end_date_param, p.DateParameter)

        end_date = end_date_param.get_selected_date()

        sqrl.set_placeholder("end_date", end_date)

    if sqrl.param_exists("date_range"):
        date_range_param = sqrl.prms["date_range"]
        assert isinstance(date_range_param, p.DateRangeParameter)

        start_date = date_range_param.get_selected_start_date()
        end_date = date_range_param.get_selected_end_date()

        sqrl.set_placeholder("start_date", start_date)
        sqrl.set_placeholder("end_date", end_date)
    
    if sqrl.param_exists("category"):
        category_param = sqrl.prms["category"]
        assert isinstance(category_param, p.MultiSelectParameter)

        ctx["has_categories"] = category_param.has_non_empty_selection()
        ctx["categories"] = category_param.get_selected_labels_quoted_joined()
    
    if sqrl.param_exists("subcategory"):
        subcategory_param = sqrl.prms["subcategory"]
        assert isinstance(subcategory_param, p.MultiSelectParameter)

        ctx["has_subcategories"] = subcategory_param.has_non_empty_selection()
        ctx["subcategories"] = subcategory_param.get_selected_labels_quoted_joined()
    
    if sqrl.param_exists("min_filter"):
        min_amount_filter = sqrl.prms["min_filter"]
        assert isinstance(min_amount_filter, p.NumberParameter)

        min_amount = min_amount_filter.get_selected_value()

        sqrl.set_placeholder("min_amount", min_amount)
    
    if sqrl.param_exists("max_filter"):
        max_amount_filter = sqrl.prms["max_filter"]
        assert isinstance(max_amount_filter, p.NumberParameter)

        max_amount = max_amount_filter.get_selected_value()

        sqrl.set_placeholder("max_amount", max_amount)

    if sqrl.param_exists("between_filter"):
        between_filter = sqrl.prms["between_filter"]
        assert isinstance(between_filter, p.NumberRangeParameter)

        min_amount = between_filter.get_selected_lower_value()
        max_amount = between_filter.get_selected_upper_value()

        sqrl.set_placeholder("min_amount", min_amount)
        sqrl.set_placeholder("max_amount", max_amount)
    