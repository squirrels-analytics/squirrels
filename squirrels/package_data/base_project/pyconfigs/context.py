from typing import Any
from squirrels import ContextArgs, parameters as p


def main(ctx: dict[str, Any], sqrl: ContextArgs) -> None:
    """
    Define context variables AFTER parameter selections are made by adding entries to the dictionary "ctx". 
    These context variables can then be used in the models.

    Note that the code here is used by all datasets, regardless of the parameters they use. You can use 
    sqrl.prms and/or sqrl.traits to determine the conditions to execute certain blocks of code.
    """
    if sqrl.prms_contain("group_by"):
        group_by_param: p.SingleSelectParameter = sqrl.prms["group_by"]
        columns = group_by_param.get_selected("columns")
        aliases = group_by_param.get_selected("aliases", default_field="columns")

        ctx["select_dim_cols"] = ", ".join(x+" as "+y for x, y in zip(columns, aliases))
        ctx["group_by_cols"] = ", ".join(columns)
        ctx["order_by_cols"] = ", ".join((x+" DESC") for x in aliases)
        ctx["order_by_cols_list"] = aliases

    if sqrl.prms_contain("description_filter"):
        descript_param: p.TextParameter = sqrl.prms["description_filter"]
        desc_pattern = descript_param.get_entered_text().apply_percent_wrap()
        sqrl.set_placeholder("desc_pattern", desc_pattern)

    if sqrl.prms_contain("start_date"):
        start_date_param: p.DateParameter = sqrl.prms["start_date"]
        start_date = start_date_param.get_selected_date()
        sqrl.set_placeholder("start_date", start_date)
    
    if sqrl.prms_contain("end_date"):
        end_date_param: p.DateParameter = sqrl.prms["end_date"]
        end_date = end_date_param.get_selected_date()
        sqrl.set_placeholder("end_date", end_date)

    if sqrl.prms_contain("date_range"):
        date_range_param: p.DateRangeParameter = sqrl.prms["date_range"]
        start_date = date_range_param.get_selected_start_date()
        end_date = date_range_param.get_selected_end_date()
        sqrl.set_placeholder("start_date", start_date)
        sqrl.set_placeholder("end_date", end_date)
    
    if sqrl.prms_contain("category"):
        category_param: p.MultiSelectParameter = sqrl.prms["category"]
        ctx["has_categories"] = category_param.has_non_empty_selection()
        ctx["categories"] = category_param.get_selected_labels_quoted_joined()
    
    if sqrl.prms_contain("subcategory"):
        subcategory_param: p.MultiSelectParameter = sqrl.prms["subcategory"]
        ctx["has_subcategories"] = subcategory_param.has_non_empty_selection()
        ctx["subcategories"] = subcategory_param.get_selected_labels_quoted_joined()
    
    if sqrl.prms_contain("min_filter"):
        min_amount_filter: p.NumberParameter = sqrl.prms["min_filter"]
        min_amount = min_amount_filter.get_selected_value()
        sqrl.set_placeholder("min_amount", min_amount)
    
    if sqrl.prms_contain("max_filter"):
        max_amount_filter: p.NumberParameter = sqrl.prms["max_filter"]
        max_amount = max_amount_filter.get_selected_value()
        sqrl.set_placeholder("max_amount", max_amount)

    if sqrl.prms_contain("between_filter"):
        between_filter: p.NumberRangeParameter = sqrl.prms["between_filter"]
        min_amount = between_filter.get_selected_lower_value()
        max_amount = between_filter.get_selected_upper_value()
        sqrl.set_placeholder("min_amount", min_amount)
        sqrl.set_placeholder("max_amount", max_amount)
    