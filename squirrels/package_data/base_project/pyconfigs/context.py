from typing import Any
import squirrels as sr


def main(ctx: dict[str, Any], sqrl: sr.ContextArgs) -> None:
    """
    Define context variables AFTER parameter selections are made by adding entries to the dictionary "ctx". 
    These context variables can then be used in the models.

    Note that the code here is used by all datasets, regardless of the parameters they use. You can use 
    sqrl.prms and/or sqrl.traits to determine the conditions to execute certain blocks of code.
    """

    if "group_by" in sqrl.prms:
        group_by_param: sr.SingleSelectParameter = sqrl.prms["group_by"]
        ctx["group_by_cols_list"] = group_by_param.get_selected("columns")
        ctx["group_by_cols"] = ",".join(ctx["group_by_cols_list"])
        ctx["order_by_cols"] = ",".join((x+" DESC") for x in ctx["group_by_cols_list"])

    if "start_date" in sqrl.prms:
        start_date_param: sr.DateParameter = sqrl.prms["start_date"]
        ctx["start_date"] = start_date_param.get_selected_date_quoted()
    
    if "end_date" in sqrl.prms:
        end_date_param: sr.DateParameter = sqrl.prms["end_date"]
        ctx["end_date"] = end_date_param.get_selected_date_quoted()

    if "date_range" in sqrl.prms:
        date_range_param: sr.DateRangeParameter = sqrl.prms["date_range"]
        ctx["start_date"] = date_range_param.get_selected_start_date_quoted()
        ctx["end_date"] = date_range_param.get_selected_end_date_quoted()
    
    if "category" in sqrl.prms:
        category_param: sr.MultiSelectParameter = sqrl.prms["category"]
        ctx["has_categories"] = category_param.has_non_empty_selection()
        ctx["categories"] = category_param.get_selected_labels_quoted_joined()
    
    if "subcategory" in sqrl.prms:
        subcategory_param: sr.MultiSelectParameter = sqrl.prms["subcategory"]
        ctx["has_subcategories"] = subcategory_param.has_non_empty_selection()
        ctx["subcategories"] = subcategory_param.get_selected_labels_quoted_joined()
    
    if "min_filter" in sqrl.prms:
        min_amount_filter: sr.NumberParameter = sqrl.prms["min_filter"]
        ctx["min_amount"] = min_amount_filter.get_selected_value()
    
    if "max_filter" in sqrl.prms:
        max_amount_filter: sr.NumberParameter = sqrl.prms["max_filter"]
        ctx["max_amount"] = max_amount_filter.get_selected_value()

    if "between_filter" in sqrl.prms:
        between_filter: sr.NumberRangeParameter = sqrl.prms["between_filter"]
        ctx["min_amount"] = between_filter.get_selected_lower_value()
        ctx["max_amount"] = between_filter.get_selected_upper_value()
    