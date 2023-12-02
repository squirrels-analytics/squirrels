from typing import Dict, List, Any, Optional
import squirrels as sr


def main(
    ctx: Dict[str, Any], user: Optional[sr.UserBase], prms: Dict[str, sr.Parameter], args: Dict[str, Any], **kwargs
) -> None:
    if "group_by" in prms:
        group_by_param: sr.SingleSelectParameter = prms["group_by"]
        ctx["group_by_cols_list"]: List[str] = group_by_param.get_selected("columns")
        ctx["group_by_cols"] = ",".join(ctx["group_by_cols_list"])
        ctx["order_by_cols"] = ",".join((x+" DESC") for x in ctx["group_by_cols_list"])

    if "start_date" in prms:
        start_date_param: sr.DateParameter = prms["start_date"]
        ctx["start_date"] = start_date_param.get_selected_date_quoted()
    
    if "end_date" in prms:
        end_date_param: sr.DateParameter = prms["end_date"]
        ctx["end_date"] = end_date_param.get_selected_date_quoted()
    
    if "category" in prms:
        category_param: sr.MultiSelectParameter = prms["category"]
        ctx["has_categories"] = category_param.has_non_empty_selection()
        ctx["categories"] = category_param.get_selected_labels_quoted_joined()
    
    if "subcategory" in prms:
        subcategory_param: sr.MultiSelectParameter = prms["subcategory"]
        ctx["has_subcategories"] = subcategory_param.has_non_empty_selection()
        ctx["subcategories"] = subcategory_param.get_selected_labels_quoted_joined()
    
    if "min_filter" in prms:
        min_amount_filter: sr.NumberParameter = prms["min_filter"]
        ctx["min_amount"] = min_amount_filter.get_selected_value()
    
    if "max_filter" in prms:
        max_amount_filter: sr.NumberParameter = prms["max_filter"]
        ctx["max_amount"] = max_amount_filter.get_selected_value()
    