from typing import Dict, Any, Optional
import squirrels as sr


def main(user: Optional[sr.UserBase], prms: Dict[str, sr.Parameter], args: Dict[str, Any], *p_args, **kwargs) -> Dict[str, Any]:
    group_by_param: sr.SingleSelectParameter = prms["group_by"]
    start_date_param: sr.DateParameter = prms["start_date"]
    end_date_param: sr.DateParameter = prms["end_date"]
    category_param: sr.MultiSelectParameter = prms["category"]
    subcategory_param: sr.MultiSelectParameter = prms["subcategory"]
    min_amount_filter: sr.NumberParameter = prms["min_filter"]
    max_amount_filter: sr.NumberParameter = prms["max_filter"]

    return {
        "group_by_cols": group_by_param.get_selected("columns"),
        "start_date": start_date_param.get_selected_date_quoted(),
        "end_date": end_date_param.get_selected_date_quoted(),
        "categories": category_param.get_selected_labels_quoted_joined(),
        "subcategories": subcategory_param.get_selected_labels_quoted_joined(),
        "min_amount": min_amount_filter.get_selected_value(),
        "max_amount": max_amount_filter.get_selected_value()
    }
