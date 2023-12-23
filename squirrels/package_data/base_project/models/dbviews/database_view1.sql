SELECT {{ ctx["group_by_cols"] }}
    , sum(-Amount) as Total_Amount
FROM transactions
WHERE 1=1
{%- if ctx["has_categories"] %}
    AND Category IN ({{ ctx["categories"] }})
{%- endif %}
{%- if ctx["has_subcategories"] %}
    AND Subcategory IN ({{ ctx["subcategories"] }})
{%- endif %}
    AND "Date" >= {{ ctx["start_date"] }}
    AND "Date" <= {{ ctx["end_date"] }}
    AND -Amount >= {{ ctx["min_amount"] }}
    AND -Amount <= {{ ctx["max_amount"] }}
GROUP BY {{ ctx["group_by_cols"] }}
