SELECT {{ ctx["group_by_cols"] }}
    , sum(-amount) as total_amount
FROM transactions
WHERE 1=1
{%- if ctx["has_categories"] %}
    AND category IN ({{ ctx["categories"] }})
{%- endif %}
{%- if ctx["has_subcategories"] %}
    AND subcategory IN ({{ ctx["subcategories"] }})
{%- endif %}
    AND date >= {{ ctx["start_date"] }}
    AND date <= {{ ctx["end_date"] }}
    AND -amount >= {{ ctx["min_amount"] }}
    AND -amount <= {{ ctx["max_amount"] }}
GROUP BY {{ ctx["group_by_cols"] }}
