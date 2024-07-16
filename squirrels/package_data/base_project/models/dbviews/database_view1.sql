WITH
transactions_with_masked_id AS (
    SELECT *,
{%- if user.role == "manager" %}
        id as masked_id
{%- else %}
        '***' as masked_id
{%- endif %}
    FROM transactions
)
SELECT {{ ctx.select_dim_cols }}
    , sum(-amount) as total_amount
FROM transactions_with_masked_id
WHERE date >= :start_date
    AND date <= :end_date
    AND -amount >= :min_amount
    AND -amount <= :max_amount
    {% if is_placeholder("desc_pattern") -%} AND description LIKE :desc_pattern {%- endif %}
    {% if ctx.has_categories -%} AND category IN ({{ ctx.categories }}) {%- endif %}
    {% if ctx.has_subcategories -%} AND subcategory IN ({{ ctx.subcategories }}) {%- endif %}
GROUP BY {{ ctx.group_by_cols }}
