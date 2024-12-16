{# DuckDB dialect #}

SELECT a.id,
    STRFTIME(a.date, '%Y-%m-%d') AS date,
    STRFTIME(a.date, '%Y-%m') AS month,
    c.category_id,
    c.category,
    b.subcategory_id,
    b.subcategory,
    a.amount,
    a.description

{# ref() can be used on a sources, seeds, or other build models -#}
FROM {{ ref("src_transactions") }} AS a 
    LEFT JOIN {{ ref("seed_subcategories") }} AS b ON a.subcategory_id = b.subcategory_id
    LEFT JOIN {{ ref("seed_categories") }} AS c ON b.category_id = c.category_id
