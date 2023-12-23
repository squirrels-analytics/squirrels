SELECT *
FROM {{ ref("database_view1") }}
ORDER BY {{ ctx["order_by_cols"] }}
