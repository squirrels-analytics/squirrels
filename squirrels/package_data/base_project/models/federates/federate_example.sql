SELECT *
FROM {{ ref("dbview_example") }}
ORDER BY {{ ctx.order_by_cols }}
