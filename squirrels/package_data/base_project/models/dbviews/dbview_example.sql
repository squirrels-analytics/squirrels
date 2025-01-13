{# SQLite dialect (based on connection used) #}

SELECT STRFTIME('%Y-%m', date) AS month
    , ROUND(SUM(amount), 2) as total_amount

FROM {{ source("src_transactions") }}

WHERE {{ date_and_amount_filters(ctx) }}

GROUP BY 1

ORDER BY 1 DESC
