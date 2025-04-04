{# SQLite dialect (based on connection used) #}

SELECT STRFTIME('%Y-%m', date) AS month
    , printf('%.2f', SUM(amount)) as total_amount

FROM {{ source("src_transactions") }}

WHERE {{ date_and_amount_filters(use_from_range=false) }}

GROUP BY 1

ORDER BY 1 DESC
