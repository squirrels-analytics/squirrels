{%- macro date_and_amount_filters(ctx) -%}

    date >= {{ ctx.start_date }}
    AND date <= {{ ctx.end_date }}
    AND amount >= {{ ctx.min_amount }}
    AND amount <= {{ ctx.max_amount }}

{%- endmacro -%}
