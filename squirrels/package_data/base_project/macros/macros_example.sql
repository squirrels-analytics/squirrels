{%- macro date_and_amount_filters(use_from_range) -%}
    {%- if use_from_range -%}

    date >= {{ ctx.start_date_from_range | quote }}
    AND date <= {{ ctx.end_date_from_range | quote }}
    AND amount >= {{ ctx.min_amount_from_range }}
    AND amount <= {{ ctx.max_amount_from_range }}

    {%- else -%}

    date >= {{ ctx.start_date | quote }}
    AND date <= {{ ctx.end_date | quote }}
    AND amount >= {{ ctx.min_amount }}
    AND amount <= {{ ctx.max_amount }}

    {%- endif -%}
{%- endmacro -%}
