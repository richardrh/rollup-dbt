{% macro metric_codes() -%}
[
    'original_ylt_loss',
    'original_ylt_loss_uplifted',
    'original_ylt_loss_uplifted_capped',
    'original_ylt_loss_uplifted_capped_localccy',
    'original_ylt_loss_uplifted_capped_localccy_202601',
    'original_ylt_loss_uplifted_capped_localccy_202607',
    'original_ylt_loss_uplifted_capped_localccy_202701',
    'original_ylt_loss_uplifted_capped_localccy_202601_euws',
    'original_ylt_loss_uplifted_capped_localccy_202607_euws',
    'original_ylt_loss_uplifted_capped_localccy_202701_euws'
]
{%- endmacro %}

{% macro metric_code_list() -%}
{%- set formatted = [] -%}
{%- for metric in metric_codes() -%}
{%- do formatted.append('\'' ~ metric ~ '\'') -%}
{%- endfor -%}
{{ formatted | join(', ') }}
{%- endmacro %}
