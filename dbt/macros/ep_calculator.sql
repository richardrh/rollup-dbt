{% macro ep_curve_from_ylt(ylt_ref, loss_column, n_simulations, key_columns) %}

    {% set n = n_simulations | int %}

    {% if 'yearid' not in key_columns | lower %}
        {{ exceptions.raise_compiler_error("key_columns must include 'yearid'") }}
    {% endif %}


    with return_periods_to_select as (
        select
            [1000, 500, 250, 200, 150, 100, 50, 30, 20, 10, 5] as return_period
        )

    ,  annual_losses as (
        select
            {{ key_columns }},
            sum({{ loss_column }}) as annual_loss
        from {{ ylt_ref }}
        group by {{ key_columns }}
    ),

    , max_losses as (
        select
            {{ key_columns }},
            sum({{ loss_column }}) as annual_loss
        from {{ ylt_ref }}
        group by {{ key_columns }}
    )

    , ranked_annual_losses as (
        select
            *,
            row_number() over (
                order by annual_loss desc
            ) as rank_num
        from annual_losses
    )

    , ranked_max_losses as (
        select
            *,
            row_number() over (
                order by annual_loss desc
            ) as rank_num
        from annual_losses
    )


    select
        *,
        rank_num::float / {{ n }} as ep_probability,
        case
            when rank_num::float / {{ n }} > 0
            then 1 / (rank_num::float / {{ n }})
        end as return_period
    from ranked_losses
    where rank_num <= {{ n }}
    order by annual_loss desc

{% endmacro %}
