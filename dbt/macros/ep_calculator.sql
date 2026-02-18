{% macro ep_curve_from_ylt(ylt_ref, loss_column, n_simulations, key_columns) %}


    {% set n = n_simulations | int %}

    {% if 'yearid' not in key_columns | lower %}
        {{ exceptions.raise_compiler_error("key_columns must include 'yearid'") }}
    {% endif %}



    with return_periods_to_select as (
        select
            [1000, 500, 250, 200, 150, 100, 50, 30, 20, 10, 5] as return_period
        )

    -- annual losses summed up by key columns
    ,  annual_losses as (
        select
            {{ key_columns }},
            sum({{ loss_column }}) as annual_loss
        from {{ ylt_ref }}
        group by {{ key_columns }}
    ),


    -- max loss is for oep
    , max_losses as (
        select
            {{ key_columns }},
            max({{ loss_column }}) as annual_loss
        from {{ ylt_ref }}
        group by {{ key_columns }}
    )

    -- Combine them up so subsequent calc happens once on one table only
    , combined_grouped_losses as (

        select
            'AEP' as ep_type,
            *
        from annual_losses

        union all

        select
            'OEP' as ep_type,
        from max_losses
    )

    -- rank the losses based on key columns + the ep_type from above
    , ranked_losses as (
        select
            *,
            row_number() over (
                partition by {{ key_columns }} , ep_type
                order by annual_loss desc
            ) as rank_num
        from annual_losses
    )

    -- do a little RP conversion and filter for the ranks we're interested in
    , ranked_losses_with_return_period as (

        select
            {{ n }} / rank_num::float as return_period
            , *
        from ranked_losses
        inner join return_periods_to_select select on select.return_period =
            ranked_losses.return_period

    )

    -- Finally supplement it with the AAL
    , aal_table as (
        select
            'AAL' as ep_type,
             0 as return_period,
             0 as rank_num,
    -- TODO: We need to get rid of the yearid from key_columns to do
    -- average annual loss which is just the sum of all losses / n_simulations
    -- so we do not want to group by yearid
             {{ key_columns }}
)


    select * from ranked_losses_with_return_period


{% endmacro %}
