{% macro ep_curve_from_ylt(ylt_ref, loss_column, n_simulations, key_column) %}

/*
    EP (Exceedance Probability) curve calculator for Year Loss Tables (YLTs).

    Produces AEP, OEP, and AAL for each unique value of key_column.

    Parameters:
        ylt_ref         - ref() or source() expression for the YLT model
        loss_column     - name of the loss column (string, e.g. 'loss')
        n_simulations   - number of simulated years (integer)
        key_column      - name of the pre-computed aggregation key column.
                          This must be a hash/surrogate that identifies a unique
                          set of dimensions WITHOUT year_id or event_id, so that
                          grouping by it produces one row per simulated year.
                          e.g. 'aggregation_key' from stg_risklink__ylts.

    Output columns:
        key_column      - the aggregation key passed through
        ep_type         - 'AEP', 'OEP', or 'AAL'
        return_period   - return period in years (0 for AAL rows)
        rank_num        - rank of the loss (0 for AAL rows)
        annual_loss     - the loss value at that return period / the AAL
*/

    {% set n = n_simulations | int %}

    with target_return_periods as (
        -- Return periods we want to report. Stored as a table so the join
        -- below filters ranked results to only these return periods.
        select unnest([1000, 500, 250, 200, 150, 100, 50, 30, 20, 10, 5]) as return_period
    )

    -- AEP: sum all losses within a simulated year
    , annual_losses as (
        select
            {{ key_column }},
            year_id,
            sum({{ loss_column }}) as annual_loss
        from {{ ylt_ref }}
        group by {{ key_column }}, year_id
    )

    -- OEP: max single-event loss within a simulated year
    , max_losses as (
        select
            {{ key_column }},
            year_id,
            max({{ loss_column }}) as annual_loss
        from {{ ylt_ref }}
        group by {{ key_column }}, year_id
    )

    -- Tag each set with the EP type before ranking
    , combined_losses as (
        select 'AEP' as ep_type, * from annual_losses
        union all
        select 'OEP' as ep_type, * from max_losses
    )

    -- Rank losses descending within each (key, ep_type) partition
    , ranked_losses as (
        select
            {{ key_column }},
            ep_type,
            annual_loss,
            row_number() over (
                partition by {{ key_column }}, ep_type
                order by annual_loss desc
            ) as rank_num
        from combined_losses
    )

    -- Convert rank to return period and keep only the target return periods
    , ep_curve as (
        select
            r.{{ key_column }},
            r.ep_type,
            rp.return_period,
            r.rank_num,
            r.annual_loss
        from ranked_losses r
        inner join target_return_periods rp
            on rp.return_period = floor({{ n }}::float / r.rank_num)
    )

    -- AAL: average annual loss = sum of all losses / n_simulations
    -- Group only by key_column (not year_id) — one AAL row per key
    , aal as (
        select
            {{ key_column }},
            'AAL'               as ep_type,
            0                   as return_period,
            0                   as rank_num,
            sum({{ loss_column }}) / {{ n }}::float as annual_loss
        from {{ ylt_ref }}
        group by {{ key_column }}
    )

    select {{ key_column }}, ep_type, return_period, rank_num, annual_loss from ep_curve
    union all
    select {{ key_column }}, ep_type, return_period, rank_num, annual_loss from aal

{% endmacro %}
