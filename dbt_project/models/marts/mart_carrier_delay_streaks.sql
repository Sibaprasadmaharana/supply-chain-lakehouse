-- Gaps-and-islands: consecutive calendar days on which a carrier's on-time rate fell below the threshold.
with daily as (
    select
        carrier_id,
        cast(delivered_ts as date) as delivery_date,
        count(*) as deliveries,
        avg(case when is_on_time then 1.0 else 0.0 end) as on_time_rate
    from {{ ref('fact_shipments') }}
    where is_delivered
    group by carrier_id, cast(delivered_ts as date)
),

bad_days as (
    select *
    from daily
    where on_time_rate < {{ var('streak_on_time_threshold') }}
),

islands as (
    select
        *,
        -- consecutive dates minus their row number give the same anchor date -> same island
        delivery_date - cast(row_number() over (partition by carrier_id order by delivery_date) as integer) as island_anchor
    from bad_days
)

select
    i.carrier_id,
    c.carrier_name,
    min(i.delivery_date) as streak_start_date,
    max(i.delivery_date) as streak_end_date,
    count(*) as streak_days,
    sum(i.deliveries) as deliveries_in_streak,
    round(avg(i.on_time_rate), 4) as avg_on_time_rate
from islands as i
inner join {{ ref('dim_carrier') }} as c
    on i.carrier_id = c.carrier_id
group by i.carrier_id, c.carrier_name, i.island_anchor
having count(*) >= 2
