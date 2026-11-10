with days as (
    select cast(day_ts as date) as date_day
    from range(timestamp '2024-01-01', timestamp '2028-01-01', interval 1 day) as t (day_ts)
)

select
    {{ date_key('date_day') }} as date_key,
    date_day,
    year(date_day) as year_number,
    quarter(date_day) as quarter_number,
    month(date_day) as month_number,
    monthname(date_day) as month_name,
    strftime(date_day, '%Y-%m') as year_month,
    weekofyear(date_day) as week_of_year,
    dayname(date_day) as day_name,
    isodow(date_day) as iso_day_of_week,
    isodow(date_day) in (6, 7) as is_weekend
from days
