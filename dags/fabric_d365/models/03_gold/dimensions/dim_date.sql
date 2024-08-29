{{
    config(
        materialized = "table"
    )
}}

/* generating dates using the macro from the dbt-utils package */
with 
    dates_raw as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('1970-01-01' as date)",
        end_date="date_add(current_date(), interval 100 year)"
        )
    }}
)

select * from dates_raw