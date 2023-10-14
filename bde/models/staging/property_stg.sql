{{
    config(
        unique_key='id'
    )
}}

with

source  as (

    select * from {{ ref('property_snapshot') }}

),

todate as (
    select
        id,
        TO_DATE(scraped_date, 'YYYY-MM-DD') AS scraped_date, 
        property_type,  
        room_type,   
        TO_DATE(dbt_updated_at, 'YYYY-MM-DD') AS dbt_updated_at,
        TO_DATE(dbt_valid_from, 'YYYY-MM-DD') AS dbt_valid_from,
        dbt_valid_to
    from source
)


select * from todate
