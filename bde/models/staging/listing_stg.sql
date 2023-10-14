{{
    config(
        unique_key='id'
    )
}}

with

source  as (

    select * from {{ ref('listing_snapshot') }}

),

todate as (
    select
        id,
        listing_id,
        scrape_id,
        TO_DATE(scraped_date, 'YYYY-MM-DD') AS scraped_date,
        listing_neighbourhood,
        accommodates,
        price,
        has_availability, 
        availability_30, 
        number_of_reviews,
        REVIEW_SCORES_RATING,
        REVIEW_SCORES_ACCURACY,
        REVIEW_SCORES_CLEANLINESS,
        REVIEW_SCORES_CHECKIN,
        REVIEW_SCORES_COMMUNICATION,
        REVIEW_SCORES_VALUE,
        TO_DATE(dbt_updated_at, 'YYYY-MM-DD') AS dbt_updated_at,
        TO_DATE(dbt_valid_from, 'YYYY-MM-DD') AS dbt_valid_from,
        dbt_valid_to
    from source
    where price > 5
)


select * from todate
