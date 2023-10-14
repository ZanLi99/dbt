{{
    config(
        unique_key='id'
    )
}}

select * from {{ ref('suburb_snapshot') }}
