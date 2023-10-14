{{
    config(
        unique_key='id'
    )
}}

select * from {{ ref('code_snapshot') }}
