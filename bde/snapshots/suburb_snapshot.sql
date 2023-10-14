{% snapshot suburb_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='check',
          unique_key='id',
          check_cols=['id','lga_name','suburb_name']
        )
    }}

select * from {{ source('raw', 'nsw_lga_suburb') }}

{% endsnapshot %}