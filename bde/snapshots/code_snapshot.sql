{% snapshot code_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='check',
          unique_key='id',
          check_cols=['id','lga_code','lga_name']
        )
    }}

select * from {{ source('raw', 'nsw_lga_code') }}

{% endsnapshot %}