{% snapshot property_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='id',
          updated_at='scraped_date'
        )
    }}

select id, scraped_date, property_type, room_type from {{ source('raw', 'house') }}

{% endsnapshot %}