{% snapshot customer_snapshot %}

    {{
        config(
          target_schema='consumption',
          strategy='check',
          unique_key='c_custkey',
          check_cols=['c_name'],
        )
    }}

    select c_custkey, c_name
    from {{ source('raw_schema','customer') }}

{% endsnapshot %}