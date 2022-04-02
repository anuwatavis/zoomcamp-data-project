{{ config(materialized='view') }}

select 
    product_id,
    product_name,
    product_group,
    recored_date,
    product_category,
    high_price,
    min_price,
    high_price - min_price as `range_price`,
    CASE
        WHEN product_category = 'ขายปลีก' THEN 'retail'
        ELSE 'wholesale'
    END AS product_category_en
from {{source('staging', 'product_price')}}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 200

{% endif %}