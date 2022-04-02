{{ config(materialized='table') }}

with price_data as (
    select *
    from {{ ref('stg_product_price') }}
), 

product_data as (
    select * from {{ ref('product_lookup') }}
)

select 
    price_data.product_id,
    price_data.product_name,
    price_data.product_group,
    price_data.recored_date,
    price_data.product_category as product_category_th,
    price_data.product_category_en,
    price_data.high_price,
    price_data.min_price,
    price_data.range_price,
    product_lookup.product_name_th,
    product_lookup.product_name_en,
    product_lookup.product_group_en,
    product_lookup.product_group_th
from price_data 
inner join product_data as product_lookup
on price_data.product_id = product_lookup.product_id

