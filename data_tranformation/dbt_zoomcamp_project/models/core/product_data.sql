{{ config(materialized='table') }}

select 
    product_id
from {{ ref('product_lookup') }}