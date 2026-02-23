{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_line_key'
) }}

with base as (
    select
        s.sls_ord_num as order_number,
        p.product_key,
        c.customer_key,
        s.sls_order_dt as order_date,
        s.sls_ship_dt as shipping_date,
        s.sls_due_dt as due_date,
        s.sls_sales as sales_amount,
        s.sls_quantity as quantity,
        s.sls_price as price,

        -- composite grain key: (order_number, product_key)
        md5(to_varchar(s.sls_ord_num) || '|' || to_varchar(p.product_key)) as order_line_key

    from {{ ref('silver_crm_sales_details') }} s
    left join {{ ref('gold_dim_products') }} p
        on s.sls_prd_key = p.product_number
    left join {{ ref('gold_dim_customers') }} c
        on s.sls_cust_id = c.customer_id
)

select *
from base
