with src as(
    select
        "sls_ord_num" as sls_ord_num,
        "sls_prd_key" as sls_prd_key,
        "sls_cust_id" as sls_cust_id,
        "sls_order_dt" as sls_order_dt,
        "sls_ship_dt" as sls_ship_dt,
        "sls_due_dt" as sls_due_dt,
        "sls_sales" as sls_sales,
        "sls_quantity" as sls_quantity,
        "sls_price" as sls_price

    from
        {{ ref("bronze_crm_sales_details")}}
)


select 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case when sls_order_dt = 0 or length(to_varchar(sls_order_dt))!= 8 then null
    else try_to_date((to_varchar(sls_order_dt)),'YYYYMMDD')
    end as sls_order_dt,
    case when sls_ship_dt = 0 or length(to_varchar(sls_ship_dt))!= 8 then null
    else try_to_date((to_varchar(sls_ship_dt)),'YYYYMMDD')
    end as sls_ship_dt,
    case when sls_due_dt = 0 or length(to_varchar(sls_due_dt))!= 8 then null
    else try_to_date((to_varchar(sls_due_dt)),'YYYYMMDD')
    end as sls_due_dt,
    case when sls_sales <= 0 or sls_sales is null or sls_sales != abs(sls_quantity*sls_price)
    then abs(sls_quantity*sls_price)
    else sls_sales
    end as sls_sales,
    sls_quantity,
    case when sls_price is null or sls_price <= 0 
    then round((sls_sales/nullif(sls_quantity,0)),0)
    else sls_price
    end as sls_price,
    current_timestamp() as dwh_create_ts

from
    src