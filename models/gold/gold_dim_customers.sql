select 
    row_number() over(order by c.cst_id) as customer_key,
    c.cst_id as customer_id,
    c.cst_key as customer_number,
    c.cst_firstname as first_name,
    c.cst_lastname as last_name,
    c2.cntry as country,
    c.cst_marital_status as marital_status,
    case when c.cst_gndr!= 'n/a' then c.cst_gndr
    else coalesce(c1.gen, 'n/a')
    end as gender,
    c1.bdate as birth_date,
    c.cst_create_date as create_date 
from
    {{ref("silver_crm_cust_info")}} c
left join {{ref("silver_erp_cust_az12")}} c1
on c.cst_key = c1.cid
left join {{ref("silver_erp_loc_a101")}} c2
on c.cst_key = c2.cid
