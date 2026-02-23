select 
    row_number() over(order by p.prd_start_dt, p.prd_key) as product_key,
    p.prd_id as product_id,
    p.prd_key as product_number,
    p.prd_nm as product_name,
    p.cat_id as category_id,
    p1.cat as category,
    p1.subcat as subcategory,
    p1.maintenance,
    p.prd_cost as cost,
    p.prd_line as product_line,
    p.prd_start_dt as start_date
from
    {{ref("silver_crm_prd_info")}} p
left join 
    {{ref("silver_erp_px_cat_g1v2")}} p1
on p.cat_id = p1.id 
where prd_end_dt is null