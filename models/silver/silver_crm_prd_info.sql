
with src as(

    select 
        "prd_id" as prd_id,
        "prd_key" as prd_key,
        "prd_nm" as prd_nm,
        "prd_cost" as prd_cost,
        "prd_line" as prd_line,
        "prd_start_dt" as prd_start_dt,
        "prd_end_dt" as prd_end_dt
    from
        {{ ref('bronze_crm_prd_info')}}

)


select
    prd_id,
    replace(substring(prd_key, 1,5), '-','_') as cat_id,
    substring(prd_key, 7,length(prd_key)) as prd_key,
    prd_nm,
    coalesce(prd_cost, 0) as prd_cost,
    case when upper(trim(prd_line)) = 'M' then 'Mountain'
    when upper(trim(prd_line)) = 'R' then 'Road'
    when upper(trim(prd_line)) = 'T' then 'Touring'
    when upper(trim(prd_line)) = 'S' then 'Other Sales'
    else 'n/a'
    end as prd_line,
    to_date(prd_start_dt) as prd_start_dt,
    to_date((lead(prd_start_dt) over(partition by prd_key order by prd_start_dt))-1) as prd_end_dt,
    current_timestamp() as dwh_create_ts
from
    src
