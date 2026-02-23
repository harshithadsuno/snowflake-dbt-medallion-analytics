
select
    case when cid like '%NAS%' then substring(cid, 4,length(cid))
    else cid
    end as cid,
    case when bdate > current_date() then null
    else bdate
    end as bdate,
    case when upper(trim(gen)) in ('M', 'MALE') then 'Male'
    when upper(trim(gen)) in ('F','FEMALE') then 'Female'
    else 'n/a'
    end as gen,
    current_timestamp() as dwh_create_ts
from
    {{ref("bronze_erp_CUST_AZ12")}}
