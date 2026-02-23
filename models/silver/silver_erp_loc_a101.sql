select 
    replace(cid, '-', '') as cid,
    case when upper(trim(cntry)) in ('USA', 'US') then 'United States'
    when upper(trim(cntry)) = 'DE' then 'Germany'
    when trim(cntry) is null or cntry = '' then 'n/a'
    else trim(cntry)
    end as cntry,
    current_timestamp() as dwh_create_ts
from
    {{ref("bronze_erp_LOC_A101")}}