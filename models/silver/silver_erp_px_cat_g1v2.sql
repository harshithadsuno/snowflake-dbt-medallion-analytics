select
    id,
    cat,
    subcat,
    maintenance,
    current_timestamp() as dwh_create_ts
from
    {{ref("bronze_erp_PX_CAT_G1V2")}}