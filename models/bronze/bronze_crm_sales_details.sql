select
    *
from 
    {{ source("raw_crm", 'RAW_CRM_SALES_DETAILS')}}
    