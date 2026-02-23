select
    *
from 
    {{ source("raw_crm", 'RAW_CRM_CUST_INFO')}}