

with src as (
    select 
        "cst_id" as cst_id,
        "cst_key" as cst_key,
        "cst_firstname" as cst_firstname,
        "cst_lastname" as cst_lastname,
        "cst_marital_status" as cst_marital_status,
        "cst_gndr" as cst_gndr,
        "cst_create_date" as cst_create_date 
    from {{ref('bronze_crm_cust_info')}}
    where "cst_id" is not null 

),


transform as (
    select 
        cst_id,
        cst_key,
        trim(cst_firstname) as cst_firstname,
        trim(cst_lastname) as cst_lastname,
        case when upper(trim(cst_marital_status)) = 'M' then 'Married'
            when upper(trim(cst_marital_status)) = 'S' then 'Single'   
            else 'n/a' 
        end cst_marital_status,
        case when upper(trim(cst_gndr)) = 'M' then 'Male'
            when upper(trim(cst_gndr)) = 'F' then 'Female'
            else 'n/a'
        end cst_gndr,
        cst_create_date,
        row_number() over( partition by cst_id order by cst_create_date desc) as rn
    from src  
)

select
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date,
    current_timestamp() as dwh_create_ts
from
    transform
where 
    rn=1