select *
from {{ ref('dim_trade') }} 
where is_current = true and end_timestamp != '9999-12-31 23:59:59.999'