select *
from {{ ref('dim_broker') }} 
where first_name is null and last_name is null