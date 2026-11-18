select *
from {{ ref('stg_water_connection') }}
where install_date > current_date