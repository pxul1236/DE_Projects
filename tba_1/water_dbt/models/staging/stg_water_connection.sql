with latest as (
    select *
    from {{ source('raw', 'water_service_connection') }}
    where load_id = (select max(load_id) from {{ source('raw', 'load_log') }})
)

select
    sapobjectid,
    nullif(nullif(upper(trim(water_pipe_lining)), ''), 'TBA') as pipe_lining,
    nullif(nullif(trim(water_srv_connectn_class), ''), 'TBA') as connection_class
from latest