with latest as (
    select *
    from {{ source('raw', 'water_service_connection') }}
    where load_id = (select max(load_id) from {{ source('raw', 'load_log') }})
),

typed as (
    select
        sapobjectid                                                          as connection_id,
        nullif(nullif(upper(trim(water_srv_connectn_material)), ''), 'TBA')  as material,
        nullif(nullif(upper(trim(water_srv_connectn_class)), ''), 'TBA')     as connection_class,
        nullif(nullif(upper(trim(water_pipe_lining)), ''), 'TBA')            as pipe_lining,
        nullif(nullif(upper(trim(water_pipe_protection)), ''), 'TBA')        as pipe_protection,
        upper(trim(water_srv_connectn_use))                                  as connection_use,
        upper(trim(water_category))                                          as water_category,
        nullif(trim(functional_loc), '')                                     as water_zone_code,
        nullif(trim(diameter_mm), '')::numeric                               as diameter_mm,
        nullif(trim(length_m), '')::numeric                                  as length_m,
        to_date(split_part(nullif(trim(gis_startup_date), ''), ' ', 1), 'MM/DD/YYYY') as install_date_recorded,
        load_id
    from latest
)

select
    connection_id,
    material,
    connection_class,
    pipe_lining,
    pipe_protection,
    connection_use,
    water_category,
    water_zone_code,
    diameter_mm,
    length_m,
    case
        when install_date_recorded < date '1900-01-01' then null
        when install_date_recorded > current_date      then null
        else install_date_recorded
    end                                                                      as install_date,
    coalesce(
        install_date_recorded < date '1900-01-01'
        or install_date_recorded > current_date,
        false
    )                                                                        as install_date_suspect,
    load_id
from typed