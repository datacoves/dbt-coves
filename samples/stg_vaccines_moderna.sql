with raw_source as (

    select * from {{ source('cdc_covid', 'vaccines_moderna_data') }}

),

final as (

    select
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_data:_1st_dose_allocations::integer as first_dose_allocations,
        _airbyte_data:_2nd_dose_allocations::integer as second_dose_allocations,
        _airbyte_data:jurisdiction::varchar as jurisdiction,
        _airbyte_data:week_of_allocations::timestamp_ntz as week_of_allocations

    from raw_source

)

select * from final
