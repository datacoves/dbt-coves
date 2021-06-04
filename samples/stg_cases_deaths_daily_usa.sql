with raw_source as (

    select * from {{ source('cdc_covid', 'cases_deaths_daily_usa_data') }}

),

final as (

    select
        raw_source._airbyte_ab_id,
        raw_source._airbyte_emitted_at,
        raw_source._airbyte_data:consent_cases::varchar as consent_cases,
        raw_source._airbyte_data:consent_deaths::varchar as consent_deaths,
        raw_source._airbyte_data:new_case::varchar as new_case,
        raw_source._airbyte_data:new_death::varchar as new_death,
        raw_source._airbyte_data:pnew_case::varchar as pnew_case,
        raw_source._airbyte_data:pnew_death::varchar as pnew_death,
        raw_source._airbyte_data:state::varchar as state,
        raw_source._airbyte_data:tot_cases::varchar as tot_cases,
        raw_source._airbyte_data:tot_death::varchar as tot_death,
        raw_source._airbyte_data:submission_date::timestamp_ntz as submission_date,
        raw_source._airbyte_data:created_at::timestamp_ntz as created_at

    from raw_source

)

select * from final
