with raw_source as (

    select
        *
    from {{ source('raw', '_airbyte_raw_ba_airtravel') }}

),

final as (

    select
        _airbyte_data:" "1958""::varchar as _"1958",
        _airbyte_data:" "1959""::varchar as _"1959",
        _airbyte_data:" "1960""::varchar as _"1960",
        _airbyte_data:"Month"::varchar as month,
        "_AIRBYTE_AB_ID" as _airbyte_ab_id,
        "_AIRBYTE_EMITTED_AT" as _airbyte_emitted_at

    from raw_source

)

select * from final

