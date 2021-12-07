with raw_source as (

    select
        *
    from {{ source('raw', '_airbyte_raw_ba_addresses') }}

),

final as (

    select
        _airbyte_data:" 08075"::varchar as _08075,
        _airbyte_data:" NJ"::varchar as _nj,
        _airbyte_data:"120 jefferson st."::varchar as 120_jefferson_st.,
        _airbyte_data:"Doe"::varchar as doe,
        _airbyte_data:"John"::varchar as john,
        _airbyte_data:"Riverside"::varchar as riverside,
        "_AIRBYTE_AB_ID" as _airbyte_ab_id,
        "_AIRBYTE_EMITTED_AT" as _airbyte_emitted_at

    from raw_source

)

select * from final

