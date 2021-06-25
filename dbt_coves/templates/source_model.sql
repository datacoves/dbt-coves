with raw_source as (

    select * from {% raw %}{{{% endraw %} source('{{ relation.schema.lower() }}', '{{ relation.name.lower() }}') {% raw %}}}{% endraw %}

),

final as (

    select
{%- for col in columns %}
        {{ col.name.lower() }},
{%- endfor %}
{%- for key, cols in variants.items() %}
  {%- for col in cols %}
        {{ key }}:{{ col.lower() }}::varchar as {{ col.lower() }}{% if not loop.last %},{% endif %}
  {%- endfor %}
{%- endfor %}

    from raw_source

)

select * from final
