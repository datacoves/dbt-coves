with raw_source as (

    select * from {% raw %}{{{% endraw %} source('{{ relation.schema.lower() }}', '{{ relation.name.lower() }}') {% raw %}}}{% endraw %}

),

final as (

    select
{%- for col in columns %}
        {{ col.name.lower() }}{% if not loop.last %},{% endif %}
{%- endfor %}
{%- for key, cols in variants.items() %}
  {%- for col in cols %}
        {{ key }}:{{ col }}::varchar as {{ col }}{% if not loop.last %},{% endif %}
  {%- endfor %}
{%- endfor %}

    from raw_source

)

select * from final