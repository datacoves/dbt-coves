with raw_source as (

    select *
    from {% raw %}{{{% endraw %} source('{{ relation.schema }}', '{{ relation.name }}') {% raw %}}}{% endraw %}

),

final as (

    select
{%- if adapter_name == 'SnowflakeAdapter' or adapter_name == 'RedshiftAdapter' %}
{%- for key, cols in nested.items() %}
  {%- for col in cols %}
        {{ key }}:{{ '"' + col + '"' }}::{{ cols[col]["type"].lower() }} as {{ cols[col]["id"] }}{% if not loop.last or columns %},{% endif %}
  {%- endfor %}
{%- endfor %}
{%- for col in columns %}
        {{ '"' + col['name'] + '"' }}::{{ col["type"].lower() }} as {{ col['id'] }}{% if not loop.last %},{% endif %}
{%- endfor %}
{%- elif adapter_name == 'BigQueryAdapter' %}
{%- for key, cols in nested.items() %}
  {%- for col in cols %}
        cast({{ key }}.{{ col }} as {{ cols[col]["type"].lower().replace("varchar", "string") }}) as {{ cols[col]["id"] }}{% if not loop.last or columns %},{% endif %}
  {%- endfor %}
{%- endfor %}
{%- for col in columns %}
        cast({{ col['name'] }} as {{ col["type"].lower().replace("varchar", "string") }}) as {{ col['id'] }}{% if not loop.last %},{% endif %}
{%- endfor %}
{%- endif %}

    from raw_source

)

select * from final
