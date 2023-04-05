with 

source as (

    select * from {% raw %}{{{% endraw %} source('{{ relation.schema }}', '{{ relation.name }}') {% raw %}}}{% endraw %}

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
        {{ key }}.{{ col }}{% if not loop.last or columns %},{% endif %}
  {%- endfor %}
{%- endfor %}
{%- for col in columns %}
        {{ col['name'] }}{% if not loop.last %},{% endif %}
{%- endfor %}
{%- endif %}

    from source

)

select * from final
