with raw_source as (

    select * from {% raw %}{{{% endraw %} source('{{ relation.schema.lower() }}', '{{ relation.name.lower() }}') {% raw %}}}{% endraw %}

),

final as (

    select
{%- for col in columns %}
        {{ col.name.lower() }}{% if not loop.last or nested %},{% endif %}
{%- endfor %}
{%- if adapter_name == 'SnowflakeAdapter' %}
{%- for key, cols in nested.items() %}
  {%- for col in cols %}
        {{ key }}:{{ col.lower() }}::varchar as {{ col.lower() }}{% if not loop.last %},{% endif %}
  {%- endfor %}
{%- endfor %}
{%- elif adapter_name == 'BigQueryAdapter' %}
{%- for key, cols in nested.items() %}
  {%- for col in cols %}
        cast({{ key }}.{{ col.lower() }} as string) as {{ col.lower() }}{% if not loop.last %},{% endif %}
  {%- endfor %}
{%- endfor %}
{%- elif adapter_name == 'RedshiftAdapter' %}
{%- for key, cols in nested.items() %}
  {%- for col in cols %}
        {{ key }}.{{ col.lower() }}::varchar as {{ col.lower() }}{% if not loop.last %},{% endif %}
  {%- endfor %}
{%- endfor %}
{%- endif %}

    from raw_source

)

select * from final
