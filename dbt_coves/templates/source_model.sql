with raw_source as (

    select
        *
    from {% raw %}{{{% endraw %} source('{{ relation.schema.lower() }}', '{{ relation.name.lower() }}') {% raw %}}}{% endraw %}

),

final as (

    select
{%- if adapter_name == 'SnowflakeAdapter' %}
{%- for key, cols in nested.items() %}
  {%- for col in cols %}
        {{ key }}:{{ '"' + col + '"' }}::varchar as {{ col.lower().replace(" ","_").replace(":","_").replace("(","_").replace(")","_").replace("-","_").replace("/","_") }}{% if not loop.last or columns %},{% endif %}
  {%- endfor %}
{%- endfor %}
{%- elif adapter_name == 'BigQueryAdapter' %}
{%- for key, cols in nested.items() %}
  {%- for col in cols %}
        cast({{ key }}.{{ col }} as string) as {{ col.lower().replace(" ","_").replace(":","_").replace("(","_").replace(")","_").replace("-","_").replace("/","_") }}{% if not loop.last or columns %},{% endif %}
  {%- endfor %}
{%- endfor %}
{%- elif adapter_name == 'RedshiftAdapter' %}
{%- for key, cols in nested.items() %}
  {%- for col in cols %}
        {{ key }}.{{ col }}::varchar as {{ col.lower().replace(" ","_").replace(":","_").replace("(","_").replace(")","_").replace("-","_").replace("/","_") }}{% if not loop.last or columns %},{% endif %}
  {%- endfor %}
{%- endfor %}
{%- endif %}
{%- for col in columns %}
        {{ '"' + col.name + '"' }} as {{ col.name.lower() }}{% if not loop.last %},{% endif %}
{%- endfor %}

    from raw_source

)

select * from final

