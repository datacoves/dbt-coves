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
      {%- if with_metadata == 'yes' %}
        {{ key }}:{{ '"' + col.name + '"' }}::{{ '"' + col.dtype + '"' }} as {{ col.name.lower().replace(" ","_").replace(":","_").replace("(","_").replace(")","_") }}{% if not loop.last or columns %},{% endif %}
      {%- else  %} 
        {{ key }}:{{ '"' + col + '"' }}:: varchar as {{ col.lower().replace(" ","_").replace(":","_").replace("(","_").replace(")","_") }}{% if not loop.last or columns %},{% endif %}
      {%- endif %}
    {%- endfor %}
  {%- endfor %}
{%- endif %}

{%- if adapter_name == 'PostgresAdapter' %}
  {%- for key, cols in nested.items() %}
    {%- for col in cols %}
      {%- if with_metadata == 'yes' %}
        {{ key }}:{{ '"' + col.name.lower() + '"' }}::{{ '"' + col.dtype + '"' }} as {{ col.name.lower().replace(" ","_").replace(":","_").replace("(","_").replace(")","_") }}{% if not loop.last or columns %},{% endif %}
      {%- else  %}
        {{ key }}:{{ '"' + col.lower() + '"' }}:: varchar as {{ col.lower().replace(" ","_").replace(":","_").replace("(","_").replace(")","_") }}{% if not loop.last or columns %},{% endif %}
      {%- endif %}
    {%- endfor %}
  {%- endfor %}
{%- endif %}

{%- if adapter_name == 'BigQueryAdapter' %}
  {%- for key, cols in nested.items() %}
    {%- for col in cols %}
      cast({{ key }}.{{ col }} as string) as {{ col.lower().replace(" ","_").replace(":","_").replace("(","_").replace(")","_") }}{% if not loop.last or columns %},{% endif %}
    {%- endfor %}
  {%- endfor %}
{%- endif %}

{%- if adapter_name == 'RedshiftAdapter' %}
  {%- for key, cols in nested.items() %}
    {%- for col in cols %}
      {{ key }}.{{ col }}::varchar as {{ col.lower().replace(" ","_").replace(":","_").replace("(","_").replace(")","_") }}{% if not loop.last or columns %},{% endif %}
    {%- endfor %}
  {%- endfor %}
{%- endif %}

{%- for col in columns %}
        {{ '"' + col.name.lower() + '"' }} as {{ col.name.lower() }}{% if not loop.last %},{% endif %}
{%- endfor %}

    from raw_source

)

select * from final

