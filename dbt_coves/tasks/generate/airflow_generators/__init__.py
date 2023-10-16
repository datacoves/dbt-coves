# Prefetch classes so that YML DAGs don't have to write "{module}.{class}" as Generators
from .airbyte import AirbyteDbtGenerator, AirbyteGenerator  # noqa
from .fivetran import FivetranDbtGenerator, FivetranGenerator  # noqa
