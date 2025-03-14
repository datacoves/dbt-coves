"""SQL database source helpers"""

import operator
from typing import Any, Callable, Iterator, List, Optional, Union

import dlt
from dlt.common.configuration.specs import BaseConfiguration, configspec
from dlt.common.typing import TDataItem
from dlt.sources.credentials import ConnectionStringCredentials
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .schema_types import SelectAny, Table, table_to_columns


class TableLoader:
    def __init__(
        self,
        engine: Engine,
        table: Table,
        chunk_size: int = 1000,
        incremental: Optional[dlt.sources.incremental[Any]] = None,
    ) -> None:
        self.engine = engine
        self.table = table
        self.chunk_size = chunk_size
        self.incremental = incremental
        if incremental:
            try:
                self.cursor_column = table.c[incremental.cursor_path]
            except KeyError as e:
                raise KeyError(
                    f"Cursor column '{incremental.cursor_path}' does not exist in table '{table.name}'"
                ) from e
            self.last_value = incremental.last_value
            self.end_value = incremental.end_value
            self.row_order = getattr(self.incremental, "row_order", None)
        else:
            self.cursor_column = None
            self.last_value = None
            self.end_value = None
            self.row_order = None

    def make_query(self) -> SelectAny:
        table = self.table
        query = table.select()
        if not self.incremental:
            return query
        last_value_func = self.incremental.last_value_func

        # generate where
        if last_value_func is max:  # Query ordered and filtered according to last_value function
            filter_op = operator.ge
            filter_op_end = operator.lt
        elif last_value_func is min:
            filter_op = operator.le
            filter_op_end = operator.gt
        else:  # Custom last_value, load everything and let incremental handle filtering
            return query

        if self.last_value is not None:
            query = query.where(filter_op(self.cursor_column, self.last_value))
            if self.end_value is not None:
                query = query.where(filter_op_end(self.cursor_column, self.end_value))

        # generate order by from declared row order
        order_by = None
        if self.row_order == "asc":
            order_by = self.cursor_column.asc()
        elif self.row_order == "desc":
            order_by = self.cursor_column.desc()
        if order_by is not None:
            query = query.order_by(order_by)

        return query

    def load_rows(self) -> Iterator[List[TDataItem]]:
        query = self.make_query()
        with self.engine.connect() as conn:
            result = conn.execution_options(yield_per=self.chunk_size).execute(query)
            for partition in result.partitions(size=self.chunk_size):
                yield [dict(row._mapping) for row in partition]


def table_rows(
    engine: Engine,
    table: Table,
    chunk_size: int,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
    detect_precision_hints: bool = False,
    defer_table_reflect: bool = False,
    table_adapter_callback: Callable[[Table], None] = None,
) -> Iterator[TDataItem]:
    if defer_table_reflect:
        table = Table(table.name, table.metadata, autoload_with=engine, extend_existing=True)
        if table_adapter_callback:
            table_adapter_callback(table)
        # set the primary_key in the incremental
        if incremental and incremental.primary_key is None:
            primary_key = get_primary_key(table)
            if primary_key is not None:
                incremental.primary_key = primary_key
        # yield empty record to set hints
        yield dlt.mark.with_hints(
            [],
            dlt.mark.make_hints(
                primary_key=get_primary_key(table),
                columns=table_to_columns(table) if detect_precision_hints else None,
            ),
        )

    loader = TableLoader(engine, table, incremental=incremental, chunk_size=chunk_size)
    yield from loader.load_rows()


def engine_from_credentials(credentials: Union[ConnectionStringCredentials, Engine, str]) -> Engine:
    if isinstance(credentials, Engine):
        return credentials
    if isinstance(credentials, ConnectionStringCredentials):
        credentials = credentials.to_native_representation()
    return create_engine(credentials)


def get_primary_key(table: Table) -> List[str]:
    """Create primary key or return None if no key defined"""
    primary_key = [c.name for c in table.primary_key]
    return primary_key if len(primary_key) > 0 else None


@configspec
class SqlDatabaseTableConfiguration(BaseConfiguration):
    incremental: Optional[dlt.sources.incremental] = None  # type: ignore[type-arg]


@configspec
class SqlTableResourceConfiguration(BaseConfiguration):
    credentials: ConnectionStringCredentials = (None,)
    table: str = (None,)
    incremental: Optional[dlt.sources.incremental] = None  # type: ignore[type-arg]
    schema: Optional[str] = None


__source_name__ = "sql_database"
