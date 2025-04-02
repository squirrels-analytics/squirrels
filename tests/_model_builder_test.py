from pathlib import Path
import pytest, asyncio, sqlite3, duckdb, polars as pl

from squirrels._connection_set import ConnectionSet, ConnectionProperties
from squirrels._manifest import ConnectionType
from squirrels._models import SourceModel
from squirrels._model_builder import ModelBuilder
from squirrels._model_configs import ColumnConfig
from squirrels._sources import Source, UpdateHints


@pytest.fixture(scope="module")
def sqlite_path():
    path = Path("playground/sqlite_test.db")
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)

@pytest.fixture(scope="module", autouse=True)
def sqlite_conn(sqlite_path):
    conn = sqlite3.connect(sqlite_path)

    try:
        conn.execute("DROP TABLE IF EXISTS test_table")

        conn.execute("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                value INTEGER
            )
        """)
        
        conn.execute("""
            INSERT INTO test_table (id, name, value)
            VALUES (1, 'foo', 100),
                (2, 'bar', 200),
                (3, 'baz', 300)
        """)
        
        conn.commit()
    finally:
        conn.close()

@pytest.fixture(scope="module")
def connection_set(sqlite_path):
    return ConnectionSet({"test_conn": ConnectionProperties(type=ConnectionType.CONNECTORX, uri=f"sqlite://{sqlite_path}")})


@pytest.fixture(scope="module")
def expected_df0():
    return pl.DataFrame({
        "id": [1, 2, 2, 3],
        "name": ["foo", "bar", "bar", "baz"],
        "value": [0, 100, 200, 300]
    })

@pytest.fixture(scope="module")
def expected_df1():
    return pl.DataFrame({
        "id": [1, 1, 2, 3],
        "name": ["foo", "foo", "bar", "baz"],
        "value": [0, 100, 200, 300]
    })

@pytest.fixture(scope="module")
def expected_df2():
    return pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["foo", "bar", "baz"],
        "value": [0, 200, 300]
    })

@pytest.fixture(scope="module")
def expected_df3():
    return pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["foo", "bar", "baz"],
        "value": [100, 200, 300]
    })


@pytest.mark.parametrize("primary_key, strictly_increasing, expected_df_name", [
    ([], True, "expected_df0"),
    ([], False, "expected_df1"),
    (["id"], True, "expected_df2"),
    (["id"], False, "expected_df3"),
])
def test_build_sources_with_increasing_column(request: pytest.FixtureRequest, primary_key: list[str], strictly_increasing: bool, expected_df_name: str):
    connection_set = request.getfixturevalue("connection_set")
    
    source_name = 'test_table'
    source = Source(
        connection='test_conn',
        load_to_duckdb=True,
        primary_key=primary_key,
        update_hints=UpdateHints(increasing_column='value', strictly_increasing=strictly_increasing),
        columns=[
            ColumnConfig(name="id", type="BIGINT"), 
            ColumnConfig(name="name", type="VARCHAR"), 
            ColumnConfig(name="value", type="BIGINT")
        ]
    ).finalize_table(source_name)
    
    source_model = SourceModel(source_name, source, conn_set=connection_set)
    model_builder = ModelBuilder(
        _duckdb_venv_path='dummy_path',
        _conn_set=connection_set,
        _static_models={source_model.name: source_model}
    )
        
    duckdb_conn = duckdb.connect()
    try:
        model_builder._attach_connections(duckdb_conn)

        cols = source.get_cols_for_create_table_stmt()
        duckdb_conn.execute(f"CREATE TABLE test_table ({cols})")
        
        duckdb_conn.execute("""
            INSERT INTO test_table (id, name, value) 
            VALUES (1, 'foo', 0), (2, 'bar', 100)
        """)

        asyncio.run(model_builder._build_models(duckdb_conn, select="test_table", full_refresh=False))
        result = duckdb_conn.table("test_table").pl()
    finally:
        duckdb_conn.close()
    
    expected_df = request.getfixturevalue(expected_df_name)
    assert result.equals(expected_df)


def test_build_sources(connection_set, expected_df3: pl.DataFrame):
    source_name = 'test_table'
    source = Source(
        connection='test_conn',
        columns=[
            ColumnConfig(name="id", type="BIGINT"), 
            ColumnConfig(name="name", type="VARCHAR"), 
            ColumnConfig(name="value", type="BIGINT")
        ],
        load_to_duckdb=True
    ).finalize_table(source_name)

    source_model = SourceModel(source_name, source, conn_set=connection_set)
    model_builder = ModelBuilder(
        _duckdb_venv_path='dummy_path',
        _conn_set=connection_set,
        _static_models={source_model.name: source_model}
    )
        
    duckdb_conn = duckdb.connect()
    try:
        model_builder._attach_connections(duckdb_conn)
        
        cols = source.get_cols_for_create_table_stmt()
        duckdb_conn.execute(f"CREATE TABLE test_table ({cols})")
        
        duckdb_conn.execute("""
            INSERT INTO test_table (id, name, value) 
            VALUES (1, 'foo', 0), (2, 'bar', 100)
        """)

        asyncio.run(model_builder._build_models(duckdb_conn, select=None, full_refresh=False))
        result = duckdb_conn.table("test_table").pl()
    finally:
        duckdb_conn.close()
    
    assert result.equals(expected_df3)
