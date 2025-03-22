import pytest

from squirrels._sources import Source, Sources, UpdateHints
from squirrels._model_configs import ColumnConfig
from squirrels._utils import ConfigurationError
from squirrels import _constants as c

def test_source_get_connection():
    # Test with no connection specified
    source = Source(name="test")
    assert source.get_connection({}) == "default"

    # Test with explicit connection
    source = Source(name="test", connection="test_connection")
    assert source.get_connection({}) == "test_connection"
    
    # Test with default connection from settings
    source = Source(name="test")
    assert source.get_connection({c.SQRL_CONNECTIONS_DEFAULT_NAME_USED: "default_connection"}) == "default_connection"

def test_source_get_table():
    # Test with explicit table
    source = Source(name="test", table="custom_table")
    assert source.get_table() == "custom_table"
    
    # Test with default table (name)
    source = Source(name="test")
    assert source.get_table() == "test"

def test_source_get_cols_for_create_table_stmt():
    source = Source(
        name="test",
        columns=[
            ColumnConfig(name="id", type="INTEGER"),
            ColumnConfig(name="name", type="TEXT")
        ],
        primary_key=["id"]
    )
    expected = "id INTEGER, name TEXT, PRIMARY KEY (id)"
    assert source.get_cols_for_create_table_stmt() == expected

def test_source_get_cols_for_insert_stmt():
    source = Source(
        name="test",
        columns=[
            ColumnConfig(name="id", type="INTEGER"),
            ColumnConfig(name="name", type="TEXT")
        ]
    )
    assert source.get_cols_for_insert_stmt() == "id, name"

def test_source_get_max_incr_col_query():
    source = Source(
        name="test",
        columns=[
            ColumnConfig(name="id", type="INTEGER"),
            ColumnConfig(name="timestamp", type="TIMESTAMP")
        ],
        update_hints=UpdateHints(increasing_column="timestamp")
    )
    assert source.get_max_incr_col_query() == "SELECT max(timestamp) FROM test"

def test_source_get_query_for_insert():
    source = Source(
        name="test",
        table="table_test",
        columns=[
            ColumnConfig(name="id", type="integer"),
            ColumnConfig(name="timestamp", type="timestamp")
        ],
        update_hints=UpdateHints(increasing_column="timestamp")
    )
    expected = "SELECT id, timestamp FROM db_default.table_test"
    assert source.get_query_for_insert(dialect="postgres", conn_name="default", table_name="table_test", max_value_of_increasing_col=None) == expected
    assert source.get_query_for_insert(dialect="postgres", conn_name="default", table_name="table_test", max_value_of_increasing_col="2024-01-01", full_refresh=True) == expected
    
    expected = "FROM postgres_query('db_default', 'SELECT id, timestamp FROM table_test WHERE CAST(timestamp AS TIMESTAMP) > CAST(''2024-01-01'' AS TIMESTAMP)')"
    assert source.get_query_for_insert(dialect="postgres", conn_name="default", table_name="table_test", max_value_of_increasing_col="2024-01-01", full_refresh=False) == expected

    expected = "FROM mysql_query('db_default', 'SELECT id, timestamp FROM table_test WHERE CAST(timestamp AS DATETIME) > CAST(''2024-01-01'' AS DATETIME)')"
    assert source.get_query_for_insert(dialect="mysql", conn_name="default", table_name="table_test", max_value_of_increasing_col="2024-01-01", full_refresh=False) == expected

    expected = "SELECT id, timestamp FROM db_default.table_test WHERE timestamp::timestamp > '2024-01-01'::timestamp"
    assert source.get_query_for_insert(dialect="sqlite", conn_name="default", table_name="table_test", max_value_of_increasing_col="2024-01-01", full_refresh=False) == expected

def test_source_get_insert_replace_clause():
    source = Source(
        name="test",
        columns=[
            ColumnConfig(name="id", type="INTEGER"),
            ColumnConfig(name="name", type="TEXT"),
            ColumnConfig(name="value", type="INTEGER")
        ],
        primary_key=["id"]
    )
    expected = "OR REPLACE"
    assert source.get_insert_replace_clause() == expected
    
    # Test with no primary key
    source_no_pk = Source(
        name="test",
        columns=[ColumnConfig(name="id", type="INTEGER")]
    )
    assert source_no_pk.get_insert_replace_clause() == ""

def test_sources_duplicate_names():
    # Test that duplicate source names raise ConfigurationError
    with pytest.raises(ConfigurationError) as exc_info:
        Sources(sources=[
            Source(name="test"),
            Source(name="test")
        ])
    assert "Duplicate source names found" in str(exc_info.value)