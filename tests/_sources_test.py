import pytest

from squirrels._sources import Source, Sources, UpdateHints
from squirrels._model_configs import ColumnConfig
from squirrels._utils import ConfigurationError
from squirrels import _constants as c

def test_source_get_connection():
    # Test with no connection specified
    source = Source().finalize_connection({})
    assert source.get_connection() == "default"

    # Test with explicit connection
    source = Source(connection="test_connection").finalize_connection({})
    assert source.get_connection() == "test_connection"
    
    # Test with default connection from settings
    source = Source().finalize_connection({c.SQRL_CONNECTIONS_DEFAULT_NAME_USED: "default_connection"})
    assert source.get_connection() == "default_connection"

def test_source_get_table():
    # Test with explicit table
    source = Source(table="custom_table").finalize_table("test")
    assert source.get_table() == "custom_table"
    
    # Test with default table (empty string, will be replaced with source name)
    source = Source().finalize_table("test")
    assert source.get_table() == "test"

def test_source_get_cols_for_create_table_stmt():
    source = Source(
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
        columns=[
            ColumnConfig(name="id", type="INTEGER"),
            ColumnConfig(name="name", type="TEXT")
        ]
    )
    assert source.get_cols_for_insert_stmt() == "id, name"

def test_source_get_max_incr_col_query():
    source = Source(
        columns=[
            ColumnConfig(name="id", type="INTEGER"),
            ColumnConfig(name="timestamp", type="TIMESTAMP")
        ],
        update_hints=UpdateHints(increasing_column="timestamp")
    )
    assert source.get_max_incr_col_query("test") == "SELECT max(timestamp) FROM test"

def test_source_get_query_for_insert():
    source = Source(
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
        columns=[ColumnConfig(name="id", type="INTEGER")]
    )
    assert source_no_pk.get_insert_replace_clause() == ""

def test_sources_creation():
    # Test creating Sources with a dictionary
    sources = Sources(sources={
        "test1": Source(table="table1"),
        "test2": Source(table="table2")
    })
    assert len(sources.sources) == 2
    assert "test1" in sources.sources
    assert "test2" in sources.sources
    assert sources.sources["test1"].table == "table1"
    assert sources.sources["test2"].table == "table2"

def test_sources_list_conversion():
    # Test that Sources validator converts a list of sources to a dictionary
    # Using type: ignore to bypass the type checker since the validator handles the conversion
    sources = Sources(sources=[  # type: ignore
        {"name": "test1", "table": "table1"},
        {"name": "test2", "table": "table2"}
    ])
    assert len(sources.sources) == 2
    assert "test1" in sources.sources
    assert "test2" in sources.sources
    assert sources.sources["test1"].table == "table1"
    assert sources.sources["test2"].table == "table2"

def test_sources_duplicate_name_error():
    # Test that Sources validator raises an error for duplicate source names
    with pytest.raises(ConfigurationError) as exc_info:
        Sources(sources=[  # type: ignore
            {"name": "test1", "table": "table1"},
            {"name": "test1", "table": "table2"}
        ])

def test_sources_missing_name_error():
    # Test that Sources validator raises an error for sources without a name
    with pytest.raises(ConfigurationError) as exc_info:
        Sources(sources=[  # type: ignore
            {"name": "test1", "table": "table1"},
            {"table": "table2"}
        ])

def test_sources_column_type_validation():
    # Test that Sources validator checks column types
    with pytest.raises(ConfigurationError) as exc_info:
        Sources(sources={
            "test1": Source(
                columns=[
                    ColumnConfig(name="id", type="INTEGER"),
                    ColumnConfig(name="name", type="")  # Empty type
                ]
            )
        })
