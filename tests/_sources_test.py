import pytest

from squirrels._manifest import Settings
from squirrels._sources import Source, Sources, UpdateHints
from squirrels._model_configs import ColumnConfig
from squirrels._utils import ConfigurationError

def test_source_get_connection():
    # Test with no connection specified
    source = Source(name="test")
    assert source.get_connection(Settings(data={})) == "default"

    # Test with explicit connection
    source = Source(name="test", connection="test_connection")
    assert source.get_connection(Settings(data={})) == "test_connection"
    
    # Test with default connection from settings
    source = Source(name="test")
    assert source.get_connection(Settings(data={"connections.default_name_used": "default_connection"})) == "default_connection"

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

def test_source_get_insert_where_cond():
    source = Source(
        name="test",
        table="table_test",
        columns=[
            ColumnConfig(name="id", type="INTEGER"),
            ColumnConfig(name="timestamp", type="TIMESTAMP")
        ],
        update_hints=UpdateHints(increasing_column="timestamp")
    )
    
    # Test with full_refresh=False
    expected = "timestamp::TIMESTAMP > (SELECT max(timestamp) FROM test)"
    assert source.get_insert_where_cond(full_refresh=False) == expected
    
    # Test with full_refresh=True
    assert source.get_insert_where_cond(full_refresh=True) == "true"

def test_source_get_insert_on_conflict_clause():
    source = Source(
        name="test",
        columns=[
            ColumnConfig(name="id", type="INTEGER"),
            ColumnConfig(name="name", type="TEXT"),
            ColumnConfig(name="value", type="INTEGER")
        ],
        primary_key=["id"]
    )
    expected = "ON CONFLICT DO UPDATE SET name = EXCLUDED.name, value = EXCLUDED.value"
    assert source.get_insert_on_conflict_clause() == expected
    
    # Test with no primary key
    source_no_pk = Source(
        name="test",
        columns=[ColumnConfig(name="id", type="INTEGER")]
    )
    assert source_no_pk.get_insert_on_conflict_clause() == ""

def test_sources_duplicate_names():
    # Test that duplicate source names raise ConfigurationError
    with pytest.raises(ConfigurationError) as exc_info:
        Sources(sources=[
            Source(name="test"),
            Source(name="test")
        ])
    assert "Duplicate source names found" in str(exc_info.value)