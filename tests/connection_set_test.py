from sqlalchemy import StaticPool, create_engine
from functools import partial
import sqlite3, pandas as pd
import pytest

from squirrels import connection_set as cs, utils


@pytest.fixture
def connection_set() -> cs.ConnectionSet:
    connection_creator = partial(sqlite3.connect, ":memory:", check_same_thread=False)
    
    pool1 = StaticPool(connection_creator)
    conn1 = pool1.connect()
    try:
        cur1 = conn1.cursor()
        cur1.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
        cur1.execute("INSERT INTO test (name) VALUES ('test1'), ('test2'), ('test3')")
        conn1.commit()
    finally:
        conn1.close()
    
    pool2 = create_engine("sqlite://")
    conn2 = pool2.raw_connection()
    try:
        cur2 = conn2.cursor()
        cur2.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, number NUMERIC)")
        cur2.execute("INSERT INTO test (name, number) VALUES ('test1', 10), ('test1', 20), ('test2', 30)")
        conn2.commit()
    finally:
        conn2.close()
    
    connection_set = cs.ConnectionSet({
        "db1": pool1, 
        "db2": pool2
    })

    yield connection_set
    connection_set.dispose()


def test_get_connection_pool(connection_set: cs.ConnectionSet):
    with pytest.raises(utils.ConfigurationError):
        connection_set.get_connection_pool('does_not_exist')


def test_get_dataframe_from_query(connection_set: cs.ConnectionSet):
    df: pd.DataFrame = connection_set.get_dataframe_from_query("db2", 
        "SELECT id, name FROM test WHERE id < 0")
    expected_df = pd.DataFrame(columns=["id", "name"]) # empty dataframe
    assert df.equals(expected_df)

    df: pd.DataFrame = connection_set.get_dataframe_from_query("db2", 
        "SELECT name, avg(number) AS avg_number FROM test GROUP BY name")
    expected_df = pd.DataFrame({
        "name": ["test1", "test2"],
        "avg_number": [15.0, 30.0]
    })
    assert df.equals(expected_df)


def test_sqldf():
    df1 = pd.DataFrame({
        "name": ["test1", "test2"],
        "number": [10, 20]
    })
    df2 = pd.DataFrame({
        "name": ["test1", "test2"],
        "number": [30, 40]
    })

    query = "SELECT a.name, a.number as num1, b.number as num2 FROM df1 a JOIN df2 b ON a.name = b.name"
    df = cs.sqldf(query, {'df1': df1, 'df2': df2})
    expected_df = pd.DataFrame({
        "name": ["test1", "test2"],
        "num1": [10, 20],
        "num2": [30, 40]
    })
    assert df.equals(expected_df)
