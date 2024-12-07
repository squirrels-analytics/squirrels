from sqlalchemy import create_engine
import polars as pl
import pytest

from squirrels import _connection_set as cs, _utils as u


@pytest.fixture(scope="module")
def connection_set():
    pool1 = create_engine("sqlite://")
    conn1 = pool1.raw_connection()
    try:
        cur1 = conn1.cursor()
        cur1.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, number NUMERIC)")
        cur1.execute("INSERT INTO test (name, number) VALUES ('test1', 10), ('test1', 20), ('test2', 30)")
        conn1.commit()
    finally:
        conn1.close()
    
    connection_set = cs.ConnectionSet({
        "db2": pool1
    })

    yield connection_set
    connection_set.dispose()


def test_get_connection_pool(connection_set: cs.ConnectionSet):
    with pytest.raises(u.ConfigurationError):
        connection_set.get_connection('does_not_exist')


def test_run_sql_query_from_conn_name(connection_set: cs.ConnectionSet):
    df: pl.DataFrame = connection_set.run_sql_query_from_conn_name("SELECT id, name FROM test WHERE id < 0", "db2")
    expected_df = pl.DataFrame({"id": [], "name": []})
    assert df.equals(expected_df)

    df: pl.DataFrame = connection_set.run_sql_query_from_conn_name("SELECT name, avg(number) AS avg_number FROM test GROUP BY name", "db2")
    expected_df = pl.DataFrame({
        "name": ["test1", "test2"],
        "avg_number": [15.0, 30.0]
    })
    assert df.equals(expected_df)

    with pytest.raises(RuntimeError):
        connection_set.run_sql_query_from_conn_name("SELECT invalid_column FROM test", "db2")
