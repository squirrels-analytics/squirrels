import pytest, polars as pl

from squirrels import _utils as u


@pytest.mark.parametrize('input_str,expected', [
    ("", []),
    ("[]", []),
    ("1", ["1"]),
    ('["1"]', ["1"]),
    ("1,2,3", ["1", "2", "3"]),
    ('["1", "2", "3"]', ["1", "2", "3"])
])
def test_load_json_or_comma_delimited_str(input_str, expected):
    assert u.load_json_or_comma_delimited_str_as_list(input_str) == expected


def test_run_sql_on_dataframes():
    df_dict = { "input_df": pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]}) }
    expected = pl.DataFrame({"total": [5, 7, 9]})
    result = u.run_sql_on_dataframes("SELECT a+b AS total FROM input_df", df_dict)
    assert result.equals(expected)


@pytest.mark.anyio
async def test_run_polars_sql_on_dataframes_basic_select():
    """Test basic SELECT query works with Polars SQL"""
    df_dict = { "result": pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]}) }
    expected = pl.DataFrame({"total": [5, 7, 9]})
    result = await u.run_polars_sql_on_dataframes("SELECT a+b AS total FROM result", df_dict)
    assert result.equals(expected)


@pytest.mark.anyio
async def test_run_polars_sql_on_dataframes_with_cte():
    """Test WITH (CTE) query"""
    df_dict = { "result": pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]}) }
    expected = pl.DataFrame({"total": [5, 7, 9]})
    result = await u.run_polars_sql_on_dataframes(
        "WITH temp AS (SELECT a, b FROM result) SELECT a+b AS total FROM temp", 
        df_dict
    )
    assert result.equals(expected)


@pytest.mark.anyio
async def test_run_polars_sql_on_dataframes_rejects_ddl():
    """Test that DDL statements are rejected"""
    df_dict = { "result": pl.LazyFrame({"a": [1, 2, 3]}) }
    
    with pytest.raises(u.ConfigurationError) as exc_info:
        await u.run_polars_sql_on_dataframes("DROP TABLE result", df_dict)
    assert "read-only" in str(exc_info.value).lower()


@pytest.mark.anyio
async def test_run_polars_sql_on_dataframes_rejects_multiple_statements():
    """Test that multiple statements are rejected"""
    df_dict = { "result": pl.LazyFrame({"a": [1, 2, 3]}) }
    
    with pytest.raises(u.ConfigurationError) as exc_info:
        await u.run_polars_sql_on_dataframes("SELECT * FROM result; SELECT * FROM result", df_dict)
    assert "single" in str(exc_info.value).lower()


@pytest.mark.anyio
async def test_run_polars_sql_on_dataframes_rejects_invalid_table():
    """Test that references to non-registered tables are rejected"""
    df_dict = { "result": pl.LazyFrame({"a": [1, 2, 3]}) }
    
    with pytest.raises(u.ConfigurationError) as exc_info:
        await u.run_polars_sql_on_dataframes("SELECT * FROM other_table", df_dict)
    assert "not allowed" in str(exc_info.value).lower() or "available" in str(exc_info.value).lower()


@pytest.mark.anyio
async def test_run_polars_sql_on_dataframes_timeout():
    """Test that queries exceeding timeout raise ConfigurationError"""
    # Create two tables that when cross joined will produce 10,000,000 rows (2,000 x 5,000)
    # This creates a computationally expensive query that should exceed a short timeout
    table1 = pl.LazyFrame({"x": range(2000)})
    table2 = pl.LazyFrame({"y": range(5000)})
    df_dict = { "t1": table1, "t2": table2 }
    
    # Use a cross join to generate 10,000,000 rows
    # This query will take time to execute due to the large number of rows generated
    large_query = """
        SELECT t1.x AS x, t2.y AS y 
        FROM t1 
        CROSS JOIN t2
    """
    
    # Set a very short timeout that should be exceeded by the query
    with pytest.raises(u.ConfigurationError) as exc_info:
        await u.run_polars_sql_on_dataframes(large_query, df_dict, timeout_seconds=0.01)
    
    error_message = str(exc_info.value).lower()
    keywords = ["timeout", "exceeded", "0.01"]
    assert all(keyword in error_message for keyword in keywords)


def test_to_bool_truthy_values():
    truthy_inputs = [True, "1", "true", "TrUe", "t", "yes", "YeS", "y", "on", "  on  ", 1]
    for val in truthy_inputs:
        assert u.to_bool(val) is True


def test_to_bool_falsey_values():
    falsey_inputs = [False, None, 0, "0", "false", "FaLsE", "f", "no", "n", "off", "", " random "]
    for val in falsey_inputs:
        assert u.to_bool(val) is False


def test_user_has_elevated_privileges_matrix():
    cases = [
        ("admin", "admin", True),
        ("admin", "member", True),
        ("admin", "guest", True),
        ("member", "admin", False),
        ("member", "member", True),
        ("member", "guest", True),
        ("guest", "admin", False),
        ("guest", "member", False),
        ("guest", "guest", True),
        ("AdMiN", "MeMbEr", True),  # case-insensitive input
    ]
    for user_level, required_level, expected in cases:
        assert u.user_has_elevated_privileges(user_level, required_level) is expected
