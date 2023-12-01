from typing import Dict, Callable, Any
from collections import OrderedDict
from functools import partial
from textwrap import dedent
import pytest, sqlite3, sqlalchemy as sa, pandas as pd

from squirrels import _renderer as rd, parameters as p, parameter_options as po, data_sources as d, \
    _parameter_configs as pc, _parameter_sets as ps
from squirrels._manifest import ManifestIO, _Manifest
from squirrels._connection_set import ConnectionSetIO, ConnectionSet


@pytest.fixture(scope="module")
def manifest() -> None:
    parms = {
        "datasets": {
            "avg_shop_price_by_city": {
                "database_views": {
                    "vw_shop_city": {},
                    "vw_avg_prices": {"db_connection": "test_db2"}
                },
                "final_view": ""
            }
        }
    }
    ManifestIO.obj = _Manifest(parms)


@pytest.fixture(scope="module")
def connection_set() -> None:
    connection_creator = partial(sqlite3.connect, ":memory:", check_same_thread=False)
    pool1 = sa.StaticPool(connection_creator)
    conn1 = pool1.connect()
    try:
        cur1 = conn1.cursor()
        cur1.execute("CREATE TABLE lu_cities (city_id TEXT, city_name TEXT)")
        cur1.execute("INSERT INTO lu_cities (city_id, city_name) VALUES ('c0', 'Toronto'), ('c1', 'Boston')")
        cur1.execute("CREATE TABLE tbl_shop_city (city_name TEXT, shop_name TEXT)")
        cur1.execute("INSERT INTO tbl_shop_city (city_name, shop_name) VALUES ('Toronto', 'test1'), ('Toronto', 'test2'), ('Boston', 'test3')")
        conn1.commit()
    finally:
        conn1.close()
    
    pool2 = sa.StaticPool(connection_creator)
    conn2 = pool2.connect()
    try:
        cur2 = conn2.cursor()
        cur2.execute("CREATE TABLE tbl_prices (shop_name TEXT, item TEXT, price NUMERIC)")
        cur2.execute("INSERT INTO tbl_prices (shop_name, price) VALUES ('test1', 10), ('test1', 20), ('test2', 30), ('test3', 15)")
        conn2.commit()
    finally:
        conn2.close()
    
    ConnectionSetIO.obj = ConnectionSet({
        "default": pool1, 
        "test_db2": pool2
    })
    yield
    ConnectionSetIO.Dispose()


@pytest.fixture(scope="module", autouse=True)
def raw_param_set(manifest, connection_set) -> None:
    city_ds = d.MultiSelectDataSource("lu_cities", "city_id", "city_name")
    p.MultiSelectParameter.CreateFromSource("city", "City", city_ds)
    p.NumberParameter.CreateSimple("limit", "Limit", 0, 100)
    
    df_dict = ps.ParameterConfigsSetIO._GetDfDict(None)
    ps.ParameterConfigsSetIO.obj._post_process_params(df_dict)


def context_main(ctx: Dict, prms: Dict[str, p.Parameter], *args, **kwargs) -> Dict[str, Any]:
    city_param: p.MultiSelectParameter = prms["city"]
    ctx["cities"] = city_param.get_selected_labels_quoted_joined()


@pytest.fixture(scope="module")
def raw_db_view_queries1() -> Dict[str, rd.Query]:
    def dbview1_query(connection_pool: sa.Pool, ctx: Dict[str, Any], *args, **kwargs) -> pd.DataFrame:
        conn = connection_pool.connect()
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT city_name, shop_name FROM tbl_shop_city WHERE city_name IN ({ctx['cities']})")
            return pd.DataFrame(cur.fetchall(), columns=["city_name", "shop_name"])
        finally:
            conn.close()

    return {
        "vw_shop_city": dbview1_query,
        "vw_avg_prices": "SELECT shop_name, avg(price) as avg_price FROM tbl_prices GROUP BY shop_name"
    }


@pytest.fixture(scope="module")
def raw_db_view_queries2() -> Dict[str, rd.Query]:
    def dbview2_query(connection_pool: sa.Pool, *args, **kwargs) -> pd.DataFrame:
        conn = connection_pool.connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT shop_name, avg(price) as avg_price FROM tbl_prices GROUP BY shop_name")
            return pd.DataFrame(cur.fetchall(), columns=["shop_name", "avg_price"])
        finally:
            conn.close()

    return {
        "vw_shop_city": "SELECT city_name, shop_name FROM tbl_shop_city WHERE city_name IN ({{ ctx['cities'] }})",
        "vw_avg_prices": dbview2_query
    }


@pytest.fixture(scope="module")
def raw_final_view_py_query() -> Callable[..., pd.DataFrame]:
    def final_view_query(database_views: Dict[str, pd.DataFrame], *args, **kwargs) -> pd.DataFrame:
        df1 = database_views["vw_shop_city"]
        df2 = database_views["vw_avg_prices"]
        df = df1.merge(df2, on="shop_name", how="left")
        return df.groupby("city_name")["avg_price"].mean().reset_index()
    return final_view_query


@pytest.fixture(scope="module")
def raw_final_view_sql_query() -> str:
    return dedent("""
        SELECT a.city_name, avg(b.avg_price) as avg_price 
        FROM vw_shop_city a LEFT JOIN vw_avg_prices b
            ON a.shop_name = b.shop_name
        GROUP BY a.city_name
    """)


@pytest.fixture(scope="module")
def renderer1(raw_db_view_queries1: Dict[str, rd.Query], raw_final_view_py_query: rd.Query):
    return rd.Renderer("avg_shop_price_by_city", context_main, raw_db_view_queries1, raw_final_view_py_query)


@pytest.fixture(scope="module")
def renderer2(raw_db_view_queries2: Dict[str, rd.Query], raw_final_view_sql_query: rd.Query):
    return rd.Renderer("avg_shop_price_by_city", context_main, raw_db_view_queries2, raw_final_view_sql_query)


def test_apply_selections(renderer1: rd.Renderer):
    city_options = [po.SelectParameterOption('c0', 'Toronto'), po.SelectParameterOption('c1', 'Boston')]
    city_config = pc.MultiSelectParameterConfig("city", "City", city_options)
    limit_options = [po.NumberParameterOption(0, 100)]
    limit_config = pc.NumberParameterConfig("limit", "Limit", limit_options)
        
    ordered_dict = OrderedDict()
    ordered_dict["city"] = p.MultiSelectParameter(city_config, city_options, [])
    ordered_dict["limit"] = p.NumberParameter(limit_config, limit_options[0], 0)
    expected = ps.ParameterSet(ordered_dict)
    
    actual = renderer1.apply_selections(None, {})
    assert actual == expected

    ordered_dict["city"]._selected_ids = ('c0',)
    actual = renderer1.apply_selections(None, {'city': 'c0'})
    assert actual == expected


def test_load_results(renderer1: rd.Renderer, renderer2: rd.Renderer):
    expected_df = pd.DataFrame({
        "city_name": ["Boston", "Toronto"],
        "avg_price": [15.0, 22.5]
    })

    _, _, _, _, df1 = renderer1.load_results(None, {})
    assert df1.equals(expected_df)

    _, _, _, _, df2 = renderer2.load_results(None, {})
    assert df2.equals(expected_df)
