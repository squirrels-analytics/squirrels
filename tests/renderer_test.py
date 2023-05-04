from typing import Dict, Callable, Any
from functools import partial
from textwrap import dedent
import pytest, sqlite3, sqlalchemy as sa, pandas as pd

from squirrels import manifest as mf, connection_set as cs, renderer as rd
import squirrels as sq


class TestRenderer:
    @pytest.fixture
    def manifest(self) -> mf.Manifest:
        parms = {
            "datasets": {
                "avg_shop_price_by_city": {
                    "database_views": {
                        "shop_city_view": {"db_connection": "test_db1"},
                        "avg_prices_view": {"db_connection": "test_db2"}
                    }
                }
            }
        }
        return mf.Manifest(parms)
    
    @pytest.fixture
    def connection_set(self) -> cs.ConnectionSet:
        connection_creator = partial(sqlite3.connect, ":memory:", check_same_thread=False)
        pool1 = sa.StaticPool(connection_creator)
        conn1 = pool1.connect()
        try:
            cur1 = conn1.cursor()
            cur1.execute("CREATE TABLE lu_cities (city_id TEXT, city TEXT)")
            cur1.execute("INSERT INTO lu_cities (city_id, city) VALUES ('c0', 'Toronto'), ('c1', 'Boston')")
            cur1.execute("CREATE TABLE shop_city (city TEXT, shop TEXT)")
            cur1.execute("INSERT INTO shop_city (city, shop) VALUES ('Toronto', 'test1'), ('Toronto', 'test2'), ('Boston', 'test3')")
            conn1.commit()
        finally:
            conn1.close()
        
        pool2 = sa.StaticPool(connection_creator)
        conn2 = pool2.connect()
        try:
            cur2 = conn2.cursor()
            cur2.execute("CREATE TABLE prices (shop TEXT, item TEXT, price NUMERIC)")
            cur2.execute("INSERT INTO prices (shop, price) VALUES ('test1', 10), ('test1', 20), ('test2', 30), ('test3', 15)")
            conn2.commit()
        finally:
            conn2.close()
        
        connection_set = cs.ConnectionSet({
            "test_db1": pool1, 
            "test_db2": pool2
        })

        yield connection_set
        connection_set.dispose()
    
    @pytest.fixture
    def raw_param_set(self) -> sq.ParameterSet:
        city_ds = sq.SelectionDataSource("test_db1", "lu_cities", "city_id", "city")
        city_param = sq.DataSourceParameter(sq.WidgetType.MultiSelect, "city", "City", city_ds)
        price_limit = sq.NumberParameter('limit', 'Limit', 0, 100)
        return sq.ParameterSet([city_param, price_limit])

    def context_main(self, prms: sq.ParameterSet, *args, **kwargs) -> Dict[str, Any]:
        city_param: sq.MultiSelectParameter = prms["city"]
        return {"cities": city_param.get_selected_labels_quoted_joined()}
    
    @pytest.fixture
    def raw_db_view_queries1(self) -> Dict[str, rd.Query]:
        def dbview1_query(connection_pool: sa.Pool, ctx: Dict[str, Any], *args, **kwargs) -> pd.DataFrame:
            conn = connection_pool.connect()
            try:
                cur = conn.cursor()
                cur.execute(f"SELECT city, shop FROM shop_city WHERE city IN ({ctx['cities']})")
                return pd.DataFrame(cur.fetchall(), columns=["city", "shop"])
            finally:
                conn.close()

        return {
            "shop_city_view": dbview1_query,
            "avg_prices_view": "SELECT shop, avg(price) as price FROM prices GROUP BY shop"
        }
    
    @pytest.fixture
    def raw_db_view_queries2(self) -> Dict[str, rd.Query]:
        def dbview2_query(connection_pool: sa.Pool, *args, **kwargs) -> pd.DataFrame:
            conn = connection_pool.connect()
            try:
                cur = conn.cursor()
                cur.execute("SELECT shop, avg(price) as price FROM prices GROUP BY shop")
                return pd.DataFrame(cur.fetchall(), columns=["shop", "price"])
            finally:
                conn.close()

        return {
            "shop_city_view": "SELECT city, shop FROM shop_city WHERE city IN ({{ ctx['cities'] }})",
            "avg_prices_view": dbview2_query
        }
    
    @pytest.fixture
    def raw_final_view_py_query(self) -> Callable[..., pd.DataFrame]:
        def final_view_query(database_views: Dict[str, pd.DataFrame], *args, **kwargs) -> pd.DataFrame:
            df1 = database_views["shop_city_view"]
            df2 = database_views["avg_prices_view"]
            df = df1.merge(df2, on="shop", how="left")
            return df.groupby("city")["price"].mean().reset_index()
        return final_view_query
    
    @pytest.fixture
    def raw_final_view_sql_query(self) -> str:
        return dedent("""
            SELECT a.city, avg(b.price) as price 
            FROM shop_city_view a LEFT JOIN avg_prices_view b
                ON a.shop = b.shop
            GROUP BY a.city
        """)

    @pytest.fixture
    def renderer1(self, manifest: mf.Manifest, connection_set: cs.ConnectionSet, raw_param_set: sq.ParameterSet, 
                  raw_db_view_queries1: Dict[str, rd.Query], raw_final_view_py_query: rd.Query):
        return rd.Renderer("avg_shop_price_by_city", manifest, connection_set, raw_param_set, self.context_main, 
                           raw_db_view_queries1, raw_final_view_py_query)

    @pytest.fixture
    def renderer2(self, manifest: mf.Manifest, connection_set: cs.ConnectionSet, raw_param_set: sq.ParameterSet, 
                  raw_db_view_queries2: Dict[str, rd.Query], raw_final_view_sql_query: rd.Query):
        return rd.Renderer("avg_shop_price_by_city", manifest, connection_set, raw_param_set, self.context_main, 
                           raw_db_view_queries2, raw_final_view_sql_query)
    
    def test_apply_selections(self, renderer1: rd.Renderer):
        expected_params = {
            "city": sq.MultiSelectParameter('city', 'City', (
                sq.SelectParameterOption('c0', 'Toronto'), sq.SelectParameterOption('c1', 'Boston')
            )),
            "limit": sq.NumberParameter('limit', 'Limit', 0, 100)
        }
        
        param_set = renderer1.apply_selections({})
        assert param_set._parameters_dict == expected_params

        expected_params["city"].selected_ids = ('c0',)
        param_set = renderer1.apply_selections({'city': 'c0'})
        assert param_set._parameters_dict == expected_params

        expected_params2 = {"city": expected_params["city"]}
        param_set = renderer1.apply_selections({'city': 'c0'}, updates_only=True)
        assert param_set._parameters_dict == expected_params2
    
    def test_load_results(self, renderer1: rd.Renderer, renderer2: rd.Renderer):
        expected_df = pd.DataFrame({
            "city": ["Boston", "Toronto"],
            "price": [15.0, 22.5]
        })

        _, _, _, _, df1 = renderer1.load_results({})
        assert df1.equals(expected_df)

        _, _, _, _, df2 = renderer2.load_results({})
        assert df2.equals(expected_df)
    