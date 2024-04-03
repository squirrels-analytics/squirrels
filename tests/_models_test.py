import pytest, asyncio, pandas as pd, time

from squirrels import _models as m, _utils as u
from squirrels.arguments.run_time_args import ContextArgs


@pytest.fixture(scope="function")
def sql_model_config1():
    return m.SqlModelConfig("my_conn_name", m.Materialization.VIEW)


@pytest.fixture(scope="function")
def sql_model_config2():
    return m.SqlModelConfig("default", m.Materialization.TABLE)


@pytest.mark.parametrize("fixture,query,expected", [
    ("sql_model_config1", "SELECT...", "CREATE VIEW my_model AS\nSELECT..."),
    ("sql_model_config2", "SELECT...", "CREATE TABLE my_model AS\nSELECT...")
])
def test_get_sql_for_create(fixture: str, query: str, expected: str, request: pytest.FixtureRequest):
    sql_model_config: m.SqlModelConfig = request.getfixturevalue(fixture)
    assert sql_model_config.get_sql_for_create("my_model", query) == expected


@pytest.mark.parametrize("fixture,kwargs,expected", [
    ("sql_model_config1", {"not_exist": "test"}, m.SqlModelConfig("my_conn_name", m.Materialization.VIEW)),
    ("sql_model_config1", {"materialized": "taBle"}, m.SqlModelConfig("my_conn_name", m.Materialization.TABLE)),
    ("sql_model_config2", {"connection_name": "Test"}, m.SqlModelConfig("Test", m.Materialization.TABLE))
])
def test_set_attribute(fixture: str, kwargs: str, expected: m.SqlModelConfig, request: pytest.FixtureRequest):
    sql_model_config: m.SqlModelConfig = request.getfixturevalue(fixture)
    assert sql_model_config.set_attribute(**kwargs) == ""
    assert sql_model_config == expected


@pytest.fixture(scope="module")
def context_args():
    return ContextArgs({}, {}, None, {}, {})


@pytest.fixture(scope="module")
def modelA_query_file():
    raw_query = m.RawSqlQuery('SELECT * FROM {{ ref("modelB1") }} JOIN {{ ref("modelB2") }} USING (row_id)')
    return m.QueryFile("/path1", m.ModelType.FEDERATE, m.QueryType.SQL, raw_query)


@pytest.fixture(scope="module")
def modelB1_query_file():
    def main_func(sqrl):
        time.sleep(1)
        return pd.DataFrame({"row_id": ["a", "b", "c"], "valB": [1, 2, 3]})
    
    raw_query = m.RawPyQuery(main_func, lambda sqrl: ["modelC2"])
    return m.QueryFile("/path2", m.ModelType.FEDERATE, m.QueryType.PYTHON, raw_query)


@pytest.fixture(scope="module")
def modelB2_query_file():
    raw_query = m.RawSqlQuery('SELECT * FROM {{ ref("modelC1") }}')
    return m.QueryFile("/path3", m.ModelType.FEDERATE, m.QueryType.SQL, raw_query)


@pytest.fixture(scope="module")
def modelC1a_query_file():
    def main_func(sqrl):
        time.sleep(1)
        return pd.DataFrame({"row_id": ["a", "b", "c"], "valC": [10, 20, 30]})
    
    raw_query = m.RawPyQuery(main_func, lambda sqrl: [])
    return m.QueryFile("/path4", m.ModelType.FEDERATE, m.QueryType.PYTHON, raw_query)


@pytest.fixture(scope="module")
def modelC1b_query_file():
    def main_func(sqrl):
        return pd.DataFrame({"row_id": ["a", "b", "c"], "valC": [10, 20, 30]})
    
    raw_query = m.RawPyQuery(main_func, lambda sqrl: ["modelA"])
    return m.QueryFile("/path4", m.ModelType.FEDERATE, m.QueryType.PYTHON, raw_query)


@pytest.fixture(scope="module")
def modelC2_query_file():
    raw_query = m.RawSqlQuery('SELECT 1 as a')
    return m.QueryFile("/path5", m.ModelType.FEDERATE, m.QueryType.SQL, raw_query)


@pytest.fixture(scope="function")
def modelA(modelA_query_file):
    model = m.Model("modelA", modelA_query_file)
    model.is_target = True
    return model


@pytest.fixture(scope="function")
def modelB1(modelB1_query_file):
    return m.Model("modelB1", modelB1_query_file)


@pytest.fixture(scope="function")
def modelB2(modelB2_query_file):
    return m.Model("modelB2", modelB2_query_file)


@pytest.fixture(scope="function")
def modelC1a(modelC1a_query_file):
    return m.Model("modelC1", modelC1a_query_file)


@pytest.fixture(scope="function")
def modelC1b(modelC1b_query_file):
    return m.Model("modelC1", modelC1b_query_file)


@pytest.fixture(scope="function")
def modelC2(modelC2_query_file):
    return m.Model("modelC2", modelC2_query_file)


@pytest.fixture(scope="function")
def compiled_dag(modelA: m.Model, modelB1, modelB2, modelC1a, modelC2, context_args):
    models: list[m.Model] = [modelA, modelB1, modelB2, modelC1a, modelC2]
    models_dict = {mod.name: mod for mod in models}
    dag = m.DAG(None, modelA, models_dict)
    asyncio.run(dag._compile_models({}, context_args, True))
    return dag


@pytest.fixture(scope="function")
def compiled_dag_with_cycle(modelA: m.Model, modelB1, modelB2, modelC1b, modelC2, context_args):
    models: list[m.Model] = [modelA, modelB1, modelB2, modelC1b, modelC2]
    models_dict = {mod.name: mod for mod in models}
    dag = m.DAG(None, modelA, models_dict)
    asyncio.run(dag._compile_models({}, context_args, True))
    return dag


def test_compile(compiled_dag: m.DAG):
    modelA = compiled_dag.models_dict["modelA"]
    modelB1 = compiled_dag.models_dict["modelB1"]
    modelB2 = compiled_dag.models_dict["modelB2"]
    modelC2 = compiled_dag.models_dict["modelC2"]
    assert modelA.compiled_query.query == "SELECT * FROM modelB1 JOIN modelB2 USING (row_id)"
    assert modelA.upstreams == {"modelB1": modelB1, "modelB2": modelB2}
    assert modelA.downstreams == {}
    assert not modelA.needs_sql_table and not modelA.needs_pandas
    assert modelB1.needs_sql_table and not modelB1.needs_pandas
    assert modelC2.needs_pandas and not modelC2.needs_sql_table
    try:
        terminal_nodes = compiled_dag._get_terminal_nodes()
    except u.ConfigurationError:
        raise AssertionError()
    
    assert terminal_nodes == {"modelC1", "modelC2"}


def test_cycles_produces_error(compiled_dag_with_cycle: m.DAG):
    with pytest.raises(u.ConfigurationError):
        compiled_dag_with_cycle._get_terminal_nodes()


def test_get_all_model_names(compiled_dag: m.DAG):
    model_names = compiled_dag.get_all_model_names()
    assert model_names == {"modelA", "modelB1", "modelB2", "modelC1", "modelC2"}


def test_run_models(compiled_dag: m.DAG):
    terminal_nodes = compiled_dag._get_terminal_nodes()
    modelA = compiled_dag.models_dict["modelA"]
    
    start = time.time()
    asyncio.run(compiled_dag._run_models(terminal_nodes))
    end = time.time()
    assert (end - start) < 1.5
    assert modelA.result.equals(pd.DataFrame({"row_id": ["a", "b", "c"], "valB": [1, 2, 3], "valC": [10, 20, 30]}))
