import pytest, asyncio, pandas as pd, time

from squirrels import _models as m, _utils as _u
from squirrels.arguments.run_time_args import ContextArgs
from squirrels._manifest import DatasetConfig


@pytest.fixture(scope="function")
def sql_model_config1():
    return m._SqlModelConfig("my_conn_name", m._Materialization.VIEW)


@pytest.fixture(scope="function")
def sql_model_config2():
    return m._SqlModelConfig("default", m._Materialization.TABLE)


@pytest.mark.parametrize("fixture,query,expected", [
    ("sql_model_config1", "SELECT...", "CREATE VIEW my_model AS\nSELECT..."),
    ("sql_model_config2", "SELECT...", "CREATE TABLE my_model AS\nSELECT...")
])
def test_get_sql_for_create(fixture: str, query: str, expected: str, request: pytest.FixtureRequest):
    sql_model_config: m._SqlModelConfig = request.getfixturevalue(fixture)
    assert sql_model_config.get_sql_for_create("my_model", query) == expected


@pytest.mark.parametrize("fixture,kwargs,expected", [
    ("sql_model_config1", {"not_exist": "test"}, m._SqlModelConfig("my_conn_name", m._Materialization.VIEW)),
    ("sql_model_config1", {"materialized": "taBle"}, m._SqlModelConfig("my_conn_name", m._Materialization.TABLE)),
    ("sql_model_config2", {"connection_name": "Test"}, m._SqlModelConfig("Test", m._Materialization.TABLE))
])
def test_set_attribute(fixture: str, kwargs: dict[str, str], expected: m._SqlModelConfig, request: pytest.FixtureRequest):
    sql_model_config: m._SqlModelConfig = request.getfixturevalue(fixture)
    assert sql_model_config.set_attribute(**kwargs) == ""
    assert sql_model_config == expected


@pytest.fixture(scope="module")
def context_args():
    return ContextArgs({}, {}, None, {}, {}, {})


@pytest.fixture(scope="module")
def modelA_query_file():
    raw_query = 'SELECT * FROM {{ ref("modelB1") }} JOIN {{ ref("modelB2") }} USING (row_id)'
    return m.SqlQueryFile("dummy/path/modelA.sql", m.ModelType.FEDERATE, raw_query)


@pytest.fixture(scope="module")
def modelB1_query_file():
    def main_func(sqrl):
        time.sleep(1)
        return pd.DataFrame({"row_id": ["a", "b", "c"], "valB": [1, 2, 3]})
    
    raw_query = m._RawPyQuery(main_func, lambda sqrl: ["modelC1", "modelSeed"])
    return m.PyQueryFile("dummy/path/modelB1.py", m.ModelType.FEDERATE, raw_query)


@pytest.fixture(scope="module")
def modelB2_query_file():
    raw_query = 'SELECT row_id, valC FROM {{ ref("modelC1") }} JOIN {{ ref("modelC2") }}'
    return m.SqlQueryFile("dummy/path/modelB2.sql", m.ModelType.FEDERATE, raw_query)


@pytest.fixture(scope="module")
def modelC1a_query_file():
    def main_func(sqrl):
        time.sleep(1)
        return pd.DataFrame({"row_id": ["a", "b", "c"], "valC": [10, 20, 30]})
    
    raw_query = m._RawPyQuery(main_func, lambda sqrl: [])
    return m.PyQueryFile("dummy/path/modelC1.py", m.ModelType.FEDERATE, raw_query)


@pytest.fixture(scope="module")
def modelC1b_query_file():
    def main_func(sqrl):
        return pd.DataFrame({"row_id": ["a", "b", "c"], "valC": [10, 20, 30]})
    
    raw_query = m._RawPyQuery(main_func, lambda sqrl: ["modelA"])
    return m.PyQueryFile("dummy/path/modelC1.py", m.ModelType.FEDERATE, raw_query)


@pytest.fixture(scope="module")
def modelC2_query_file():
    raw_query = 'SELECT 1 AS a'
    return m.SqlQueryFile("dummy/path/modelC2.sql", m.ModelType.FEDERATE, raw_query)


@pytest.fixture(scope="function")
def modelA(modelA_query_file, simple_manifest_config, simple_conn_set):
    model = m.Model("modelA", modelA_query_file, simple_manifest_config, simple_conn_set)
    model.is_target = True
    return model


@pytest.fixture(scope="function")
def modelB1(modelB1_query_file, simple_manifest_config, simple_conn_set):
    return m.Model("modelB1", modelB1_query_file, simple_manifest_config, simple_conn_set)


@pytest.fixture(scope="function")
def modelB2(modelB2_query_file, simple_manifest_config, simple_conn_set):
    return m.Model("modelB2", modelB2_query_file, simple_manifest_config, simple_conn_set)


@pytest.fixture(scope="function")
def modelC1a(modelC1a_query_file, simple_manifest_config, simple_conn_set):
    return m.Model("modelC1", modelC1a_query_file, simple_manifest_config, simple_conn_set)


@pytest.fixture(scope="function")
def modelC1b(modelC1b_query_file, simple_manifest_config, simple_conn_set):
    return m.Model("modelC1", modelC1b_query_file, simple_manifest_config, simple_conn_set)


@pytest.fixture(scope="function")
def modelC2(modelC2_query_file, simple_manifest_config, simple_conn_set):
    return m.Model("modelC2", modelC2_query_file, simple_manifest_config, simple_conn_set)


@pytest.fixture(scope="function")
def modelSeed():
    return m.Seed("modelSeed", pd.DataFrame())


@pytest.fixture(scope="function")
def compiled_dag(simple_manifest_config, modelA, modelB1, modelB2, modelC1a, modelC2, modelSeed, context_args):
    models: list[m.Referable] = [modelA, modelB1, modelB2, modelC1a, modelC2, modelSeed]
    models_dict = {mod.name: mod for mod in models}
    dag = m.DAG(simple_manifest_config, DatasetConfig(name="test"), modelA, models_dict)
    asyncio.run(dag._compile_models({}, context_args, True))
    return dag


@pytest.fixture(scope="function")
def compiled_dag_with_cycle(simple_manifest_config, modelA, modelB1, modelB2, modelC1b, modelC2, modelSeed, context_args):
    models: list[m.Referable] = [modelA, modelB1, modelB2, modelC1b, modelC2, modelSeed]
    models_dict = {mod.name: mod for mod in models}
    dag = m.DAG(simple_manifest_config, DatasetConfig(name="test"), modelA, models_dict)
    asyncio.run(dag._compile_models({}, context_args, True))
    return dag


def test_compile(compiled_dag: m.DAG):
    assert isinstance(modelA := compiled_dag.models_dict["modelA"], m.Model)
    assert isinstance(modelB1 := compiled_dag.models_dict["modelB1"], m.Model)
    assert isinstance(modelB2 := compiled_dag.models_dict["modelB2"], m.Model)
    assert isinstance(modelC2 := compiled_dag.models_dict["modelC2"], m.Model)
    assert isinstance(modelA.compiled_query, m._Query)
    assert modelA.compiled_query.query == "SELECT * FROM modelB1 JOIN modelB2 USING (row_id)"
    assert modelA.upstreams == {"modelB1": modelB1, "modelB2": modelB2}
    assert modelA.downstreams == {}
    assert not modelA.needs_sql_table and not modelA.needs_pandas
    assert modelB1.needs_sql_table and not modelB1.needs_pandas
    assert not modelC2.needs_pandas and modelC2.needs_sql_table
    try:
        terminal_nodes = compiled_dag._get_terminal_nodes()
    except _u.ConfigurationError:
        pytest.fail("Unexpected exception")
    
    assert terminal_nodes == {"modelC1", "modelC2", "modelSeed"}


def test_cycles_produces_error(compiled_dag_with_cycle: m.DAG):
    with pytest.raises(_u.ConfigurationError):
        compiled_dag_with_cycle._get_terminal_nodes()


def test_get_all_model_names(compiled_dag: m.DAG):
    model_names = compiled_dag.get_all_query_models()
    assert model_names == {"modelA", "modelB1", "modelB2", "modelC1", "modelC2"}


def test_run_models(compiled_dag: m.DAG):
    terminal_nodes = compiled_dag._get_terminal_nodes()
    modelA = compiled_dag.models_dict["modelA"]
    
    start = time.time()
    asyncio.run(compiled_dag._run_models(terminal_nodes))
    end = time.time()
    
    assert pd.DataFrame(modelA.result).equals(pd.DataFrame({"row_id": ["a", "b", "c"], "valB": [1, 2, 3], "valC": [10, 20, 30]}))
    assert (end - start) < 2.5
