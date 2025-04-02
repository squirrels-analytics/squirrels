import pytest, asyncio, polars as pl, time

from squirrels import _models as m, _utils as u, _model_queries as mq, _model_configs as mc
from squirrels.arguments.run_time_args import ParametersArgs, ContextArgs
from squirrels._manifest import DatasetConfig


def simple_model_config() -> mc.FederateModelConfig:
    return mc.FederateModelConfig()


@pytest.fixture(scope="module")
def context_args() -> ContextArgs:
    param_args = ParametersArgs("", {}, {})
    return ContextArgs(param_args, None, {}, {})


@pytest.fixture(scope="module")
def modelA_query_file() -> mq.QueryFileWithConfig:
    raw_query = 'SELECT * FROM {{ ref("modelB1") }} JOIN {{ ref("modelB2") }} USING (row_id)'
    query_file = mq.SqlQueryFile("dummy/path/modelA.sql", raw_query)
    return mq.QueryFileWithConfig(query_file, simple_model_config())


@pytest.fixture(scope="module")
def modelB1_query_file() -> mq.QueryFileWithConfig:
    def main_func(sqrl):
        time.sleep(1)
        return pl.LazyFrame({"row_id": ["a", "b", "c"], "valB": [1, 2, 3]})
    
    model_config = mc.FederateModelConfig(depends_on={"modelC2", "modelSeed"})
    query_file = mq.PyQueryFile("dummy/path/modelB1.py", main_func)
    return mq.QueryFileWithConfig(query_file, model_config)


@pytest.fixture(scope="module")
def modelB2_query_file() -> mq.QueryFileWithConfig:
    raw_query = 'SELECT row_id, valC FROM {{ ref("modelC1") }} JOIN {{ ref("modelC2") }} ON true'
    query_file = mq.SqlQueryFile("dummy/path/modelB2.sql", raw_query)
    return mq.QueryFileWithConfig(query_file, simple_model_config())


@pytest.fixture(scope="module")
def modelC1a_query_file() -> mq.QueryFileWithConfig:
    def main_func(sqrl):
        time.sleep(1)
        return pl.LazyFrame({"row_id": ["a", "b", "c"], "valC": [10, 20, 30]})
    
    query_file = mq.PyQueryFile("dummy/path/modelC1.py", main_func)
    return mq.QueryFileWithConfig(query_file, simple_model_config())


@pytest.fixture(scope="module")
def modelC1b_query_file() -> mq.QueryFileWithConfig:
    def main_func(sqrl):
        return pl.LazyFrame({"row_id": ["a", "b", "c"], "valC": [10, 20, 30]})
    
    model_config = mc.FederateModelConfig(depends_on={"modelA"})
    query_file = mq.PyQueryFile("dummy/path/modelC1.py", main_func)
    return mq.QueryFileWithConfig(query_file, model_config)


@pytest.fixture(scope="module")
def modelC2_query_file() -> mq.QueryFileWithConfig:
    raw_query = 'SELECT 1 AS a'
    query_file = mq.SqlQueryFile("dummy/path/modelC2.sql", raw_query)
    return mq.QueryFileWithConfig(query_file, simple_model_config())


@pytest.fixture(scope="function")
def modelA(modelA_query_file: mq.QueryFileWithConfig) -> m.FederateModel:
    model = m.FederateModel("modelA", modelA_query_file.config, modelA_query_file.query_file)
    model.is_target = True
    return model


@pytest.fixture(scope="function")
def modelB1(modelB1_query_file: mq.QueryFileWithConfig) -> m.FederateModel:
    return m.FederateModel("modelB1", modelB1_query_file.config, modelB1_query_file.query_file)


@pytest.fixture(scope="function")
def modelB2(modelB2_query_file: mq.QueryFileWithConfig) -> m.FederateModel:
    return m.FederateModel("modelB2", modelB2_query_file.config, modelB2_query_file.query_file)


@pytest.fixture(scope="function")
def modelC1a(modelC1a_query_file: mq.QueryFileWithConfig) -> m.FederateModel:
    return m.FederateModel("modelC1", modelC1a_query_file.config, modelC1a_query_file.query_file)


@pytest.fixture(scope="function")
def modelC1b(modelC1b_query_file: mq.QueryFileWithConfig) -> m.FederateModel:
    return m.FederateModel("modelC1", modelC1b_query_file.config, modelC1b_query_file.query_file)


@pytest.fixture(scope="function")
def modelC2(modelC2_query_file: mq.QueryFileWithConfig) -> m.FederateModel:
    return m.FederateModel("modelC2", modelC2_query_file.config, modelC2_query_file.query_file)


@pytest.fixture(scope="function")
def modelSeed() -> m.Seed:
    return m.Seed("modelSeed", mc.SeedConfig(), pl.LazyFrame({"row_id": ["a", "b", "c"]}))


@pytest.fixture(scope="function")
def compiled_dag(modelA, modelB1, modelB2, modelC1a, modelC2, modelSeed, context_args):
    models: list[m.DataModel] = [modelA, modelB1, modelB2, modelC1a, modelC2, modelSeed]
    models_dict = {mod.name: mod for mod in models}
    dag = m.DAG(DatasetConfig(name="test"), modelA, models_dict)
    dag._compile_models({}, context_args, True)
    return dag


@pytest.fixture(scope="function")
def compiled_dag_with_cycle(modelA, modelB1, modelB2, modelC1b, modelC2, modelSeed, context_args):
    models: list[m.DataModel] = [modelA, modelB1, modelB2, modelC1b, modelC2, modelSeed]
    models_dict = {mod.name: mod for mod in models}
    dag = m.DAG(DatasetConfig(name="test"), modelA, models_dict)
    dag._compile_models({}, context_args, True)
    return dag


def test_compile(compiled_dag: m.DAG):
    assert isinstance(modelA := compiled_dag.models_dict["modelA"], m.FederateModel)
    assert isinstance(modelB1 := compiled_dag.models_dict["modelB1"], m.FederateModel)
    assert isinstance(modelB2 := compiled_dag.models_dict["modelB2"], m.FederateModel)
    assert isinstance(modelC1 := compiled_dag.models_dict["modelC1"], m.FederateModel)
    assert isinstance(modelC2 := compiled_dag.models_dict["modelC2"], m.FederateModel)
    assert isinstance(modelA.compiled_query, mq.SqlModelQuery)
    assert modelA.compiled_query.query == "SELECT * FROM modelB1 JOIN modelB2 USING (row_id)"
    assert modelA.upstreams == {"modelB1": modelB1, "modelB2": modelB2}
    assert modelA.downstreams == {}
    assert not modelA.needs_python_df
    assert not modelB1.needs_python_df
    assert not modelC1.needs_python_df
    assert modelC2.needs_python_df
    try:
        terminal_nodes = compiled_dag._get_terminal_nodes()
    except u.ConfigurationError:
        pytest.fail("Unexpected exception")
    
    assert terminal_nodes == {"modelC1", "modelC2", "modelSeed"}


def test_cycles_produces_error(compiled_dag_with_cycle: m.DAG):
    with pytest.raises(u.ConfigurationError):
        compiled_dag_with_cycle._get_terminal_nodes()


def test_get_all_model_names(compiled_dag: m.DAG):
    model_names = compiled_dag.get_all_query_models()
    assert model_names == {"modelA", "modelB1", "modelB2", "modelC1", "modelC2"}


def test_run_models(compiled_dag: m.DAG):
    modelA = compiled_dag.models_dict["modelA"]
    
    start = time.time()
    asyncio.run(compiled_dag._run_models())
    end = time.time()
    
    assert isinstance(modelA, m.FederateModel)
    assert isinstance(modelA.result, pl.LazyFrame)
    assert modelA.result.collect().equals(pl.DataFrame({"row_id": ["a", "b", "c"], "valB": [1, 2, 3], "valC": [10, 20, 30]}))
    # assert (end - start) < 1.5
