import pytest, asyncio, polars as pl
from pathlib import Path

from squirrels import _models as m, _utils as u, _model_queries as mq
from squirrels.arguments.run_time_args import ParametersArgs, ContextArgs
from squirrels._manifest import DatasetConfig
from squirrels._model_configs import DbviewModelConfig, FederateModelConfig, SeedConfig


# Model Type Tests
def test_model_types():
    assert m.ModelType.DBVIEW.value == "dbview"
    assert m.ModelType.FEDERATE.value == "federate"
    assert m.ModelType.SEED.value == "seed"


# Seed Model Tests
@pytest.fixture(scope="function")
def seed_model() -> m.Seed:
    df = pl.LazyFrame({"col1": [1, 2, 3]})
    return m.Seed("seed_model", SeedConfig(), df)

def test_seed_model(seed_model: m.Seed):
    assert seed_model.name == "seed_model"
    assert seed_model.model_type == m.ModelType.SEED
    assert not seed_model.is_target
    assert seed_model.result is not None
    assert seed_model.result.collect().equals(pl.DataFrame({"col1": [1, 2, 3]}))


# Dbview Model Tests
@pytest.fixture(scope="function")
def dbview_model() -> m.DbviewModel:
    config = DbviewModelConfig()
    query_file = mq.SqlQueryFile("test.sql", "SELECT * FROM table")
    return m.DbviewModel("test_model", config, query_file)

def test_dbview_model(dbview_model: m.DbviewModel):
    assert dbview_model.name == "test_model"
    assert dbview_model.model_type == m.ModelType.DBVIEW
    assert not dbview_model.is_target
    assert dbview_model.compiled_query is None


# Federate Model Tests
@pytest.fixture(scope="function")
def federate_model() -> m.FederateModel:
    config = FederateModelConfig()
    query_file = mq.SqlQueryFile("test.sql", 'SELECT * FROM {{ ref("upstream") }}')
    return m.FederateModel("test_model", config, query_file)

def test_federate_model(federate_model: m.FederateModel):
    assert federate_model.name == "test_model"
    assert federate_model.model_type == m.ModelType.FEDERATE
    assert not federate_model.is_target


# DAG Tests
@pytest.fixture(scope="function")
def simple_dag() -> m.DAG:
    # Create a simple DAG: A -> B -> C
    model_c = m.Seed("C", SeedConfig(), pl.LazyFrame({"id": [1, 2, 3]}))
    
    config_b = FederateModelConfig()
    query_b = 'SELECT * FROM {{ ref("C") }}'
    query_file_b = mq.SqlQueryFile("B.sql", query_b)
    model_b = m.FederateModel("B", config_b, query_file_b)
    
    config_a = FederateModelConfig()
    query_a = 'SELECT * FROM {{ ref("B") }}'
    query_file_a = mq.SqlQueryFile("A.sql", query_a)
    model_a = m.FederateModel("A", config_a, query_file_a)
    
    models = {"A": model_a, "B": model_b, "C": model_c}
    return m.DAG(DatasetConfig(name="test"), model_a, models)

def test_dag_compilation(simple_dag: m.DAG):
    ctx = {}
    param_args = ParametersArgs("", {}, {})
    ctx_args = ContextArgs(param_args, None, {}, {})
    simple_dag._compile_models(ctx, ctx_args, True)
    
    model_a = simple_dag.models_dict["A"]
    model_b = simple_dag.models_dict["B"]
    model_c = simple_dag.models_dict["C"]
    
    assert model_a.upstreams == {"B": model_b}
    assert model_b.upstreams == {"C": model_c}
    assert model_c.upstreams == {}

def test_dag_terminal_nodes(simple_dag: m.DAG):
    ctx = {}
    param_args = ParametersArgs("", {}, {})
    ctx_args = ContextArgs(param_args, None, {}, {})
    simple_dag._compile_models(ctx, ctx_args, True)
    
    terminal_nodes = simple_dag._get_terminal_nodes()
    assert terminal_nodes == {"C"}

def test_dag_cycle_detection():
    # Create a DAG with cycle: A -> B -> A
    config_b = FederateModelConfig()
    query_b = 'SELECT * FROM {{ ref("A") }}'
    query_file_b = mq.SqlQueryFile("B.sql", query_b)
    model_b = m.FederateModel("B", config_b, query_file_b)
    
    config_a = FederateModelConfig()
    query_a = 'SELECT * FROM {{ ref("B") }}'
    query_file_a = mq.SqlQueryFile("A.sql", query_a)
    model_a = m.FederateModel("A", config_a, query_file_a)
    
    models: dict[str, m.DataModel] = {"A": model_a, "B": model_b}
    dag = m.DAG(DatasetConfig(name="test"), model_a, models)
    
    ctx = {}
    param_args = ParametersArgs("", {}, {})
    ctx_args = ContextArgs(param_args, None, {}, {})
    dag._compile_models(ctx, ctx_args, True)
    
    with pytest.raises(u.ConfigurationError, match="Cycle found in model dependency graph"):
        dag._get_terminal_nodes()


# ModelsIO Tests
def test_load_files(tmp_path: Path):
    # Create temporary model files
    builds_path = tmp_path / "models" / "builds"
    dbviews_path = tmp_path / "models" / "dbviews"
    federates_path = tmp_path / "models" / "federates"
    builds_path.mkdir(parents=True)
    dbviews_path.mkdir(parents=True)
    federates_path.mkdir(parents=True)

    # Create a build model
    (builds_path / "model0.sql").write_text("SELECT * FROM table0")
    
    # Create a dbview model
    (dbviews_path / "model1.sql").write_text("SELECT * FROM table1")
    (dbviews_path / "model1.yml").write_text("connection: test_conn")
    
    # Create a federate model
    (federates_path / "model2.sql").write_text('SELECT * FROM {{ ref("model1") }}')
    
    logger = u.Logger("")
    build_model_files = m.ModelsIO.load_build_files(logger, str(tmp_path))
    dbview_model_files = m.ModelsIO.load_dbview_files(logger, str(tmp_path), env_vars={})
    federate_model_files = m.ModelsIO.load_federate_files(logger, str(tmp_path))
    
    assert set(build_model_files.keys()) == {"model0"}
    assert set(dbview_model_files.keys()) == {"model1"}
    assert set(federate_model_files.keys()) == {"model2"}

