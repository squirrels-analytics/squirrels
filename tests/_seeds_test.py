import pytest
import polars as pl
from pathlib import Path

from squirrels._seeds import Seed, Seeds, SeedsIO
from squirrels._model_configs import SeedConfig, ColumnConfig
from squirrels._utils import Logger

@pytest.fixture
def sample_csv_content():
    return """id,name,age,created_at
1,Alice,25,2024-01-01
2,Bob,30,2024-01-02
"""

@pytest.fixture
def sample_yaml_content():
    return """
cast_column_types: true
columns:
  - name: id
    type: integer
  - name: age
    type: int1
  - name: created_at
    type: date
"""

@pytest.fixture
def temp_seed_dir(tmp_path: Path):
    seed_dir = tmp_path / "seeds"
    seed_dir.mkdir()
    return seed_dir

@pytest.fixture
def setup_seed_files(temp_seed_dir: Path, sample_csv_content: str, sample_yaml_content: str):
    # Create CSV file
    csv_path = temp_seed_dir / "test_seed.csv"
    csv_path.write_text(sample_csv_content)
    
    # Create YAML config file
    yaml_path = temp_seed_dir / "test_seed.yml"
    yaml_path.write_text(sample_yaml_content)
    
    return temp_seed_dir.parent

def test_seed_post_init():
    # Test type casting in Seed class
    config = SeedConfig(
        cast_column_types=True,
        columns=[
            ColumnConfig(name="age", type="integer"),
            ColumnConfig(name="name", type="string")
        ]
    )
    
    df = pl.LazyFrame({
        "age": ["25", "30"],
        "name": ["Alice", "Bob"]
    })
    
    seed = Seed(config=config, df=df)
    result = seed.df.collect()
    
    assert result["age"].dtype == pl.Int32
    assert result["name"].dtype == pl.String

def test_seeds_run_query():
    # Test SQL query execution
    seed1_df = pl.LazyFrame({"id": [1, 2], "value": ["a", "b"]})
    seed2_df = pl.LazyFrame({"id": [1, 2], "other": [10, 20]})
    
    seeds = Seeds(
        _data={
            "seed1": Seed(config=SeedConfig(), df=seed1_df),
            "seed2": Seed(config=SeedConfig(), df=seed2_df)
        }
    )
    
    result = seeds.run_query("SELECT seed1.id, seed1.value, seed2.other FROM seed1 JOIN seed2 ON seed1.id = seed2.id")
    assert len(result) == 2
    assert all(col in result.columns for col in ["id", "value", "other"])

def test_seeds_get_dataframes():
    seeds_dict = {
        "test": Seed(config=SeedConfig(), df=pl.LazyFrame({"id": [1]}))
    }
    seeds = Seeds(_data=seeds_dict)
    
    result = seeds.get_dataframes()
    assert "test" in result
    assert isinstance(result["test"], Seed)
    assert result is not seeds._data  # Should be a copy

def test_seeds_io_load_files(setup_seed_files: Path):
    logger = Logger("test")
    seeds = SeedsIO.load_files(logger, str(setup_seed_files), env_vars={})
    
    assert "test_seed" in seeds._data
    seed = seeds._data["test_seed"]
    df = seed.df.collect()
    
    assert len(df) == 2
    assert df["age"].dtype == pl.Int8
    assert df["created_at"].dtype == pl.Date

def test_seeds_io_load_files_without_config(temp_seed_dir: Path, sample_csv_content: str):
    # Test loading CSV without a config file
    csv_path = temp_seed_dir / "test_seed.csv"
    csv_path.write_text(sample_csv_content)
    
    logger = Logger("test")
    seeds = SeedsIO.load_files(logger, str(temp_seed_dir.parent), env_vars={})
    
    assert "test_seed" in seeds._data
    seed = seeds._data["test_seed"]
    df = seed.df.collect()
    
    assert isinstance(seed.config, SeedConfig)
    assert not seed.config.cast_column_types
    
    assert len(df) == 2
    assert df["age"].dtype == pl.Int64
    assert df["created_at"].dtype == pl.Date
