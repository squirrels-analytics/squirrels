"""
Tests for SQRL_DATASETS__MAX_RESULT_ROWS environment variable enforcement
"""
import pytest
import polars as pl
from unittest.mock import patch

from squirrels._project import SquirrelsProject
from squirrels._exceptions import InvalidInputError, ConfigurationError
from squirrels import _constants as c


@pytest.fixture
def mock_project_with_max_rows():
    """Create a mock project with max_result_rows set"""
    with patch('squirrels._project.SquirrelsProject._env_vars', new_callable=lambda: {
        c.SQRL_SECRET_KEY: "test_secret_key",
        c.SQRL_DATASETS_MAX_RESULT_ROWS: "5"  # Small limit for testing
    }):
        project = SquirrelsProject(filepath=".", load_dotenv_globally=False)
        # Override the cached property to return our test value
        project._max_result_rows = 5
        return project


def test_max_result_rows_property_default():
    """Test that _max_result_rows defaults to 100000 when env var is not set"""
    with patch('squirrels._project.SquirrelsProject._env_vars', new_callable=lambda: {
        c.SQRL_SECRET_KEY: "test_secret_key"
    }):
        project = SquirrelsProject(filepath=".", load_dotenv_globally=False)
        assert project._max_result_rows == 100000


def test_max_result_rows_property_custom():
    """Test that _max_result_rows reads from env var when set"""
    with patch('squirrels._project.SquirrelsProject._env_vars', new_callable=lambda: {
        c.SQRL_SECRET_KEY: "test_secret_key",
        c.SQRL_DATASETS_MAX_RESULT_ROWS: "50000"
    }):
        project = SquirrelsProject(filepath=".", load_dotenv_globally=False)
        assert project._max_result_rows == 50000


def test_max_result_rows_property_invalid():
    """Test that _max_result_rows raises ConfigurationError for invalid values"""
    # Test the validation logic directly by patching the property
    with patch('squirrels._project.SquirrelsProject._env_vars', new_callable=lambda: {
        c.SQRL_SECRET_KEY: "test_secret_key",
        c.SQRL_DATASETS_MAX_RESULT_ROWS: "invalid"
    }):
        # Create project and access property - should raise during property evaluation
        project = SquirrelsProject(filepath=".", load_dotenv_globally=False)
        # Clear any cached value first
        if hasattr(project, '__dict__') and '_max_result_rows' in project.__dict__:
            del project.__dict__['_max_result_rows']
        with pytest.raises(ConfigurationError, match="must be a positive integer"):
            _ = project._max_result_rows


def test_enforce_max_result_rows_within_limit(mock_project_with_max_rows: SquirrelsProject):
    """Test that _enforce_max_result_rows allows results within limit"""
    project = mock_project_with_max_rows
    lazy_df = pl.LazyFrame({"col1": [1, 2, 3, 4, 5]})
    
    result = project._enforce_max_result_rows(lazy_df, "dataset")
    assert len(result) == 5


def test_enforce_max_result_rows_exceeds_limit(mock_project_with_max_rows: SquirrelsProject):
    """Test that _enforce_max_result_rows raises InvalidInputError when limit exceeded"""
    project = mock_project_with_max_rows
    lazy_df = pl.LazyFrame({"col1": list(range(10))})  # 10 rows, limit is 5
    
    with pytest.raises(InvalidInputError) as exc_info:
        project._enforce_max_result_rows(lazy_df, "dataset")
    
    assert exc_info.value.status_code == 413
    assert exc_info.value.error == "dataset_result_too_large"
