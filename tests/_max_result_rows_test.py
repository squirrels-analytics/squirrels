"""
Tests for SQRL_DATASETS__MAX_ROWS_OUTPUT environment variable enforcement
"""
import pytest
import polars as pl
from unittest.mock import patch

from squirrels._project import SquirrelsProject
from squirrels._exceptions import InvalidInputError
from squirrels import _constants as c


@pytest.fixture
def mock_project_with_max_rows():
    """Create a mock project with max_result_rows set"""
    with patch('squirrels._project.SquirrelsProject._load_env_vars', return_value={
        c.SQRL_DATASETS_MAX_ROWS_OUTPUT: "5"  # Small limit for testing
    }):
        project = SquirrelsProject()
        return project


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
