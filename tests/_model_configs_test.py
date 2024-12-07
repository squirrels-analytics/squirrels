import pytest

from squirrels._model_configs import FederateModelConfig


@pytest.mark.parametrize("eager,create_type", [
    (False, "VIEW"),
    (True, "TABLE")
])
def test_get_sql_for_create(eager: bool, create_type: str):
    config = FederateModelConfig(eager=eager)
    select_query = "SELECT * FROM table"
    result = config.get_sql_for_create("test_model", select_query)
    
    expected = f"CREATE {create_type} test_model AS\nSELECT * FROM table"
    assert result == expected
