from typing import Any
import pytest

from squirrels._arguments.init_time_args import ParametersArgs
from squirrels._arguments.run_time_args import ContextArgs, TextValue
from squirrels._schemas.auth_models import GuestUser, CustomUserFields


@pytest.mark.parametrize("placeholder,value,expected",[
    ("placeholder1", "value1", "value1"),
    ("placeholder2", None, None),
    ("placeholder3", TextValue("value3"), "value3"),
])
def test_set_placeholder(placeholder: str, value: Any, expected: Any):
    param_args = ParametersArgs(project_path="", proj_vars={}, env_vars={})
    user = GuestUser(username="test_user", custom_fields=CustomUserFields())
    context_args = ContextArgs(**param_args.__dict__, user=user, prms={}, configurables={}, _conn_args=param_args)
    assert context_args.set_placeholder(placeholder, value) == ""
    assert context_args._placeholders[placeholder] == expected
