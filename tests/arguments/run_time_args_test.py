from typing import Any
import pytest

from squirrels.arguments.run_time_args import ContextArgs, TextValue


@pytest.mark.parametrize("placeholder,value,expected",[
    ("placeholder1", "value1", "value1"),
    ("placeholder2", None, None),
    ("placeholder3", TextValue("value3"), "value3"),
])
def test_set_placeholder(placeholder: str, value: Any, expected: Any):
    context_args = ContextArgs({}, {}, None, {}, {}, {})
    assert context_args.set_placeholder(placeholder, value) == ""
    assert context_args.placeholders[placeholder] == expected
