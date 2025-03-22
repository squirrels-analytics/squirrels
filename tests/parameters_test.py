from copy import copy
from datetime import datetime, date
from decimal import Decimal
import pytest

from squirrels import parameters as p, parameter_options as _po, _parameter_configs as _pc, _utils as u
from squirrels._exceptions import InvalidInputError, ConfigurationError


class TestSingleSelectParameter:
    @pytest.fixture(scope="class")
    def config1(self) -> _pc.SingleSelectParameterConfig:
        param_options = [
            _po.SelectParameterOption('ss0', 'My Label', is_default=True, field0 = "a", field1 = "b",
                                     custom_fields={"field1": "x", "field2": "y", "label": "z"}),
            _po.SelectParameterOption('ss1', "Another Label")
        ]
        return _pc.SingleSelectParameterConfig("test", "Test", param_options)

    @pytest.fixture(scope="class")
    def param1(self, config1: _pc.SingleSelectParameterConfig) -> p.SingleSelectParameter:
        return p.SingleSelectParameter(config1, config1.all_options, 'ss0')
    
    def test_invalid_init(self, config1: _pc.SingleSelectParameterConfig):
        with pytest.raises(AssertionError):
            p.SingleSelectParameter(config1, config1.all_options, None)
        with pytest.raises(InvalidInputError):
            p.SingleSelectParameter(config1, config1.all_options, 'wrong_id')

    def test_get_selected(self, param1: p.SingleSelectParameter):
        param_option = param1._config.all_options[0]
        assert param1.get_selected() == param_option
        assert param1.get_selected_id() == "ss0"
        assert param1.get_selected_id_quoted() == "'ss0'"
        assert param1.get_selected_label() == "My Label"
        assert param1.get_selected_label_quoted() == "'My Label'"

    def test_get_selected_custom_field(self, param1: p.SingleSelectParameter):
        assert param1.get_selected("id") == "ss0"
        assert param1.get_selected("label") == "My Label"
        assert param1.get_selected("field0") == "a"
        assert param1.get_selected("field1") == "x" # custom_fields take precedence over keyword arguments
        assert param1.get_selected("field2") == "y"
        assert param1.get_selected("field3", default_field="field2") == "y"
        assert param1.get_selected("field3", default="b") == "b"
        assert param1.get_selected("field3", default_field="field2", default="b") == "y"
        assert param1.get_selected("field3", default_field="field4", default="b") == "b"

    def test_to_json_dict(self, param1: p.SingleSelectParameter):
        expected = {
            "widget_type": "single_select",
            "name": "test",
            "label": "Test",
            "description": "",
            "options": [
                {"id": "ss0", "label": "My Label"},
                {"id": "ss1", "label": "Another Label"}
            ],
            "trigger_refresh": False,
            "selected_id": "ss0"
        }
        assert param1._to_json_dict0() == expected


class TestMultiSelectParameter:
    @pytest.fixture(scope="class")
    def config1(self) -> _pc.MultiSelectParameterConfig:
        options = (
            _po.SelectParameterOption('ms0', 'Option 1', field0 = "a", field1 = "b", field2 = "c"), 
            _po.SelectParameterOption('ms1', 'Option 2', custom_fields = {"field0": "x", "field1": "y", "field2": "z"}), 
            _po.SelectParameterOption('ms2', 'Option 3', field0 = "m", custom_fields = {"field1": "n"})
        )
        return _pc.MultiSelectParameterConfig("test", "Test", options)
    
    @pytest.fixture(scope="class")
    def param1(self, config1: _pc.MultiSelectParameterConfig) -> p.MultiSelectParameter:
        return p.MultiSelectParameter(config1, config1.all_options, ["ms1", "ms2"])
    
    @pytest.fixture(scope="class")
    def param2(self, config1: _pc.MultiSelectParameterConfig) -> p.MultiSelectParameter:
        return p.MultiSelectParameter(config1, config1.all_options, [])
    
    @pytest.fixture(scope="class")
    def param3(self, config1: _pc.MultiSelectParameterConfig) -> p.MultiSelectParameter:
        config = copy(config1)
        config.none_is_all = False
        return p.MultiSelectParameter(config, config.all_options, [])
    
    def test_invalid_init(self, config1: _pc.MultiSelectParameterConfig):
        with pytest.raises(InvalidInputError):
            p.MultiSelectParameter(config1, config1.all_options, "wrong_id")
    
    def test_get_selected1(self, param1: p.MultiSelectParameter):
        assert param1.get_selected_ids_as_list() == ('ms1', 'ms2')
        assert param1.get_selected_ids_joined() == "ms1,ms2"
        assert param1.get_selected_ids_quoted_as_list() == ("'ms1'", "'ms2'")
        assert param1.get_selected_ids_quoted_joined() == "'ms1','ms2'"

        assert param1.get_selected_labels_as_list() == ('Option 2', 'Option 3')
        assert param1.get_selected_labels_joined() == "Option 2,Option 3"
        assert param1.get_selected_labels_quoted_as_list() == ("'Option 2'", "'Option 3'")
        assert param1.get_selected_labels_quoted_joined() == "'Option 2','Option 3'"

    def test_get_selected2(self, param2: p.MultiSelectParameter):
        assert param2.get_selected_ids_as_list() == ('ms0', 'ms1', 'ms2')
    
    def test_get_selected3(self, param3: p.MultiSelectParameter):
        assert param3.get_selected_ids_as_list() == ()
    
    def test_get_selected_custom_field(self, param1: p.MultiSelectParameter):
        assert param1.get_selected_list("id") == ("ms1", "ms2")
        assert param1.get_selected_list("label") == ("Option 2", "Option 3")
        assert param1.get_selected_list("field0") == ("x", "m")
        assert param1.get_selected_list("field1") == ("y", "n")
        assert param1.get_selected_list("field2", default_field="field0", default="h") == ("z", "m")
        assert param1.get_selected_list("field2", default="h") == ("z", "h")
        with pytest.raises(ConfigurationError):
            param1.get_selected_list("field2")

    def test_to_json_dict(self, param1: p.MultiSelectParameter):
        expected = {
            "widget_type": "multi_select",
            "name": "test",
            "label": "Test",
            "description": "",
            "options": [
                {"id": "ms0", "label": "Option 1"},
                {"id": "ms1", "label": "Option 2"},
                {"id": "ms2", "label": "Option 3"}
            ],
            "trigger_refresh": False,
            "show_select_all": True,
            "order_matters": False,
            "selected_ids": ['ms1', 'ms2']
        }
        assert param1._to_json_dict0() == expected


class TestDateParameter:
    @pytest.fixture(scope="class")
    def config1(self) -> _pc.DateParameterConfig:
        options = (_po.DateParameterOption("2020-01-01", min_date="2020-01-01"),)
        return _pc.DateParameterConfig("test", "Test", options)
    
    @pytest.fixture(scope="class")
    def param1(self, config1: _pc.DateParameterConfig) -> p.DateParameter:
        return p.DateParameter(config1, config1.all_options[0], date(2021,1,1))
    
    def test_invalid_init(self, config1: _pc.DateParameterConfig):
        with pytest.raises(InvalidInputError):
            p.DateParameter(config1, config1.all_options[0], date(2019,1,1))
    
    def test_get_selected1(self, param1: p.DateParameter):
        assert param1.get_selected_date() == "2021-01-01"
        assert param1.get_selected_date_quoted() == "'2021-01-01'"
    
    def test_to_json_dict(self, param1: p.DateParameter):
        expected = {
            "widget_type": "date",
            "name": "test",
            "label": "Test",
            "description": "",
            "selected_date": "2021-01-01",
            "min_date": "2020-01-01",
            "max_date": None
        }
        assert param1._to_json_dict0() == expected


class TestDateRangeParameter:
    @pytest.fixture(scope="class")
    def param1(self) -> p.DateRangeParameter:
        options = (_po.DateRangeParameterOption("20220101", "20221231", date_format="%Y%m%d"),)
        config = _pc.DateRangeParameterConfig("test_id", "Test Label", options)
        return p.DateRangeParameter(config, options[0], "2022-06-14", "2023-03-15")
    
    def test_get_selected(self, param1: p.DateRangeParameter):
        assert param1.get_selected_start_date() == "20220614"
        assert param1.get_selected_start_date_quoted() == "'20220614'"
        assert param1.get_selected_end_date() == "20230315"
        assert param1.get_selected_end_date_quoted() == "'20230315'"
    
    def test_to_json_dict(self, param1: p.DateRangeParameter):
        expected = {
            "widget_type": "date_range",
            "name": "test_id",
            "label": "Test Label",
            "description": "",
            "selected_start_date": "2022-06-14",
            "selected_end_date": "2023-03-15",
            "min_date": None,
            "max_date": None
        }
        assert param1._to_json_dict0() == expected


class TestNumberParameter:
    @pytest.fixture(scope="class")
    def config1(self) -> _pc.NumberParameterConfig:
        options = (_po.NumberParameterOption(0, 10, increment="0.5"),)
        return _pc.NumberParameterConfig("test", "Test", options)
    
    @pytest.fixture(scope="class")
    def param1(self, config1: _pc.NumberParameterConfig) -> p.NumberParameter:
        return p.NumberParameter(config1, config1.all_options[0], Decimal("4.5"))
    
    def test_invalid_init(self, config1: _pc.NumberParameterConfig):
        with pytest.raises(InvalidInputError):
            p.NumberParameter(config1, config1.all_options[0], Decimal("4.6"))
    
    def test_get_selected(self, param1: p.NumberParameter):
        assert param1.get_selected_value() == 4.5

    def test_to_json_dict(self, param1: p.NumberParameter):
        expected = {
            "widget_type": "number",
            "name": "test",
            "label": "Test",
            "description": "",
            "min_value": 0,
            "max_value": 10,
            "increment": 0.5,
            "selected_value": 4.5
        }
        assert param1._to_json_dict0() == expected


class TestNumberRangeParameter:
    @pytest.fixture(scope="class")
    def config1(self) -> _pc.NumberRangeParameterConfig:
        options = (_po.NumberRangeParameterOption(0, 10, increment="0.5"),)
        return _pc.NumberRangeParameterConfig("test", "Test", options)
    
    @pytest.fixture(scope="class")
    def param1(self, config1: _pc.NumberRangeParameterConfig) -> p.NumberRangeParameter:
        return p.NumberRangeParameter(config1, config1.all_options[0], "2.5", "6.5")

    def test_invalid_init(self, config1: _pc.NumberRangeParameterConfig):
        with pytest.raises(InvalidInputError):
            p.NumberRangeParameter(config1, config1.all_options[0], "2.5", "6.7")
    
    def test_get_selected(self, param1: p.NumberRangeParameter):
        assert param1.get_selected_lower_value() == 2.5
        assert param1.get_selected_upper_value() == 6.5

    def test_to_json_dict(self, param1: p.NumberRangeParameter):
        expected = {
            "widget_type": "number_range",
            "name": "test",
            "label": "Test",
            "description": "",
            "min_value": 0,
            "max_value": 10,
            "increment": 0.5,
            "selected_lower_value": 2.5,
            "selected_upper_value": 6.5
        }
        assert param1._to_json_dict0() == expected


class TestTextValue:
    @pytest.fixture(scope="class")
    def text_value1(self):
        return p.TextValue("test")

    @pytest.fixture(scope="class")
    def text_value2(self):
        return p.TextValue("2024-01-01")
    
    def test_apply(self, text_value1: p.TextValue):
        assert text_value1.apply(lambda x: x + "1") == p.TextValue("test1")
    
    def test_invalid_apply(self, text_value1: p.TextValue):
        with pytest.raises(ConfigurationError):
            text_value1.apply(lambda x: len(x)) # type: ignore
    
    def test_apply_percent_wrap(self, text_value1: p.TextValue):
        assert text_value1.apply_percent_wrap() == p.TextValue("%test%")
    
    def test_apply_as_bool(self, text_value1: p.TextValue):
        assert text_value1.apply_as_bool(lambda x: len(x) > 3) == True
        assert text_value1.apply_as_bool(lambda x: len(x) > 4) == False
    
    def test_invalid_apply_as_bool(self, text_value1: p.TextValue):
        with pytest.raises(ConfigurationError):
            text_value1.apply_as_bool(lambda x: x) # type: ignore
    
    def test_apply_as_int(self, text_value1: p.TextValue):
        assert text_value1.apply_as_number(lambda x: len(x)) == 4
    
    def test_invalid_apply_as_int(self, text_value1: p.TextValue):
        with pytest.raises(ConfigurationError):
            text_value1.apply_as_number(lambda x: x) # type: ignore
    
    def test_apply_as_datetime(self, text_value2: p.TextValue):
        assert text_value2.apply_as_datetime(lambda x: datetime.strptime(x, "%Y-%m-%d")) == datetime(2024, 1, 1)
    
    def test_invalid_apply_as_datetime(self, text_value2: p.TextValue):
        with pytest.raises(ConfigurationError):
            text_value2.apply_as_datetime(lambda x: x) # type: ignore


class TestTextParameter:
    def create_param(self, default_text: str, input_type: str) -> p.TextParameter:
        options = (_po.TextParameterOption(default_text=default_text),)
        config = _pc.TextParameterConfig("test", "Test", options, input_type=input_type)
        return p.TextParameter(config, config.all_options[0], default_text)
    
    @pytest.mark.parametrize("default_text,input_type", [
        ("", "number"),
        ("", "date"),
        ("", "datetime-local"),
        ("2024-01-01T00:00:00", "datetime-local"),
        ("", "month"),
        ("2024-01-01", "month"),
        ("", "time"),
        ("24:00", "time"),
        ("", "color"),
        ("#00000G", "color")
    ])
    def test_invalid_init(self, default_text: str, input_type: str):
        with pytest.raises(InvalidInputError):
            self.create_param(default_text, input_type)
    
    @pytest.mark.parametrize("default_text,input_type", [
        ("", "text"),
        ("", "textarea"),
        ("", "password"),
        ("10", "number"),
        ("2024-01-01", "date"),
        ("2024-01-01T00:00", "datetime-local"),
        ("2024-01", "month"),
        ("00:00", "time"),
        ("#000000", "color")
    ])
    def test_valid_init(self, default_text: str, input_type: str):
        try:
            self.create_param(default_text, input_type)
        except Exception:
            pytest.fail("Unexpected exception")
    
    def test_get_entered_text(self):
        param = self.create_param("my entered text", "text")
        assert param.get_entered_text() == p.TextValue("my entered text")

    def test_get_entered_int(self):
        param = self.create_param("1000", "number")
        assert param.get_entered_int() == 1000
    
    @pytest.mark.parametrize("default_text,input_type", [
        ("", "text"),
        ("", "textarea"),
        ("", "password"),
        ("2024-01-01", "date"),
        ("2024-01-01T00:00", "datetime-local"),
        ("2024-01", "month"),
        ("00:00", "time"),
        ("#000000", "color")
    ])
    def test_invalid_get_entered_int(self, default_text: str, input_type: str):
        param = self.create_param(default_text, input_type)
        with pytest.raises(ConfigurationError):
            param.get_entered_int()

    @pytest.mark.parametrize("default_text,input_type,expected", [
        ("2024-01-05", "date", datetime(2024, 1, 5, 0, 0)),
        ("2024-01-05T15:15", "datetime-local", datetime(2024, 1, 5, 15, 15)),
        ("2024-01", "month", datetime(2024, 1, 1, 0, 0)),
        ("15:15", "time", datetime(1900, 1, 1, 15, 15))
    ])
    def test_get_entered_datetime(self, default_text: str, input_type: str, expected: datetime):
        param = self.create_param(default_text, input_type)
        assert param.get_entered_datetime() == expected
    
    @pytest.mark.parametrize("default_text,input_type", [
        ("", "text"),
        ("", "textarea"),
        ("", "password"),
        ("10", "number"),
        ("#000000", "color")
    ])
    def test_invalid_get_entered_datetime(self, default_text: str, input_type: str):
        param = self.create_param(default_text, input_type)
        with pytest.raises(ConfigurationError):
            param.get_entered_datetime()
    