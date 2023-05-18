from typing import Dict, Any
import squirrels as sr


def main(prms: sr.ParameterSet, args: Dict[str, Any], *p_args, **kwargs) -> Dict[str, Any]:
    limit_parameter: sr.NumberParameter = prms['upper_bound']
    limit: str = limit_parameter.get_selected_value()
    return {'limit': limit}
