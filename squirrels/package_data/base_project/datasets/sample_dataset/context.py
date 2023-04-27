from typing import Dict, Any
import squirrels as sq


def main(prms: sq.ParameterSet, proj: Dict[str, str], *args, **kwargs) -> Dict[str, Any]:
    limit_parameter: sq.NumberParameter = prms['upper_bound']
    limit: str = limit_parameter.get_selected_value()
    return {'limit': limit}
