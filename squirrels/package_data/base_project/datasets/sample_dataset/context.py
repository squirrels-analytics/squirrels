from typing import Dict, Any
import squirrels as sq


def main(prms: sq.ParameterSet, proj: Dict[str, str]) -> Dict[str, Any]:
    limit_parameter: sq.NumberParameter = prms('number_example')
    limit: str = limit_parameter.get_selected_value()
    return {'limit': limit}
