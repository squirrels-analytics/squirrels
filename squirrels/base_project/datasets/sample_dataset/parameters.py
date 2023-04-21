import squirrels as sq
from typing import Dict


def main() -> Dict[str, sq.Parameter]:
    return {
        'limit': sq.NumberParameter('Upper Bound', min_value=1, max_value=10, increment=1, default_value=5),
    }
