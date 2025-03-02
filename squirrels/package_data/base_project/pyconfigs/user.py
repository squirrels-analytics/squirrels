from typing import Literal
from squirrels import BaseUser


class User(BaseUser):
    """
    Extend the BaseUser class with custom attributes. The attributes defined here will be added as columns to the users table. 
    - Only the following types are supported: [str, int, float, bool, typing.Literal]
    - For str, int, and float types, add "| None" after the type to make it nullable. 
    - Always set a default value for the column (use None if default is null).
    
    Example:
        organization: str | None = None
    """
    role: Literal["manager", "employee"] = "employee"

    @classmethod
    def dropped_columns(cls) -> list[str]:
        """
        The fields defined above cannot be modified once added to the database. 
        However, you can choose to drop columns by adding them to this list.
        """
        return []
