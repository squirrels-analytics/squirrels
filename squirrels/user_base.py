from typing import Any
from dataclasses import dataclass


@dataclass
class User:
    """
    Base class for extending the custom User model class

    Attributes:
        username: The identifier for the user
        is_internal: Setting this to True lets the user access "private" datasets
    """
    username: str
    is_internal: bool

    def __init__(self, username: str, *, is_internal: bool = False, **kwargs):
        """
        Constructor for the User base class

        Parameters:
            ...see Attributes of User
        """
        self.username = username
        self.is_internal = is_internal
    
    def __hash__(self) -> int:
        return hash(self.username)
    
    def set_attributes(self, user_dict: dict[str, Any]) -> None:
        """
        Can be overwritten in "auth.py" to introduce custom attributes. Does nothing by default
        """
        pass
    
    def with_attributes(self, user_dict: dict[str, Any]):
        self.set_attributes(user_dict)
        return self
    
    @classmethod
    def _FromDict(cls, user_dict: dict[str, Any]):
        user = cls(username="TBA")
        for key, val in user_dict.items():
            setattr(user, key, val)
        return user


@dataclass
class WrongPassword:
    """
    Return this object if the username was found but the password was incorrect

    This ensures that if the username exists as a real user, we won't continue to use the environcfg.yml file to authenticate

    Attributes:
        username: The identifier for the user
    """
    username: str
