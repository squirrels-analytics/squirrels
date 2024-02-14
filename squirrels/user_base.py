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

    def __init__(self, username: str, is_internal: bool):
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
    
    @classmethod
    def Create(cls, username: str, user_dict: dict[str, Any], *, is_internal: bool = False, **kwargs):
        user = cls(username, is_internal)
        user.set_attributes(user_dict)
        return user
    
    @classmethod
    def _FromDict(cls, user_obj_as_dict: dict[str, Any]):
        username, is_internal = user_obj_as_dict["username"], user_obj_as_dict["is_internal"]
        user = cls(username=username, is_internal=is_internal)
        for key, val in user_obj_as_dict.items():
            setattr(user, key, val)
        return user


@dataclass
class WrongPassword:
    """
    Return this object if the username was found but the password was incorrect

    This ensures that if the username exists as a real user, we won't continue to use the environcfg.yml file to authenticate
    """
